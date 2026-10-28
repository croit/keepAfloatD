//! Separate permission to create bindings from bounded continuity of completed bindings.

use super::*;
use crate::raft::admission::AdmissionContext;
use std::future::Future;

#[derive(Clone, Debug)]
pub(crate) struct RetentionStamp {
    identity: Arc<()>,
    context: AdmissionContext,
    epoch: u64,
}

struct Permit {
    freshness: Arc<ConsensusFreshness>,
    stamp: RetentionStamp,
    acquisition_epoch: Option<u64>,
    deadline: Instant,
    progress: VerifiedProgress,
    changes: watch::Receiver<Proof>,
}

pub(crate) struct AcquirePermit(Permit);
pub(crate) struct RetainPermit(Permit);

impl ConsensusFreshness {
    pub(crate) fn acquire(self: &Arc<Self>, has_leader: bool) -> Option<AcquirePermit> {
        let mut changes = self.proof.subscribe();
        let proof = changes.borrow_and_update().clone();
        if !has_leader || !proof.available || !proof.fresh(self.lifetime) {
            return None;
        }
        let progress = proof.progress.as_ref()?;
        Some(AcquirePermit(Permit {
            freshness: self.clone(),
            stamp: RetentionStamp {
                identity: self.identity.clone(),
                context: progress.context().clone(),
                epoch: proof.invalidation_epoch,
            },
            acquisition_epoch: Some(proof.acquisition_epoch),
            deadline: proof.deadline(self.lifetime)?,
            progress: progress.clone(),
            changes,
        }))
    }

    pub(crate) fn retain(self: &Arc<Self>, stamp: &RetentionStamp) -> Option<RetainPermit> {
        let mut changes = self.proof.subscribe();
        let proof = changes.borrow_and_update().clone();
        let progress = proof.progress.as_ref()?;
        if !Arc::ptr_eq(&self.identity, &stamp.identity)
            || proof.invalidation_epoch != stamp.epoch
            || progress.context() != &stamp.context
            || !proof.fresh(self.lifetime)
        {
            return None;
        }
        Some(RetainPermit(Permit {
            freshness: self.clone(),
            stamp: stamp.clone(),
            acquisition_epoch: None,
            deadline: proof.deadline(self.lifetime)?,
            progress: progress.clone(),
            changes,
        }))
    }
}

impl Permit {
    fn check(&self) -> bool {
        if Instant::now() >= self.deadline || self.progress.check().is_err() {
            return false;
        }
        let proof = self.freshness.proof.borrow();
        proof.invalidation_epoch == self.stamp.epoch
            && proof.progress.as_ref().is_some_and(|progress| {
                progress.context() == &self.stamp.context && progress.check().is_ok()
            })
            && self
                .acquisition_epoch
                .is_none_or(|epoch| proof.available && proof.acquisition_epoch == epoch)
    }

    async fn run<F: Future>(&self, operation: F) -> Option<F::Output> {
        if !self.check() {
            return None;
        }
        let mut changes = self.changes.clone();
        tokio::pin!(operation);
        loop {
            tokio::select! {
                biased;
                _ = self.progress.revoked() => return None,
                _ = tokio::time::sleep_until(self.deadline) => return None,
                changed = changes.changed() => {
                    if changed.is_err() || !self.check() { return None; }
                }
                result = &mut operation => return self.check().then_some(result),
            }
        }
    }
}

impl AcquirePermit {
    pub(crate) fn stamp(&self) -> RetentionStamp {
        self.0.stamp.clone()
    }
    pub(crate) fn context(&self) -> &AdmissionContext {
        &self.0.stamp.context
    }
    pub(crate) fn check(&self) -> bool {
        self.0.check()
    }
    pub(crate) async fn run<F: Future>(&self, operation: F) -> Option<F::Output> {
        self.0.run(operation).await
    }
}

impl RetainPermit {
    pub(crate) fn context(&self) -> &AdmissionContext {
        &self.0.stamp.context
    }
    pub(crate) fn check(&self) -> bool {
        self.0.check()
    }
    #[cfg(test)]
    pub(crate) fn deadline(&self) -> Instant {
        self.0.deadline
    }
    pub(crate) async fn run<F: Future>(&self, operation: F) -> Option<F::Output> {
        self.0.run(operation).await
    }
}
