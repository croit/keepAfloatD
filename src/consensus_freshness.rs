//! Process-local quorum proof lifetime; no wall clock enters replicated placement (#26).
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

pub(crate) mod failure;
mod permits;
use crate::raft::admission::runtime::VerifiedProgress;
pub(crate) use failure::{FailureKind, failure_kind};
pub(crate) use permits::{AcquirePermit, RetainPermit, RetentionStamp};

/// A successful submit proves quorum contact only for a bounded probe/handoff window.
/// Measure from request start so a delayed response cannot manufacture a new lease.
pub(crate) struct ConsensusFreshness {
    proof: watch::Sender<Proof>,
    lifetime: Duration,
    identity: Arc<()>,
}

#[derive(Clone)]
struct Proof {
    started: Option<Instant>,
    invalidation_epoch: u64,
    acquisition_epoch: u64,
    available: bool,
    progress: Option<VerifiedProgress>,
}

#[derive(Debug)]
pub(crate) struct ProofRejected(&'static str);

impl std::fmt::Display for ProofRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for ProofRejected {}

impl Proof {
    fn deadline(&self, lifetime: Duration) -> Option<Instant> {
        let deadline = self.started?.checked_add(lifetime)?;
        Some(
            self.progress
                .as_ref()
                .map_or(deadline, |proof| deadline.min(proof.accepted_deadline())),
        )
    }

    fn fresh(&self, lifetime: Duration) -> bool {
        self.deadline(lifetime)
            .is_some_and(|deadline| Instant::now() < deadline)
            && self
                .progress
                .as_ref()
                .is_none_or(|proof| proof.check().is_ok())
    }
}

impl ConsensusFreshness {
    /// Normal probe work can finish just after the next interval boundary (#26).
    /// Permit half a configured activation interval for renewal jitter. Committed
    /// rounds can arrive in bursts, so the successor separately waits in local time.
    pub(crate) fn for_probe_cadence(interval_ms: u64, stale_missed_probes: u64) -> Self {
        let stale = Duration::from_millis(interval_ms.saturating_mul(stale_missed_probes));
        let handoff = Duration::from_millis(
            interval_ms.saturating_mul(crate::raft::store::OWNERSHIP_ACTIVATION_HOLDOFF_TICKS),
        );
        Self::new(stale.saturating_add(handoff / 2))
    }

    pub(crate) fn new(lifetime: Duration) -> Self {
        Self {
            proof: watch::channel(Proof {
                started: None,
                invalidation_epoch: 0,
                acquisition_epoch: 0,
                available: false,
                progress: None,
            })
            .0,
            lifetime,
            identity: Arc::new(()),
        }
    }

    #[cfg(test)]
    pub(crate) fn record_success(&self, started: Instant) {
        self.proof.send_if_modified(|proof| {
            if proof.started.is_none_or(|previous| started > previous) {
                if !proof.fresh(self.lifetime) || proof.progress.is_some() {
                    proof.invalidation_epoch = proof.invalidation_epoch.wrapping_add(1);
                }
                proof.started = Some(started);
                proof.progress = None;
                proof.available = true;
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn record_applied(&self, progress: VerifiedProgress) -> Result<(), ProofRejected> {
        if progress.healthy() != Some(true) {
            return Err(ProofRejected(
                "only completed healthy progress can renew freshness",
            ));
        }
        progress
            .check()
            .map_err(|_| ProofRejected("health proof authority is revoked"))?;
        let started = progress.started();
        let deadline = started
            .checked_add(self.lifetime)
            .ok_or(ProofRejected("health proof lifetime overflow"))?
            .min(progress.accepted_deadline());
        if started > Instant::now() || Instant::now() >= deadline {
            return Err(ProofRejected(
                "health proof is outside its original lifetime",
            ));
        }
        let mut result = Ok(());
        self.proof.send_if_modified(|proof| {
            if proof.started.is_some_and(|previous| started < previous)
                || proof.progress.as_ref().is_some_and(|previous| {
                    previous.context() == progress.context()
                        && (progress.log_id().index <= previous.log_id().index
                            || progress.log_id() <= previous.log_id())
                })
            {
                result = Err(ProofRejected(
                    "health proof does not advance verified progress",
                ));
                return false;
            }
            if !proof.fresh(self.lifetime)
                || proof
                    .progress
                    .as_ref()
                    .is_none_or(|previous| previous.context() != progress.context())
            {
                proof.invalidation_epoch = proof.invalidation_epoch.wrapping_add(1);
            }
            proof.started = Some(started);
            proof.progress = Some(progress.clone());
            proof.available = true;
            tracing::debug!(
                log_id = %progress.log_id(),
                remaining = ?deadline.saturating_duration_since(Instant::now()),
                "verified healthy proof accepted for VIP effects"
            );
            true
        });
        result
    }

    pub(crate) fn mark_unavailable(&self) {
        self.proof.send_if_modified(|proof| {
            if !proof.available {
                return false;
            }
            proof.available = false;
            proof.acquisition_epoch = proof.acquisition_epoch.wrapping_add(1);
            true
        });
    }

    pub(crate) fn invalidate(&self) {
        self.proof.send_modify(|proof| {
            proof.started = None;
            proof.available = false;
            proof.invalidation_epoch = proof.invalidation_epoch.wrapping_add(1);
        });
    }

    #[cfg(test)]
    pub(crate) fn is_fresh(&self) -> bool {
        let proof = self.proof.borrow();
        proof.available && proof.fresh(self.lifetime)
    }

    pub(crate) fn lifetime(&self) -> Duration {
        self.lifetime
    }

    #[cfg(test)]
    async fn wait_until_stale(&self) {
        let mut proof = self.proof.subscribe();
        loop {
            let deadline = proof
                .borrow_and_update()
                .started
                .and_then(|started| started.checked_add(self.lifetime));
            let Some(deadline) = deadline else { return };
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    // #26: renewal may already be queued when the old timer wakes.
                    if !self.is_fresh() { return; }
                },
                changed = proof.changed() => {
                    if changed.is_err() { return; }
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn renewal_margin_is_bounded_to_half_a_configured_probe_interval() {
        let freshness = ConsensusFreshness::for_probe_cadence(1_000, 1);
        freshness.record_success(Instant::now());
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(
            freshness.is_fresh(),
            "ordinary probe jitter must not withdraw a VIP"
        );
        tokio::time::advance(Duration::from_millis(800)).await;
        assert!(!freshness.is_fresh(), "renewal grace must remain bounded");
    }

    #[tokio::test(start_paused = true)]
    async fn proof_expires_at_the_exact_deadline_without_another_probe() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(1));
        assert!(!freshness.is_fresh());
        freshness.record_success(Instant::now());
        assert!(freshness.is_fresh());
        tokio::time::advance(Duration::from_millis(999)).await;
        assert!(freshness.is_fresh());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!freshness.is_fresh());
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_acknowledgement_does_not_start_a_new_lease() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(1));
        let started = Instant::now();
        tokio::time::advance(Duration::from_secs(2)).await;
        freshness.record_success(started);
        assert!(!freshness.is_fresh());
        freshness.wait_until_stale().await;
    }

    #[tokio::test(start_paused = true)]
    async fn older_completion_cannot_replace_a_newer_proof() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(1));
        let older = Instant::now();
        tokio::time::advance(Duration::from_millis(500)).await;
        freshness.record_success(Instant::now());
        freshness.record_success(older);
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(freshness.is_fresh());
    }

    #[tokio::test(start_paused = true)]
    async fn renewal_moves_the_waiter_deadline_and_failure_invalidates_immediately() {
        let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
        freshness.record_success(Instant::now());
        let copy = freshness.clone();
        let waiter = tokio::spawn(async move { copy.wait_until_stale().await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(500)).await;
        freshness.record_success(Instant::now());
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(!waiter.is_finished());
        freshness.invalidate();
        waiter.await.unwrap();
        assert!(!freshness.is_fresh());
    }

    #[tokio::test(start_paused = true)]
    async fn queued_renewal_at_the_old_deadline_does_not_expire_current_proof() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(1));
        freshness.record_success(Instant::now());
        let waiter = freshness.wait_until_stale();
        tokio::pin!(waiter);
        assert!(futures::poll!(&mut waiter).is_pending());
        tokio::time::advance(Duration::from_millis(500)).await;
        freshness.record_success(Instant::now());
        // #26: simulate the waiter being descheduled until its old timer is also ready.
        // Both renewal and timer notifications must be handled using the current proof.
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(
            freshness.is_fresh(),
            "the renewed proof must still be valid"
        );
        assert!(
            futures::poll!(&mut waiter).is_pending(),
            "a queued renewal must supersede the previous deadline"
        );
        freshness.invalidate();
        waiter.await;
    }
}

#[cfg(test)]
mod timing_tests;
