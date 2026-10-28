//! Completed health progress bound to its accepting runtime certificate.

use super::*;
use crate::raft::admission::{AdmissionContext, AppliedHealthProgress};
use crate::raft::types::TypeConfig;
use openraft::alias::LogIdOf;

pub(super) struct InstalledAdmission {
    pub(super) session: AdmissionSession,
    pub(super) deadline: Instant,
    pub(super) started: Instant,
    pub(super) request: crate::raft::admission::AdmissionRequest,
}

/// Only a completed, locally applied renewal can create this evidence.
#[derive(Clone)]
pub(crate) struct VerifiedProgress {
    started: Instant,
    applied: AppliedHealthProgress,
    accepted_deadline: Instant,
    session: AdmissionSession,
    authority: Arc<RuntimeAuthority>,
}

impl VerifiedProgress {
    /// Reconciliation fixtures supply progress while retaining real runtime revocation.
    #[cfg(test)]
    pub(crate) fn for_test(
        runtime: &RuntimeDriver,
        started: Instant,
        log_id: LogIdOf<TypeConfig>,
        healthy: Option<bool>,
    ) -> Result<Self, AdmissionDenied> {
        let session = runtime
            .current()
            .ok_or(AdmissionDenied("test progress requires live admission"))?;
        let context = session.context();
        let proof = Self {
            started,
            accepted_deadline: session.check()?,
            applied: AppliedHealthProgress {
                request: crate::raft::admission::HealthProgress {
                    node_id: context.local_replica.physical_id,
                    replica: context.local_replica,
                    healthy,
                    epoch: context.genesis.epoch,
                    genesis: context.genesis.clone(),
                    request_nonce: [0; 32],
                },
                log_id,
            },
            session,
            authority: runtime.authority.clone(),
        };
        proof.check()?;
        Ok(proof)
    }

    pub(super) fn new(
        started: Instant,
        applied: AppliedHealthProgress,
        installed: InstalledAdmission,
        authority: Arc<RuntimeAuthority>,
    ) -> Result<Self, AdmissionDenied> {
        let context = installed.session.context();
        applied.request.validate()?;
        if applied.request.replica != context.local_replica
            || applied.request.genesis != context.genesis
            || applied.request.request_nonce != installed.request.request_nonce
            || started > installed.started
            || installed.request.mode
                != (AdmissionMode::Renew {
                    progress: Some(applied.log_id),
                })
        {
            return Err(AdmissionDenied(
                "health proof differs from accepted admission",
            ));
        }
        let proof = Self {
            started,
            applied,
            accepted_deadline: installed.deadline,
            session: installed.session,
            authority,
        };
        proof.check()?;
        Ok(proof)
    }

    pub(crate) fn started(&self) -> Instant {
        self.started
    }

    pub(crate) fn log_id(&self) -> LogIdOf<TypeConfig> {
        self.applied.log_id
    }

    pub(crate) fn healthy(&self) -> Option<bool> {
        self.applied.request.healthy
    }

    pub(crate) fn context(&self) -> &AdmissionContext {
        self.session.context()
    }

    pub(crate) fn accepted_deadline(&self) -> Instant {
        self.accepted_deadline
    }

    pub(crate) fn check(&self) -> Result<(), AdmissionDenied> {
        self.session.check()?;
        if Instant::now() >= self.accepted_deadline {
            return Err(AdmissionDenied("health proof admission deadline expired"));
        }
        Ok(())
    }

    pub(crate) async fn revoked(&self) {
        self.authority.wait_until_sealed().await;
    }
}

#[cfg(test)]
mod tests;
