//! Quarantine, physical-roster reservations and nonce-bound admission certificates.

mod bootstrap;
pub use bootstrap::{JoinBinding, VerifiedJoinBootstrap};

use super::{AdmissionContext, AdmissionDenied, Genesis, ReplicaId};
use crate::config::{ClusterConfigFingerprint, Config};
use crate::raft::store::KafStorageState;
use openraft::alias::LogIdOf;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::time::Instant;

pub trait AdmissionTiming: Send + Sync {
    fn consumer_deadline(&self, start: Instant) -> Result<Instant, AdmissionDenied>;
    fn reservation_deadline(&self, reserved: Instant) -> Result<Instant, AdmissionDenied>;
    fn quarantine_deadline(&self, boot: Instant) -> Result<Instant, AdmissionDenied>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionMode {
    Cold,
    Join {
        prepared: Option<JoinBinding>,
    },
    Renew {
        progress: Option<LogIdOf<crate::raft::TypeConfig>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionRequest {
    pub consumer: ReplicaId,
    pub genesis: Genesis,
    pub request_nonce: [u8; 32],
    pub mode: AdmissionMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedAdmission<T> {
    pub sender: ReplicaId,
    pub recipient: ReplicaId,
    pub binding: [u8; 32],
    pub payload: T,
    pub tag: [u8; 32],
}

impl<T: Serialize> SignedAdmission<T> {
    pub fn sign(
        secret: Option<&str>,
        role: u8,
        sender: ReplicaId,
        recipient: ReplicaId,
        binding: [u8; 32],
        payload: T,
    ) -> Result<Self, AdmissionDenied> {
        let bytes = serde_json::to_vec(&(binding, &payload))
            .map_err(|_| AdmissionDenied("admission record encoding failed"))?;
        let tag = crate::auth::sign_admission_record(secret, role, sender, recipient, &bytes)
            .map_err(|_| AdmissionDenied("admission record signing failed"))?;
        Ok(Self {
            sender,
            recipient,
            binding,
            payload,
            tag,
        })
    }

    pub fn verify(
        &self,
        secret: Option<&str>,
        role: u8,
        sender: ReplicaId,
        recipient: ReplicaId,
        binding: [u8; 32],
    ) -> Result<(), AdmissionDenied> {
        if self.sender != sender || self.recipient != recipient || self.binding != binding {
            return Err(AdmissionDenied("admission record boot or channel mismatch"));
        }
        let bytes = serde_json::to_vec(&(self.binding, &self.payload))
            .map_err(|_| AdmissionDenied("admission record encoding failed"))?;
        crate::auth::verify_admission_record(secret, role, sender, recipient, &bytes, &self.tag)
            .map_err(|_| AdmissionDenied("admission record signature mismatch"))
    }
}

/// A response checked against transport metadata, not metadata supplied by its payload.
#[derive(Clone)]
pub struct ReceivedGrant {
    record: SignedAdmission<AdmissionRequest>,
    issuer: ReplicaId,
    binding: [u8; 32],
}

impl ReceivedGrant {
    pub fn authenticate(
        secret: Option<&str>,
        issuer: ReplicaId,
        recipient: ReplicaId,
        binding: [u8; 32],
        request: &AdmissionRequest,
        record: SignedAdmission<AdmissionRequest>,
    ) -> Result<Self, AdmissionDenied> {
        record.verify(secret, 11, issuer, recipient, binding)?;
        if record.payload != *request {
            return Err(AdmissionDenied(
                "admission response differs from its request",
            ));
        }
        Ok(Self {
            record,
            issuer,
            binding,
        })
    }
}

pub struct AdmissionRound {
    request: AdmissionRequest,
    started: Instant,
    deadline: Instant,
    completed: bool,
    prepared_join: Option<super::PreparedJoin>,
}

impl AdmissionRound {
    pub fn request(&self) -> &AdmissionRequest {
        &self.request
    }
    pub fn request_started(&self) -> Instant {
        self.started
    }

    pub fn bind_progress(
        &mut self,
        applied: &super::AppliedHealthProgress,
    ) -> Result<(), AdmissionDenied> {
        if !matches!(self.request.mode, AdmissionMode::Renew { progress: None })
            || applied.request.replica != self.request.consumer
            || applied.request.genesis != self.request.genesis
            || applied.request.request_nonce != self.request.request_nonce
            || self.completed
            || Instant::now() >= self.deadline
        {
            return Err(AdmissionDenied(
                "progress is not for this outstanding renewal",
            ));
        }
        self.request.mode = AdmissionMode::Renew {
            progress: Some(applied.log_id),
        };
        Ok(())
    }

    pub fn bind_prepared(&mut self, prepared: &super::PreparedJoin) -> Result<(), AdmissionDenied> {
        prepared.plan.validate()?;
        if !matches!(self.request.mode, AdmissionMode::Join { prepared: None })
            || prepared.plan.consumer != self.request.consumer
            || prepared.plan.genesis != self.request.genesis
            || self.completed
            || Instant::now() >= self.deadline
        {
            return Err(AdmissionDenied(
                "preparation is not for this outstanding join",
            ));
        }
        self.request.mode = AdmissionMode::Join {
            prepared: Some(JoinBinding::from_prepared(prepared)),
        };
        let mut bound = prepared.clone();
        bound.learner_applied = None;
        self.prepared_join = Some(bound);
        Ok(())
    }
}

/// Only successful physical-quorum verification constructs this process-local result.
pub struct VerifiedAdmission {
    context: AdmissionContext,
    // Tests inspect challenge provenance after quorum verification consumes the round.
    #[cfg(test)]
    request_nonce: [u8; 32],
    deadline: Instant,
    mode: AdmissionMode,
    bootstrap: Option<VerifiedJoinBootstrap>,
}

impl VerifiedAdmission {
    #[cfg(test)]
    pub fn request_nonce(&self) -> [u8; 32] {
        self.request_nonce
    }
    pub fn context(&self) -> &AdmissionContext {
        &self.context
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn mode(&self) -> &AdmissionMode {
        &self.mode
    }
    pub fn join_bootstrap(&self) -> Option<&VerifiedJoinBootstrap> {
        self.bootstrap.as_ref()
    }
}

struct Reservation {
    genesis: Genesis,
    until: Instant,
}

pub struct AdmissionController {
    cfg: Arc<Config>,
    configured: BTreeSet<u64>,
    config_identity: ClusterConfigFingerprint,
    local: ReplicaId,
    timing: Arc<dyn AdmissionTiming>,
    quarantine_until: Instant,
    reservation: Option<Reservation>,
}

impl AdmissionController {
    pub fn new(
        cfg: Arc<Config>,
        local: ReplicaId,
        boot: Instant,
        timing: Arc<dyn AdmissionTiming>,
    ) -> Result<Self, AdmissionDenied> {
        let configured: BTreeSet<_> = cfg.peers.iter().map(|peer| peer.id).collect();
        if local.physical_id != cfg.node_id || !configured.contains(&local.physical_id) {
            return Err(AdmissionDenied("local boot is not the configured member"));
        }
        let config_identity = cfg
            .cluster_config_fingerprint()
            .map_err(|_| AdmissionDenied("invalid admission configuration"))?;
        let quarantine_until = timing.quarantine_deadline(boot)?;
        Ok(Self {
            cfg,
            configured,
            config_identity,
            local,
            timing,
            quarantine_until,
            reservation: None,
        })
    }

    pub fn begin(
        &self,
        genesis: Genesis,
        mode: AdmissionMode,
    ) -> Result<AdmissionRound, AdmissionDenied> {
        if matches!(
            mode,
            AdmissionMode::Join { prepared: Some(_) } | AdmissionMode::Renew { progress: Some(_) }
        ) {
            return Err(AdmissionDenied(
                "new rounds must bind their own committed evidence",
            ));
        }
        self.validate_genesis(&genesis)?;
        let started = Instant::now();
        if started < self.quarantine_until {
            return Err(AdmissionDenied("restart quarantine has not elapsed"));
        }
        let deadline = self.timing.consumer_deadline(started)?;
        let mut request_nonce = [0; 32];
        getrandom::fill(&mut request_nonce)
            .map_err(|_| AdmissionDenied("admission request entropy unavailable"))?;
        Ok(AdmissionRound {
            request: AdmissionRequest {
                consumer: self.local,
                genesis,
                request_nonce,
                mode,
            },
            started,
            deadline,
            completed: false,
            prepared_join: None,
        })
    }

    fn validate_genesis(&self, genesis: &Genesis) -> Result<(), AdmissionDenied> {
        genesis.validate_roster(&self.configured)?;
        if genesis.config != self.config_identity {
            return Err(AdmissionDenied("admission configuration mismatch"));
        }
        Ok(())
    }

    pub fn reserve(
        &mut self,
        signed: &SignedAdmission<AdmissionRequest>,
        binding: [u8; 32],
        state: Option<&KafStorageState>,
    ) -> Result<SignedAdmission<AdmissionRequest>, AdmissionDenied> {
        signed.verify(
            self.cfg.cluster_secret.as_deref(),
            10,
            signed.payload.consumer,
            self.local,
            binding,
        )?;
        let request = &signed.payload;
        self.validate_genesis(&request.genesis)?;
        if !self.configured.contains(&request.consumer.physical_id) {
            return Err(AdmissionDenied("admission consumer is not configured"));
        }
        let now = Instant::now();
        if now < self.quarantine_until {
            return Err(AdmissionDenied("restart quarantine has not elapsed"));
        }
        if self
            .reservation
            .as_ref()
            .is_some_and(|current| now < current.until && current.genesis != request.genesis)
        {
            return Err(AdmissionDenied(
                "physical member is reserved for another genesis",
            ));
        }
        match &request.mode {
            AdmissionMode::Cold => {
                if !request.genesis.voters.contains(&request.consumer)
                    || !request.genesis.voters.contains(&self.local)
                {
                    return Err(AdmissionDenied(
                        "cold grant requires exact proposed boot consent",
                    ));
                }
                if let Some(state) = state {
                    if let Some(session) = state.admission.as_ref() {
                        session.check()?;
                        if session.context().local_replica != self.local
                            || session.context().genesis != request.genesis
                            || state
                                .genesis
                                .as_ref()
                                .is_some_and(|genesis| genesis != &request.genesis)
                        {
                            return Err(AdmissionDenied(
                                "cold consent differs from active original cohort",
                            ));
                        }
                    } else if state.genesis.is_some() {
                        return Err(AdmissionDenied(
                            "stored history alone cannot authorize cold consent",
                        ));
                    }
                }
            }
            AdmissionMode::Join { prepared } => {
                let state = state.ok_or(AdmissionDenied(
                    "join requires locally applied committed preparation",
                ))?;
                let session = state
                    .admission
                    .as_ref()
                    .ok_or(AdmissionDenied("issuer has no continuing authority"))?;
                session.check()?;
                let operation = state
                    .prepared_join
                    .as_ref()
                    .ok_or(AdmissionDenied("join preparation is not applied"))?;
                operation.plan.validate()?;
                if session.context().local_replica != self.local
                    || session.context().genesis != request.genesis
                    || state.genesis.as_ref() != Some(&request.genesis)
                    || *prepared != Some(JoinBinding::from_prepared(operation))
                    || operation.plan.consumer != request.consumer
                    || operation.plan.genesis != request.genesis
                    || state.last_membership.membership().get_joint_config().len() != 1
                    || state
                        .last_membership
                        .membership()
                        .voter_ids()
                        .collect::<BTreeSet<_>>()
                        != operation.plan.previous_voters
                {
                    return Err(AdmissionDenied(
                        "join does not match continuing committed authority",
                    ));
                }
            }
            AdmissionMode::Renew { progress } => {
                let state =
                    state.ok_or(AdmissionDenied("renewal requires locally applied progress"))?;
                let session = state
                    .admission
                    .as_ref()
                    .ok_or(AdmissionDenied("issuer has no continuing authority"))?;
                session.check()?;
                let applied = state
                    .applied_progress
                    .get(&request.consumer.physical_id)
                    .ok_or(AdmissionDenied("consumer progress has not been applied"))?;
                if session.context().local_replica != self.local
                    || session.context().genesis != request.genesis
                    || state.genesis.as_ref() != Some(&request.genesis)
                    || Some(applied.log_id) != *progress
                    || applied.request.replica != request.consumer
                    || applied.request.genesis != request.genesis
                    || applied.request.request_nonce != request.request_nonce
                {
                    tracing::debug!(
                        issuer = %self.local,
                        consumer = %request.consumer,
                        expected_progress = ?progress,
                        applied_progress = ?applied.log_id,
                        last_applied = ?state.last_applied_log,
                        issuer_boot_matches = session.context().local_replica == self.local,
                        session_genesis_matches = session.context().genesis == request.genesis,
                        state_genesis_matches = state.genesis.as_ref() == Some(&request.genesis),
                        consumer_boot_matches = applied.request.replica == request.consumer,
                        progress_genesis_matches = applied.request.genesis == request.genesis,
                        challenge_matches = applied.request.request_nonce == request.request_nonce,
                        "renewal progress does not match locally applied evidence"
                    );
                    return Err(AdmissionDenied(
                        "renewal is not this consumer's fresh committed progress",
                    ));
                }
            }
        }
        let until = self.timing.reservation_deadline(now)?;
        if until <= now {
            return Err(AdmissionDenied("reservation deadline is not in the future"));
        }
        let until = self
            .reservation
            .as_ref()
            .filter(|r| r.genesis == request.genesis)
            .map_or(until, |r| r.until.max(until));
        self.reservation = Some(Reservation {
            genesis: request.genesis.clone(),
            until,
        });
        SignedAdmission::sign(
            self.cfg.cluster_secret.as_deref(),
            11,
            self.local,
            request.consumer,
            binding,
            request.clone(),
        )
    }

    pub fn complete(
        &self,
        round: &mut AdmissionRound,
        grants: &[ReceivedGrant],
    ) -> Result<VerifiedAdmission, AdmissionDenied> {
        if matches!(
            round.request.mode,
            AdmissionMode::Join { prepared: None } | AdmissionMode::Renew { progress: None }
        ) {
            return Err(AdmissionDenied("admission round lacks committed evidence"));
        }
        if round.completed || Instant::now() >= round.deadline {
            round.completed = true;
            return Err(AdmissionDenied(
                "admission round expired or already consumed",
            ));
        }
        if round.request.consumer != self.local {
            return Err(AdmissionDenied("admission round belongs to another boot"));
        }
        let mut physical = BTreeSet::new();
        let mut exact = BTreeSet::new();
        for received in grants {
            let grant = &received.record;
            grant.verify(
                self.cfg.cluster_secret.as_deref(),
                11,
                received.issuer,
                self.local,
                received.binding,
            )?;
            if grant.payload != round.request
                || !self.configured.contains(&grant.sender.physical_id)
                || !physical.insert(grant.sender.physical_id)
            {
                return Err(AdmissionDenied(
                    "grant mismatch or duplicate physical issuer",
                ));
            }
            exact.insert(grant.sender);
        }
        if physical.len() <= self.configured.len() / 2 {
            return Err(AdmissionDenied("admission grants lack a physical majority"));
        }
        if matches!(round.request.mode, AdmissionMode::Cold)
            && exact != round.request.genesis.voters
        {
            return Err(AdmissionDenied(
                "cold genesis lacks every exact boot's consent",
            ));
        }
        round.completed = true;
        Ok(VerifiedAdmission {
            #[cfg(test)]
            request_nonce: round.request.request_nonce,
            context: AdmissionContext {
                local_replica: self.local,
                genesis: round.request.genesis.clone(),
            },
            deadline: round.deadline,
            mode: round.request.mode.clone(),
            bootstrap: round
                .prepared_join
                .clone()
                .map(|prepared| VerifiedJoinBootstrap { prepared }),
        })
    }
}

#[cfg(test)]
mod tests;
