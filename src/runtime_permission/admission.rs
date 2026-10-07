//! Bind a verified quorum result to one terminal process-local authority.

use super::{AuthorityContext, LeaseTiming, RuntimePermission};
use crate::raft::admission::{
    AdmissionContext, AdmissionDenied, AdmissionFence, AdmissionMode, AdmissionSession,
    AdmissionTiming, JoinBinding, ReplicaId, VerifiedAdmission,
};
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

pub(crate) struct RuntimeAuthority {
    local: ReplicaId,
    permission: RuntimePermission,
    join_binding: Mutex<Option<JoinBinding>>,
}

impl RuntimeAuthority {
    pub(crate) fn new(
        local: ReplicaId,
        timing: LeaseTiming,
        boot: Instant,
    ) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            local,
            permission: RuntimePermission::quarantined(timing.quarantine_deadline(boot)?),
            join_binding: Mutex::new(None),
        }))
    }

    pub(crate) fn accept(
        self: &Arc<Self>,
        verified: &VerifiedAdmission,
    ) -> Result<AdmissionSession, AdmissionDenied> {
        let expected = verified.context();
        if expected.local_replica != self.local {
            return Err(AdmissionDenied(
                "admission result belongs to another runtime",
            ));
        }
        let context = self.identity(expected);
        let mut join_binding = self
            .join_binding
            .lock()
            .map_err(|_| AdmissionDenied("join binding lock poisoned"))?;
        let accepted = match verified.mode() {
            AdmissionMode::Cold => self.permission.admit(context, verified.deadline()),
            AdmissionMode::Join {
                prepared: Some(prepared),
            } => {
                if join_binding.is_some_and(|bound| bound != *prepared) {
                    return Err(AdmissionDenied("join differs from the admitted operation"));
                }
                let accepted = self
                    .permission
                    .join(context, prepared.log_id, verified.deadline());
                if accepted {
                    *join_binding = Some(*prepared);
                }
                accepted
            }
            AdmissionMode::Renew { progress: Some(_) } => {
                self.permission.renew(context, verified.deadline())
            }
            _ => false,
        };
        if !accepted {
            return Err(AdmissionDenied(
                "runtime cannot accept this admission transition",
            ));
        }
        AdmissionSession::new(expected.clone(), self.clone())
    }

    /// A completion receipt survives later membership changes, but never restores authority.
    pub(crate) fn mark_promoted(
        &self,
        prepared: &crate::raft::admission::PreparedJoin,
        state: &crate::raft::KafStorageState,
    ) -> Result<(), AdmissionDenied> {
        prepared.plan.validate()?;
        let expected = AdmissionContext {
            local_replica: prepared.plan.consumer,
            genesis: prepared.plan.genesis.clone(),
        };
        self.check(&expected)?;
        let session = state
            .admission
            .as_ref()
            .ok_or(AdmissionDenied("promotion requires local admission"))?;
        session.check()?;
        let acknowledged = prepared.learner_applied.ok_or(AdmissionDenied(
            "promotion requires committed learner acknowledgement",
        ))?;
        let membership_log = state.last_membership.log_id().ok_or(AdmissionDenied(
            "promotion requires committed final membership",
        ))?;
        let completed =
            state
                .completed_joins
                .get(&self.local.physical_id)
                .ok_or(AdmissionDenied(
                    "promotion requires the committed completion receipt",
                ))?;
        let bound = *self
            .join_binding
            .lock()
            .map_err(|_| AdmissionDenied("join binding lock poisoned"))?;
        let membership = state.last_membership.membership();
        if session.context() != &expected
            || state.genesis.as_ref() != Some(&expected.genesis)
            || bound != Some(JoinBinding::from_prepared(prepared))
            || completed.prepared != *prepared
            || state.prepared_join.as_ref().is_some_and(|pending| {
                pending.prepared == prepared.prepared && pending.plan == prepared.plan
            })
            || membership.get_joint_config().is_empty()
            || membership
                .get_joint_config()
                .iter()
                .any(|voters| !voters.contains(&self.local))
            || membership
                .voter_ids()
                .filter(|id| id.physical_id == self.local.physical_id)
                .count()
                != 1
            || acknowledged.index <= prepared.prepared.index
            || completed.promoted.index <= acknowledged.index
            || membership_log.index < completed.promoted.index
            || (membership_log.index == completed.promoted.index
                && membership_log != completed.promoted)
            || state.last_applied_log.is_none_or(|last| {
                last.index < membership_log.index
                    || (last.index == membership_log.index && last != membership_log)
            })
        {
            return Err(AdmissionDenied(
                "promotion differs from locally applied join completion",
            ));
        }
        if !self
            .permission
            .mark_promoted(self.identity(&expected), prepared.prepared)
        {
            return Err(AdmissionDenied(
                "runtime is not this operation's active learner",
            ));
        }
        Ok(())
    }

    fn identity(&self, context: &AdmissionContext) -> AuthorityContext {
        AuthorityContext {
            boot_nonce: self.local.boot_nonce,
            cluster_epoch: context.genesis.epoch,
            genesis_digest: context.genesis.digest(),
            admission_generation: 1,
        }
    }

    pub(crate) fn current(&self) -> Option<(AuthorityContext, Instant)> {
        self.permission.current()
    }

    pub(crate) fn seal(&self) {
        self.permission.seal();
    }

    pub(crate) async fn wait_until_sealed(&self) {
        self.permission.wait_until_sealed().await;
    }
}

impl AdmissionFence for RuntimeAuthority {
    fn local_replica(&self) -> ReplicaId {
        self.local
    }

    fn check(&self, expected: &AdmissionContext) -> Result<Instant, AdmissionDenied> {
        let (context, until) = self
            .current()
            .ok_or(AdmissionDenied("runtime has no active admission"))?;
        if expected.local_replica != self.local || context != self.identity(expected) {
            return Err(AdmissionDenied("runtime authority context mismatch"));
        }
        Ok(until)
    }
}

impl AdmissionTiming for LeaseTiming {
    fn consumer_deadline(&self, start: Instant) -> Result<Instant, AdmissionDenied> {
        (*self)
            .consumer_deadline(start)
            .map_err(|_| AdmissionDenied("admission consumer deadline overflow"))
    }
    fn reservation_deadline(&self, start: Instant) -> Result<Instant, AdmissionDenied> {
        (*self)
            .reservation_deadline(start)
            .map_err(|_| AdmissionDenied("admission reservation deadline overflow"))
    }
    fn quarantine_deadline(&self, start: Instant) -> Result<Instant, AdmissionDenied> {
        (*self)
            .quarantine_deadline(start)
            .map_err(|_| AdmissionDenied("admission quarantine deadline overflow"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::raft::admission::{
        AdmissionController, AdmissionMode, Genesis, ReplicaId, SignedAdmission,
    };
    use crate::runtime_permission::LeaseTiming;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::Instant;

    fn config() -> Arc<Config> {
        Arc::new(serde_yaml::from_str(
            "node_id: 1\nraft_listen: '127.0.0.1:1000'\nclient_submit_listen: '127.0.0.1:2000'\npeers:\n  - {id: 1, raft_address: '127.0.0.1:1000', client_submit_address: '127.0.0.1:2000'}\nvips: []\nhealth: {command: [/bin/true], interval_ms: 1000, timeout_ms: 500}\ncluster_secret: runtime-authority-fixture-key-0123456789\ndry_run: true\n",
        ).unwrap())
    }

    #[tokio::test(start_paused = true)]
    async fn verified_admission_installs_only_its_exact_runtime_and_expires_terminally() {
        let cfg = config();
        let replica = ReplicaId {
            physical_id: 1,
            boot_nonce: [7; 32],
        };
        let timing = LeaseTiming::new(Duration::from_secs(5), Duration::from_secs(1)).unwrap();
        let boot = Instant::now();
        let authority = RuntimeAuthority::new(replica, timing, boot).unwrap();
        let controller =
            AdmissionController::new(cfg.clone(), replica, boot, Arc::new(timing)).unwrap();
        let genesis = Genesis {
            config: cfg.cluster_config_fingerprint().unwrap(),
            epoch: u128::MAX,
            voters: [replica].into(),
        };
        assert!(authority.current().is_none());
        tokio::time::advance(timing.restart_quarantine()).await;
        let mut round = controller
            .begin(genesis.clone(), AdmissionMode::Cold)
            .unwrap();
        let grant = SignedAdmission::sign(
            cfg.cluster_secret.as_deref(),
            11,
            replica,
            replica,
            [3; 32],
            round.request().clone(),
        )
        .unwrap();
        let grant = crate::raft::admission::ReceivedGrant::authenticate(
            cfg.cluster_secret.as_deref(),
            replica,
            replica,
            [3; 32],
            round.request(),
            grant,
        )
        .unwrap();
        let verified = controller.complete(&mut round, &[grant]).unwrap();
        let foreign = RuntimeAuthority::new(
            ReplicaId {
                boot_nonce: [8; 32],
                ..replica
            },
            timing,
            boot,
        )
        .unwrap();
        assert!(foreign.accept(&verified).is_err());
        let session = authority.accept(&verified).unwrap();
        assert_eq!(session.context().local_replica, replica);
        assert_eq!(session.context().genesis, genesis);
        assert_eq!(session.check().unwrap(), verified.deadline());
        assert!(authority.accept(&verified).is_err());
        tokio::time::advance(timing.consumer_use()).await;
        assert!(session.check().is_err());
        authority.wait_until_sealed().await;
        assert!(authority.accept(&verified).is_err());
    }

    fn verified_join(
        controller: &AdmissionController,
        cfg: &Config,
        prepared: &crate::raft::admission::PreparedJoin,
    ) -> VerifiedAdmission {
        let mut round = controller
            .begin(
                prepared.plan.genesis.clone(),
                AdmissionMode::Join { prepared: None },
            )
            .unwrap();
        round.bind_prepared(prepared).unwrap();
        let grants = prepared
            .plan
            .previous_voters
            .iter()
            .map(|issuer| {
                let record = SignedAdmission::sign(
                    cfg.cluster_secret.as_deref(),
                    11,
                    *issuer,
                    prepared.plan.consumer,
                    [3; 32],
                    round.request().clone(),
                )
                .unwrap();
                crate::raft::admission::ReceivedGrant::authenticate(
                    cfg.cluster_secret.as_deref(),
                    *issuer,
                    prepared.plan.consumer,
                    [3; 32],
                    round.request(),
                    record,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        controller.complete(&mut round, &grants).unwrap()
    }

    fn join_operation(cfg: &Config) -> crate::raft::admission::PreparedJoin {
        use crate::raft::admission::{JoinPlan, PreparedJoin};
        let previous = ReplicaId {
            physical_id: 1,
            boot_nonce: [6; 32],
        };
        let consumer = ReplicaId {
            physical_id: 1,
            boot_nonce: [7; 32],
        };
        PreparedJoin {
            plan: JoinPlan {
                genesis: Genesis {
                    config: cfg.cluster_config_fingerprint().unwrap(),
                    epoch: 9,
                    voters: [previous].into(),
                },
                consumer,
                request_nonce: [11; 32],
                previous_voters: [previous].into(),
                next_voters: [consumer].into(),
            },
            prepared: openraft::testing::log_id::<crate::raft::TypeConfig>(1, previous, 3),
            learner_applied: Some(openraft::testing::log_id::<crate::raft::TypeConfig>(
                1, previous, 5,
            )),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn promotion_uses_completed_operation_after_a_later_membership() {
        use crate::raft::admission::CompletedJoin;
        let mut cfg = (*config()).clone();
        for id in [2, 3] {
            cfg.peers.push(crate::config::PeerConfig {
                id,
                raft_address: format!("127.0.0.1:{}", 1000 + id),
                client_submit_address: format!("127.0.0.1:{}", 2000 + id),
            });
        }
        let cfg = Arc::new(cfg);
        let mut prepared = join_operation(&cfg);
        let old_other = ReplicaId {
            physical_id: 2,
            boot_nonce: [6; 32],
        };
        let third = ReplicaId {
            physical_id: 3,
            boot_nonce: [6; 32],
        };
        prepared.plan.previous_voters.extend([old_other, third]);
        prepared.plan.genesis.voters = prepared.plan.previous_voters.clone();
        prepared.plan.next_voters.extend([old_other, third]);
        let local = prepared.plan.consumer;
        let timing = LeaseTiming::new(Duration::from_secs(5), Duration::from_secs(1)).unwrap();
        let boot = Instant::now();
        let authority = RuntimeAuthority::new(local, timing, boot).unwrap();
        let controller =
            AdmissionController::new(cfg.clone(), local, boot, Arc::new(timing)).unwrap();
        tokio::time::advance(timing.restart_quarantine()).await;
        let verified = verified_join(&controller, &cfg, &prepared);
        let session = authority.accept(&verified).unwrap();
        let (_, _, storage) = crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
        let mut state = storage.write().await;
        state.bind_admission(session).unwrap();
        state.genesis = Some(prepared.plan.genesis.clone());
        let promoted = openraft::testing::log_id::<crate::raft::TypeConfig>(1, local, 6);
        let later = openraft::testing::log_id::<crate::raft::TypeConfig>(2, local, 9);
        state.completed_joins.insert(
            local.physical_id,
            CompletedJoin {
                prepared: prepared.clone(),
                promoted,
            },
        );
        let other = ReplicaId {
            physical_id: 2,
            boot_nonce: [8; 32],
        };
        let voters = std::collections::BTreeSet::from([local, other, third]);
        state.last_membership = openraft::StoredMembership::new(
            Some(later),
            openraft::Membership::new_with_defaults(vec![voters.clone()], voters.clone()),
        );
        state.last_applied_log = Some(later);
        let mut unrelated = prepared.clone();
        unrelated.plan.consumer = ReplicaId {
            boot_nonce: [9; 32],
            ..third
        };
        unrelated.plan.previous_voters = voters.clone();
        unrelated.plan.next_voters = [local, other, unrelated.plan.consumer].into();
        unrelated.plan.validate().unwrap();
        state.prepared_join = Some(unrelated);
        for case in [
            "nonce",
            "binding",
            "promotion",
            "future",
            "conflicting_log",
            "unapplied",
            "missing",
            "replaced",
        ] {
            let receipt = state.completed_joins[&local.physical_id].clone();
            let membership = state.last_membership.clone();
            let mut evidence = prepared.clone();
            match case {
                "nonce" => {
                    state
                        .completed_joins
                        .get_mut(&local.physical_id)
                        .unwrap()
                        .prepared
                        .plan
                        .request_nonce[0] ^= 1
                }
                "binding" => {
                    evidence.plan.request_nonce[0] ^= 1;
                    state
                        .completed_joins
                        .get_mut(&local.physical_id)
                        .unwrap()
                        .prepared = evidence.clone();
                }
                "promotion" => {
                    state
                        .completed_joins
                        .get_mut(&local.physical_id)
                        .unwrap()
                        .promoted = prepared.learner_applied.unwrap()
                }
                "future" => {
                    state
                        .completed_joins
                        .get_mut(&local.physical_id)
                        .unwrap()
                        .promoted
                        .index = later.index + 1
                }
                "conflicting_log" => {
                    state
                        .completed_joins
                        .get_mut(&local.physical_id)
                        .unwrap()
                        .promoted =
                        openraft::testing::log_id::<crate::raft::TypeConfig>(3, local, later.index)
                }
                "unapplied" => state.last_applied_log = Some(prepared.prepared),
                "missing" => {
                    state.completed_joins.remove(&local.physical_id);
                }
                _ => {
                    state.last_membership = openraft::StoredMembership::new(
                        Some(later),
                        openraft::Membership::new_with_defaults(vec![[other].into()], [other]),
                    )
                }
            }
            assert!(
                authority.mark_promoted(&evidence, &state).is_err(),
                "{case}"
            );
            state.completed_joins.insert(local.physical_id, receipt);
            state.last_membership = membership;
            state.last_applied_log = Some(later);
        }
        authority
            .mark_promoted(&prepared, &state)
            .expect("the exact completed operation remains valid after an unrelated membership");
        assert_eq!(authority.current().unwrap().1, verified.deadline());
    }

    #[tokio::test(start_paused = true)]
    async fn verified_join_extends_only_the_same_live_learner_operation() {
        let cfg = config();
        let prepared = join_operation(&cfg);
        let local = prepared.plan.consumer;
        let timing = LeaseTiming::new(Duration::from_secs(5), Duration::from_secs(1)).unwrap();
        let boot = Instant::now();
        let authority = RuntimeAuthority::new(local, timing, boot).unwrap();
        let controller =
            AdmissionController::new(cfg.clone(), local, boot, Arc::new(timing)).unwrap();
        tokio::time::advance(timing.restart_quarantine()).await;
        let first = verified_join(&controller, &cfg, &prepared);
        let session = authority.accept(&first).unwrap();
        assert!(authority.accept(&first).is_err());
        tokio::time::advance(Duration::from_secs(1)).await;
        let fresh = verified_join(&controller, &cfg, &prepared);
        assert_ne!(first.request_nonce(), fresh.request_nonce());
        authority
            .accept(&fresh)
            .expect("same live learner must accept a fresh challenge");
        assert_eq!(session.check().unwrap(), fresh.deadline());
        let mut other = prepared.clone();
        other.prepared.index += 1;
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(
            authority
                .accept(&verified_join(&controller, &cfg, &other))
                .is_err()
        );
        assert_eq!(session.check().unwrap(), fresh.deadline());
        authority.seal();
        assert!(
            authority
                .accept(&verified_join(&controller, &cfg, &prepared))
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn verified_join_extension_rejects_another_plan_at_the_same_log_id() {
        let cfg = config();
        let prepared = join_operation(&cfg);
        let timing = LeaseTiming::new(Duration::from_secs(5), Duration::from_secs(1)).unwrap();
        let boot = Instant::now();
        let authority = RuntimeAuthority::new(prepared.plan.consumer, timing, boot).unwrap();
        let controller =
            AdmissionController::new(cfg.clone(), prepared.plan.consumer, boot, Arc::new(timing))
                .unwrap();
        tokio::time::advance(timing.restart_quarantine()).await;
        let first = verified_join(&controller, &cfg, &prepared);
        authority.accept(&first).unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        let mut changed = prepared;
        changed.plan.request_nonce[0] ^= 1;
        assert!(
            authority
                .accept(&verified_join(&controller, &cfg, &changed))
                .is_err()
        );
        assert_eq!(authority.current().unwrap().1, first.deadline());
    }

    #[tokio::test(start_paused = true)]
    async fn promotion_requires_exact_applied_final_membership_and_blocks_join_renewal() {
        let cfg = config();
        let prepared = join_operation(&cfg);
        let local = prepared.plan.consumer;
        let timing = LeaseTiming::new(Duration::from_secs(5), Duration::from_secs(1)).unwrap();
        let boot = Instant::now();
        let authority = RuntimeAuthority::new(local, timing, boot).unwrap();
        let controller =
            AdmissionController::new(cfg.clone(), local, boot, Arc::new(timing)).unwrap();
        tokio::time::advance(timing.restart_quarantine()).await;
        let first = verified_join(&controller, &cfg, &prepared);
        let session = authority.accept(&first).unwrap();
        let (_, _, state) = crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
        let mut state = state.write().await;
        state.bind_admission(session).unwrap();
        state.genesis = Some(prepared.plan.genesis.clone());
        let mut final_log = prepared.prepared;
        final_log.index = 6;
        let final_membership = openraft::StoredMembership::new(
            Some(final_log),
            openraft::Membership::new_with_defaults(
                vec![prepared.plan.next_voters.clone()],
                prepared.plan.next_voters.clone(),
            ),
        );
        state.last_membership = final_membership.clone();
        state.last_applied_log = Some(final_log);
        let completed = crate::raft::admission::CompletedJoin {
            prepared: prepared.clone(),
            promoted: final_log,
        };
        state
            .completed_joins
            .insert(local.physical_id, completed.clone());
        for case in [
            "ack",
            "barrier",
            "pending",
            "genesis",
            "membership",
            "applied",
            "receipt",
        ] {
            let mut evidence = prepared.clone();
            match case {
                "ack" => evidence.learner_applied = None,
                "barrier" => evidence.learner_applied = Some(final_log),
                "pending" => state.prepared_join = Some(prepared.clone()),
                "genesis" => state.genesis = None,
                "receipt" => {
                    state.completed_joins.remove(&local.physical_id);
                }
                "membership" => {
                    state.last_membership = openraft::StoredMembership::new(
                        Some(final_log),
                        openraft::Membership::new_with_defaults(
                            vec![
                                prepared.plan.previous_voters.clone(),
                                prepared.plan.next_voters.clone(),
                            ],
                            prepared
                                .plan
                                .previous_voters
                                .union(&prepared.plan.next_voters)
                                .copied(),
                        ),
                    )
                }
                _ => state.last_applied_log = Some(prepared.prepared),
            }
            assert!(
                authority.mark_promoted(&evidence, &state).is_err(),
                "{case}"
            );
            assert_eq!(authority.current().unwrap().1, first.deadline());
            state.prepared_join = None;
            state.genesis = Some(prepared.plan.genesis.clone());
            state.last_membership = final_membership.clone();
            state.last_applied_log = Some(final_log);
            state
                .completed_joins
                .insert(local.physical_id, completed.clone());
        }
        authority.mark_promoted(&prepared, &state).unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(
            authority
                .accept(&verified_join(&controller, &cfg, &prepared))
                .is_err()
        );
        assert_eq!(authority.current().unwrap().1, first.deadline());
        authority.seal();
        assert!(authority.mark_promoted(&prepared, &state).is_err());
    }
}
