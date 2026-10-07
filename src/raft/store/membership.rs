//! Deterministic admission history and exact-boot membership transitions.

use super::state::KafStorageState;
use crate::raft::admission::{AdmissionCommand, CompletedJoin, PreparedJoin, ReplicaId};
use crate::raft::types::{KafResponse, TypeConfig};
use openraft::alias::LogIdOf;
use openraft::{BasicNode, Membership, StoredMembership};
use std::collections::{BTreeMap, BTreeSet};

/// A joint replacement with two boots for one physical member is not a VIP candidate.
pub(super) fn unique_voters(
    membership: &Membership<ReplicaId, BasicNode>,
) -> BTreeMap<u64, ReplicaId> {
    let mut voters = BTreeMap::new();
    let mut ambiguous = BTreeSet::new();
    for replica in membership.voter_ids() {
        if voters.insert(replica.physical_id, replica).is_some() {
            ambiguous.insert(replica.physical_id);
        }
    }
    voters.retain(|id, _| !ambiguous.contains(id));
    voters
}

impl KafStorageState {
    pub(super) fn apply_admission_membership(
        &mut self,
        command: &AdmissionCommand,
        log_id: LogIdOf<TypeConfig>,
    ) -> KafResponse {
        let membership = self.last_membership.membership();
        match command {
            AdmissionCommand::CancelJoin { plan, prepared } => {
                let Some(pending) = &self.prepared_join else {
                    return KafResponse::Rejected("no join is pending".into());
                };
                if pending.plan != *plan
                    || pending.prepared != *prepared
                    || self.genesis.as_ref() != Some(&plan.genesis)
                    || log_id.index <= prepared.index
                    || membership.get_joint_config().len() != 1
                    || membership.voter_ids().collect::<BTreeSet<_>>() != plan.previous_voters
                    || membership.nodes().any(|(id, _)| *id == plan.consumer)
                {
                    return KafResponse::Rejected(
                        "cancellation requires the exact pending join and removed learner".into(),
                    );
                }
                self.prepared_join = None;
                KafResponse::Ok
            }
            AdmissionCommand::PrepareJoin(plan) => {
                if plan.validate().is_err() || self.genesis.as_ref() != Some(&plan.genesis) {
                    return KafResponse::Rejected(
                        "join plan differs from immutable genesis or replacement".into(),
                    );
                }
                if let Some(pending) = &self.prepared_join {
                    return if pending.plan == *plan {
                        KafResponse::Ok
                    } else {
                        KafResponse::Rejected("another join is pending".into())
                    };
                }
                if membership.get_joint_config().len() != 1
                    || membership.voter_ids().collect::<BTreeSet<_>>() != plan.previous_voters
                {
                    return KafResponse::Rejected(
                        "join requires the exact stable applied voter set".into(),
                    );
                }
                self.prepared_join = Some(PreparedJoin {
                    plan: plan.clone(),
                    prepared: log_id,
                    learner_applied: None,
                });
                KafResponse::Ok
            }
            AdmissionCommand::LearnerApplied {
                consumer,
                request_nonce,
                prepared,
            } => {
                let Some(pending) = &mut self.prepared_join else {
                    return KafResponse::Rejected("no join is pending".into());
                };
                if pending.plan.consumer != *consumer
                    || pending.plan.request_nonce != *request_nonce
                    || pending.prepared != *prepared
                    || self.genesis.as_ref() != Some(&pending.plan.genesis)
                {
                    return KafResponse::Rejected(
                        "learner acknowledgement differs from prepared join".into(),
                    );
                }
                if pending.learner_applied.is_some() {
                    return KafResponse::Ok;
                }
                if !membership.learner_ids().any(|id| id == *consumer)
                    || membership.get_joint_config().len() != 1
                    || membership.voter_ids().collect::<BTreeSet<_>>()
                        != pending.plan.previous_voters
                    || log_id.index <= prepared.index
                {
                    return KafResponse::Rejected(
                        "acknowledgement requires the prepared learner stage".into(),
                    );
                }
                pending.learner_applied = Some(log_id);
                KafResponse::Ok
            }
        }
    }

    pub(super) fn apply_membership(
        &mut self,
        membership: Membership<ReplicaId, BasicNode>,
        log_id: LogIdOf<TypeConfig>,
    ) {
        let before = unique_voters(self.last_membership.membership());
        let after = unique_voters(&membership);
        let physical: BTreeSet<_> = before.keys().chain(after.keys()).copied().collect();
        for id in physical {
            // Physical-only health remains valid until the first admission genesis.
            if self.genesis.is_some() && before.get(&id) != after.get(&id) {
                self.node_health.remove(&id);
                self.node_probe_ticks.remove(&id);
                self.node_recovery_tick.remove(&id);
                self.applied_progress.remove(&id);
            }
        }
        self.applied_progress.retain(|_, progress| {
            membership
                .nodes()
                .any(|(replica, _)| *replica == progress.request.replica)
        });
        if let Some(pending) = &self.prepared_join
            && pending
                .learner_applied
                .is_some_and(|ack| ack.index < log_id.index)
            && membership.get_joint_config().len() == 1
            && membership.voter_ids().collect::<BTreeSet<_>>() == pending.plan.next_voters
        {
            self.completed_joins.insert(
                pending.plan.consumer.physical_id,
                CompletedJoin {
                    prepared: pending.clone(),
                    promoted: log_id,
                },
            );
            tracing::info!(consumer = %pending.plan.consumer, "committed learner promotion");
            self.prepared_join = None;
        }
        self.last_membership = StoredMembership::new(Some(log_id), membership);
    }
}
