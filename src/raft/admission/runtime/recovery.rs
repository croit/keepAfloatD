//! Resume committed promotion or cancel a stalled learner through consensus.

use super::*;
use crate::raft::admission::{AdmissionCommand, JoinBinding, JoinPlan};
use crate::raft::types::{KafRequest, TypeConfig};
use openraft::ChangeMembers;
use openraft::alias::LogIdOf;
use openraft::async_runtime::WatchReceiver;

pub(super) struct PendingProgress {
    operation: JoinBinding,
    matched_index: Option<u64>,
    acknowledged_index: Option<u64>,
    since: Instant,
}

impl RuntimeDriver {
    pub(super) fn observe_pending(
        &self,
        pending: &PreparedJoin,
        matched_index: Option<u64>,
    ) -> anyhow::Result<bool> {
        let mut observed = self
            .pending_progress
            .lock()
            .map_err(|_| AdmissionDenied("pending progress lock poisoned"))?;
        let binding = JoinBinding::from_prepared(pending);
        let acknowledged_index = pending.learner_applied.map(|log| log.index);
        if let Some(previous) = observed.as_mut()
            && previous.operation == binding
        {
            if matched_index > previous.matched_index
                || acknowledged_index > previous.acknowledged_index
            {
                previous.matched_index = previous.matched_index.max(matched_index);
                previous.acknowledged_index = previous.acknowledged_index.max(acknowledged_index);
                previous.since = Instant::now();
            }
            return Ok(Instant::now() >= self.timing.reservation_deadline(previous.since)?);
        }
        *observed = Some(PendingProgress {
            operation: binding,
            matched_index,
            acknowledged_index,
            since: Instant::now(),
        });
        Ok(false)
    }

    fn clear_pending_progress(&self) -> anyhow::Result<()> {
        self.pending_progress
            .lock()
            .map_err(|_| AdmissionDenied("pending progress lock poisoned"))?
            .take();
        Ok(())
    }

    pub(super) async fn cancel_join(
        &self,
        raft: &KafRaft,
        peer: ReplicaId,
        plan: JoinPlan,
        prepared: LogIdOf<TypeConfig>,
    ) -> anyhow::Result<ManagementResult> {
        plan.validate()?;
        let learner_present = {
            let state = self.state.read().await;
            let pending = state
                .prepared_join
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no committed join remains to cancel"))?;
            anyhow::ensure!(
                pending.plan == plan
                    && pending.prepared == prepared
                    && state.genesis.as_ref() == Some(&plan.genesis),
                "cancellation differs from committed operation"
            );
            let membership = state.last_membership.membership();
            anyhow::ensure!(
                membership.get_joint_config().len() == 1
                    && membership.voter_ids().collect::<BTreeSet<_>>() == plan.previous_voters,
                "a voting transition cannot be cancelled as a learner"
            );
            anyhow::ensure!(
                peer.physical_id == plan.consumer.physical_id
                    || membership.voter_ids().any(|id| id == peer),
                "only the consumer physical member or an admitted voter may cancel"
            );
            membership.nodes().any(|(id, _)| *id == plan.consumer)
        };
        if learner_present {
            self.coherent_session().await?.check()?;
            raft.change_membership(ChangeMembers::RemoveNodes([plan.consumer].into()), false)
                .await?;
        }
        let log = self
            .commit(
                raft,
                KafRequest::AdmissionMembership(AdmissionCommand::CancelJoin {
                    plan: plan.clone(),
                    prepared,
                }),
            )
            .await?;
        anyhow::ensure!(
            self.state.read().await.prepared_join.is_none(),
            "cancellation was not applied locally"
        );
        tracing::info!(consumer = %plan.consumer, "committed learner removal and join cancellation");
        Ok(ManagementResult::Applied(log))
    }

    pub(super) async fn recover_pending(&self, network: &RaftNetworkImpl) -> anyhow::Result<()> {
        let raft = self.raft()?;
        if raft.metrics().borrow_watched().current_leader != Some(self.local) {
            self.clear_pending_progress()?;
            return Ok(());
        }
        let pending = self.state.read().await.prepared_join.clone();
        let Some(pending) = pending else {
            self.clear_pending_progress()?;
            return Ok(());
        };
        let joint_committed = {
            let state = self.state.read().await;
            let membership = state.last_membership.membership();
            membership.get_joint_config()
                == &vec![
                    pending.plan.previous_voters.clone(),
                    pending.plan.next_voters.clone(),
                ]
                && pending.learner_applied.is_some_and(|ack| {
                    state.last_membership.log_id().is_some_and(|membership| {
                        membership.index > ack.index
                            && state
                                .last_applied_log
                                .is_some_and(|applied| applied.index >= membership.index)
                    })
                })
        };
        if joint_committed {
            self.clear_pending_progress()?;
            let _mutation = self.mutation.lock().await;
            self.coherent_session().await?.check()?;
            {
                let state = self.state.read().await;
                anyhow::ensure!(
                    state.prepared_join.as_ref() == Some(&pending),
                    "pending promotion changed"
                );
            }
            raft.change_membership(pending.plan.next_voters.clone(), false)
                .await?;
            return Ok(());
        }
        let reachable = network
            .management_rpc(
                pending.plan.consumer,
                ManagementAction::Discover,
                self.rpc_budget(),
            )
            .await
            .is_ok();
        let matched_index = {
            let metrics = raft.metrics();
            let metrics = metrics.borrow_watched();
            let membership = metrics.membership_config.membership();
            if membership.get_joint_config().len() != 1
                || membership.voter_ids().collect::<BTreeSet<_>>() != pending.plan.previous_voters
            {
                self.clear_pending_progress()?;
                return Ok(());
            }
            metrics
                .replication
                .as_ref()
                .and_then(|replication| replication.get(&pending.plan.consumer))
                .and_then(|matched| matched.map(|log| log.index))
        };
        let pending = {
            let state = self.state.read().await;
            let Some(current) = state.prepared_join.as_ref().filter(|current| {
                current.plan == pending.plan && current.prepared == pending.prepared
            }) else {
                self.clear_pending_progress()?;
                return Ok(());
            };
            current.clone()
        };
        if !self.observe_pending(&pending, matched_index)? {
            return Ok(());
        }
        tracing::debug!(consumer = %pending.plan.consumer, reachable, ?matched_index,
            "stalled learner selected for committed cancellation");
        // Missing progress selects a proposal; only committed removal and cancellation retire it.
        self.manage(
            self.local,
            ManagementAction::CancelJoin {
                plan: pending.plan,
                prepared: pending.prepared,
            },
        )
        .await?;
        Ok(())
    }
}
