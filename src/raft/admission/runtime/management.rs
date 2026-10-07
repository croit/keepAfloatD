//! Authenticated management mutations with committed membership barriers.

use super::*;
use crate::raft::admission::{AdmissionCommand, HealthProgress, JoinPlan};
use crate::raft::types::{KafRequest, KafResponse, TypeConfig};
use openraft::alias::LogIdOf;
use openraft::async_runtime::WatchReceiver;

impl RuntimeDriver {
    pub(super) async fn manage(
        &self,
        peer: ReplicaId,
        action: ManagementAction,
    ) -> anyhow::Result<ManagementResult> {
        if matches!(action, ManagementAction::Discover) {
            let state = self.state.read().await;
            self.ensure_running()?;
            let genesis = state.genesis.clone().or_else(|| {
                state
                    .admission
                    .as_ref()
                    .filter(|session| session.check().is_ok())
                    .map(|session| session.context().genesis.clone())
            });
            return Ok(ManagementResult::Discovery {
                genesis,
                voters: state.last_membership.membership().voter_ids().collect(),
            });
        }
        let _mutation = self.mutation.lock().await;
        let session = self.coherent_session().await?;
        let raft = self.raft()?;
        let leader = raft.metrics().borrow_watched().current_leader;
        if leader != Some(self.local) {
            return leader
                .map(ManagementResult::Redirect)
                .ok_or_else(|| anyhow::anyhow!("management has no elected leader"));
        }
        session.check()?;
        match action {
            ManagementAction::Discover => {
                anyhow::bail!("discovery reached the mutation dispatcher")
            }
            ManagementAction::PrepareJoin {
                genesis,
                operation_nonce,
            } => {
                anyhow::ensure!(
                    genesis == session.context().genesis,
                    "join genesis differs from authority"
                );
                let existing = {
                    let state = self.state.read().await;
                    anyhow::ensure!(
                        state.genesis.as_ref() == Some(&genesis),
                        "join requires committed genesis"
                    );
                    state.prepared_join.clone()
                };
                let prepared = if let Some(prepared) = existing {
                    anyhow::ensure!(
                        prepared.plan.consumer == peer
                            && prepared.plan.request_nonce == operation_nonce
                            && prepared.plan.genesis == genesis,
                        "another join operation is pending"
                    );
                    prepared
                } else {
                    let previous_voters = {
                        let state = self.state.read().await;
                        anyhow::ensure!(
                            state.last_membership.membership().get_joint_config().len() == 1,
                            "join requires committed stable membership"
                        );
                        state
                            .last_membership
                            .membership()
                            .voter_ids()
                            .collect::<BTreeSet<_>>()
                    };
                    let mut next_voters = previous_voters.clone();
                    next_voters.retain(|id| id.physical_id != peer.physical_id);
                    next_voters.insert(peer);
                    let plan = JoinPlan {
                        genesis,
                        consumer: peer,
                        request_nonce: operation_nonce,
                        previous_voters,
                        next_voters,
                    };
                    plan.validate()?;
                    self.commit(
                        &raft,
                        KafRequest::AdmissionMembership(AdmissionCommand::PrepareJoin(
                            plan.clone(),
                        )),
                    )
                    .await?;
                    let state = self.state.read().await;
                    let prepared = state
                        .prepared_join
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("join preparation not applied locally"))?;
                    anyhow::ensure!(prepared.plan == plan, "applied join preparation changed");
                    prepared.clone()
                };
                session.check()?;
                let learner_present = self
                    .state
                    .read()
                    .await
                    .last_membership
                    .membership()
                    .nodes()
                    .any(|(id, _)| *id == peer);
                if !learner_present {
                    raft.add_learner(peer, self.node(peer)?, false).await?;
                }
                tracing::debug!(consumer = %peer, "committed learner preparation available");
                Ok(ManagementResult::Prepared(Box::new(prepared)))
            }
            ManagementAction::LearnerReady {
                genesis,
                operation_nonce,
                prepared,
            } => {
                anyhow::ensure!(
                    genesis == session.context().genesis,
                    "learner genesis differs from authority"
                );
                if let Some(completed) = self
                    .state
                    .read()
                    .await
                    .completed_joins
                    .get(&peer.physical_id)
                    .filter(|completed| {
                        completed.prepared.plan.consumer == peer
                            && completed.prepared.plan.genesis == genesis
                            && completed.prepared.plan.request_nonce == operation_nonce
                            && completed.prepared.prepared == prepared
                    })
                {
                    return Ok(ManagementResult::Prepared(Box::new(
                        completed.prepared.clone(),
                    )));
                }
                let mut pending = self
                    .state
                    .read()
                    .await
                    .prepared_join
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("learner operation is no longer pending"))?;
                anyhow::ensure!(
                    pending.plan.consumer == peer
                        && pending.plan.genesis == genesis
                        && pending.plan.request_nonce == operation_nonce
                        && pending.prepared == prepared,
                    "learner acknowledgement differs from committed preparation"
                );
                let caught_up = raft
                    .metrics()
                    .borrow_watched()
                    .replication
                    .as_ref()
                    .and_then(|replication| replication.get(&peer))
                    .and_then(|log| *log)
                    .is_some_and(|log| log.index >= prepared.index);
                anyhow::ensure!(caught_up, "learner has not replicated its preparation");
                if pending.learner_applied.is_none() {
                    let applied = self
                        .commit(
                            &raft,
                            KafRequest::AdmissionMembership(AdmissionCommand::LearnerApplied {
                                consumer: peer,
                                request_nonce: operation_nonce,
                                prepared,
                            }),
                        )
                        .await?;
                    pending.learner_applied = Some(applied);
                }
                session.check()?;
                {
                    let state = self.state.read().await;
                    anyhow::ensure!(
                        state.prepared_join.as_ref() == Some(&pending),
                        "learner acknowledgement is not committed locally"
                    );
                }
                raft.change_membership(pending.plan.next_voters.clone(), false)
                    .await?;
                session.check()?;
                Ok(ManagementResult::Prepared(Box::new(pending)))
            }
            ManagementAction::Progress(progress) => {
                self.apply_progress(&raft, peer, progress).await
            }
            ManagementAction::CancelJoin { plan, prepared } => {
                anyhow::ensure!(
                    plan.genesis == session.context().genesis,
                    "cancellation genesis differs from authority"
                );
                self.cancel_join(&raft, peer, plan, prepared).await
            }
        }
    }

    async fn apply_progress(
        &self,
        raft: &KafRaft,
        peer: ReplicaId,
        progress: HealthProgress,
    ) -> anyhow::Result<ManagementResult> {
        progress.validate()?;
        anyhow::ensure!(
            progress.replica == peer,
            "progress differs from authenticated boot"
        );
        {
            let state = self.state.read().await;
            anyhow::ensure!(
                state.genesis.as_ref() == Some(&progress.genesis),
                "progress requires committed genesis"
            );
            anyhow::ensure!(
                state
                    .last_membership
                    .membership()
                    .nodes()
                    .any(|(id, _)| *id == peer),
                "progress sender is not a committed member"
            );
        }
        Ok(ManagementResult::Applied(
            self.commit(raft, KafRequest::HealthProgress(progress))
                .await?,
        ))
    }

    pub(super) async fn commit(
        &self,
        raft: &KafRaft,
        request: KafRequest,
    ) -> anyhow::Result<LogIdOf<TypeConfig>> {
        self.coherent_session().await?.check()?;
        let response = raft.client_write(request).await?;
        match response.data {
            KafResponse::Ok => Ok(response.log_id),
            KafResponse::Rejected(reason) => anyhow::bail!("admission command rejected: {reason}"),
        }
    }

    pub(super) fn node(&self, replica: ReplicaId) -> anyhow::Result<openraft::BasicNode> {
        self.cfg
            .get_peer(replica.physical_id)
            .map(|peer| openraft::BasicNode {
                addr: peer.raft_address.clone(),
            })
            .ok_or_else(|| anyhow::anyhow!("replica physical identity is not configured"))
    }
}
