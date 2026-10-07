//! Admission-controller boundary and exhaustive Raft identity checks.

#[path = "management.rs"]
pub mod management;

mod replay;
mod snapshot;
#[cfg(test)]
mod snapshot_bootstrap_tests;
mod state;
pub use replay::ReplicationConflict;

use management::{ManagementRequest, ManagementResponse};

use super::request::RaftRequest;
use crate::raft::admission::{
    AdmissionCommand, AppliedHealthProgress, Genesis, JoinPlan, PreparedJoin,
};
pub use crate::raft::admission::{AdmissionDenied, AdmissionSession, ReplicaId};
use crate::raft::admission::{AdmissionRequest, SignedAdmission};
use crate::raft::{KafRequest, TypeConfig};
use futures::future::BoxFuture;
use openraft::alias::{LogIdOf, StoredMembershipOf, VoteOf};
use openraft::raft::{AppendEntriesResponse, SnapshotResponse, VoteResponse};
use openraft::vote::RaftLeaderId;
use openraft::{BasicNode, EntryPayload, Membership};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

/// The controller owns mode semantics, round nonces and receipt validation.
pub type AdmissionRpc = SignedAdmission<AdmissionRequest>;

/// No permissive implementation exists: composition must supply real authority.
pub trait AdmissionController: Send + Sync {
    fn local_replica(&self) -> ReplicaId;
    fn authorize_raft(&self, peer: ReplicaId) -> Result<RaftAuthorization, AdmissionDenied>;
    /// Transport callers bound this wait by their existing RPC deadline.
    fn authorize_raft_async(
        &self,
        peer: ReplicaId,
    ) -> BoxFuture<'_, Result<RaftAuthorization, AdmissionDenied>> {
        Box::pin(async move { self.authorize_raft(peer) })
    }
    fn dispatch(
        &self,
        peer: ReplicaId,
        channel_binding: [u8; 32],
        request: AdmissionRpc,
    ) -> BoxFuture<'_, anyhow::Result<AdmissionRpc>>;
    fn management(
        &self,
        _peer: ReplicaId,
        _binding: [u8; 32],
        _request: SignedAdmission<ManagementRequest>,
    ) -> BoxFuture<'_, anyhow::Result<SignedAdmission<ManagementResponse>>> {
        Box::pin(async { anyhow::bail!("admission controller does not support management") })
    }
}

/// One coherent authorization view, including explicitly permitted historical identities.
pub struct RaftAuthorization {
    pub session: AdmissionSession,
    pub peer: ReplicaId,
    /// Physical roster from local configuration, never inferred from a peer's data.
    pub configured_physical_ids: BTreeSet<u64>,
    /// Exact boots with current voting permission, excluding historical-only identities.
    pub voters: BTreeSet<ReplicaId>,
    /// Known history for diagnostics, never an authority source or an exhaustive replay list.
    pub log_replicas: BTreeSet<ReplicaId>,
    /// Exact historical and committed-barrier transitions supplied by the controller.
    pub memberships: Vec<Membership<ReplicaId, BasicNode>>,
    pub committed_membership: StoredMembershipOf<TypeConfig>,
    pub last_applied: Option<LogIdOf<TypeConfig>>,
    pub prepared_join: Option<PreparedJoin>,
    pub(crate) verified_bootstrap: Option<crate::raft::admission::VerifiedJoinBootstrap>,
    pub(crate) replication: Option<replay::ReplicationHistory>,
}

impl RaftAuthorization {
    pub(super) fn check(&self, local: ReplicaId, peer: ReplicaId) -> anyhow::Result<()> {
        self.session.check()?;
        anyhow::ensure!(
            self.session.context().local_replica == local && self.peer == peer,
            "admission authorization belongs to another boot"
        );
        Ok(())
    }

    fn vote(&self, vote: &VoteOf<TypeConfig>) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.voters.contains(vote.leader_id().node_id())
                && self
                    .configured_physical_ids
                    .contains(&vote.leader_id().node_id().physical_id),
            "Raft vote names an unauthorized replica"
        );
        Ok(())
    }

    fn sender_vote(&self, sender: ReplicaId, vote: &VoteOf<TypeConfig>) -> anyhow::Result<()> {
        anyhow::ensure!(
            *vote.leader_id().node_id() == sender,
            "Raft sender differs from the authenticated boot"
        );
        self.vote(vote)
    }

    fn log(&self, log: Option<&LogIdOf<TypeConfig>>) -> anyhow::Result<()> {
        if let Some(log) = log {
            anyhow::ensure!(
                self.configured_physical_ids
                    .contains(&log.leader_id.node_id().physical_id),
                "Raft log names an unauthorized replica"
            );
        }
        Ok(())
    }

    fn membership(&self, membership: &Membership<ReplicaId, BasicNode>) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.memberships.contains(membership)
                && membership.nodes().all(|(replica, _)| self
                    .configured_physical_ids
                    .contains(&replica.physical_id)),
            "Raft membership is outside the authorized transition"
        );
        Ok(())
    }

    fn stored_membership(&self, membership: &StoredMembershipOf<TypeConfig>) -> anyhow::Result<()> {
        self.log(membership.log_id().as_ref())?;
        self.membership(membership.membership())
    }

    fn stable_previous(membership: &Membership<ReplicaId, BasicNode>, plan: &JoinPlan) -> bool {
        let configs = membership.get_joint_config();
        configs.len() == 1 && configs[0] == plan.previous_voters
    }

    fn join_plan(&self, plan: &JoinPlan, historical: bool) -> anyhow::Result<()> {
        self.join_plan_data(plan)?;
        if !historical && let Some(pending) = &self.prepared_join {
            anyhow::ensure!(pending.plan == *plan, "another committed join is pending");
            return Ok(());
        }
        anyhow::ensure!(
            if historical {
                self.memberships
                    .iter()
                    .any(|membership| Self::stable_previous(membership, plan))
            } else {
                Self::stable_previous(self.committed_membership.membership(), plan)
                    && self
                        .memberships
                        .contains(self.committed_membership.membership())
            },
            "join plan requires the exact authorized stable previous membership"
        );
        Ok(())
    }

    fn join_plan_data(&self, plan: &JoinPlan) -> anyhow::Result<()> {
        plan.validate()?;
        plan.genesis
            .validate_roster(&self.configured_physical_ids)?;
        anyhow::ensure!(
            plan.genesis == self.session.context().genesis,
            "join plan differs from admission"
        );
        anyhow::ensure!(
            plan.previous_voters
                .iter()
                .chain(&plan.next_voters)
                .all(|replica| { self.configured_physical_ids.contains(&replica.physical_id) }),
            "join plan names an unconfigured physical member"
        );
        Ok(())
    }

    fn command(&self, command: &KafRequest, log: &LogIdOf<TypeConfig>) -> anyhow::Result<()> {
        let context = self.session.context();
        let historical = self
            .last_applied
            .as_ref()
            .is_some_and(|applied| log.index <= applied.index);
        match command {
            KafRequest::AdmissionMembership(command) => match command {
                AdmissionCommand::PrepareJoin(plan) => self.join_plan(plan, historical)?,
                AdmissionCommand::CancelJoin { plan, prepared } => {
                    self.join_plan_data(plan)?;
                    self.log(Some(prepared))?;
                    anyhow::ensure!(
                        log.index > prepared.index,
                        "cancellation precedes preparation"
                    );
                    if !historical {
                        let pending = self
                            .prepared_join
                            .as_ref()
                            .ok_or_else(|| anyhow::anyhow!("no committed join is pending"))?;
                        let membership = self.committed_membership.membership();
                        anyhow::ensure!(
                            pending.plan == *plan
                                && pending.prepared == *prepared
                                && Self::stable_previous(membership, plan)
                                && !membership.nodes().any(|(id, _)| *id == plan.consumer),
                            "cancellation differs from the committed pending operation or removed learner"
                        );
                    }
                }
                AdmissionCommand::LearnerApplied {
                    consumer,
                    request_nonce,
                    prepared,
                } => {
                    anyhow::ensure!(
                        self.configured_physical_ids.contains(&consumer.physical_id),
                        "learner names an unauthorized replica"
                    );
                    self.log(Some(prepared))?;
                    anyhow::ensure!(
                        log.index > prepared.index,
                        "learner acknowledgement precedes preparation"
                    );
                    if !historical {
                        let pending = self
                            .prepared_join
                            .as_ref()
                            .ok_or_else(|| anyhow::anyhow!("no committed join is pending"))?;
                        self.join_plan_data(&pending.plan)?;
                        anyhow::ensure!(
                            pending.plan.consumer == *consumer
                                && pending.plan.request_nonce == *request_nonce
                                && pending.prepared == *prepared,
                            "learner acknowledgement differs from committed preparation"
                        );
                        let membership = self.committed_membership.membership();
                        anyhow::ensure!(
                            pending.learner_applied.is_some()
                                || (Self::stable_previous(membership, &pending.plan)
                                    && membership.learner_ids().any(|id| id == *consumer)),
                            "learner acknowledgement requires the committed learner stage"
                        );
                    }
                }
            },
            KafRequest::AdmissionGenesis(genesis) => {
                anyhow::ensure!(
                    genesis == &context.genesis,
                    "Raft genesis differs from admission"
                );
            }
            KafRequest::HealthProgress(progress) => {
                progress.validate()?;
                anyhow::ensure!(
                    progress.genesis == context.genesis
                        && self
                            .configured_physical_ids
                            .contains(&progress.replica.physical_id),
                    "Raft progress differs from admission"
                );
            }
            KafRequest::ClusterFormed { cluster_id, .. } => {
                anyhow::ensure!(
                    *cluster_id == context.genesis.epoch,
                    "Raft epoch differs from admission"
                );
            }
            KafRequest::HealthUpdate { .. }
            | KafRequest::VipReleased { .. }
            | KafRequest::EnableFailoverSemanticsV2
            | KafRequest::EnableConfigIdentityV1 => {}
        }
        Ok(())
    }

    pub(super) fn validate_request(
        &self,
        sender: ReplicaId,
        request: &RaftRequest,
    ) -> anyhow::Result<()> {
        self.session.check()?;
        match request {
            RaftRequest::Status(_)
            | RaftRequest::Admission(_)
            | RaftRequest::AdmissionControl(_) => {
                anyhow::bail!("discovery is not Raft authorization")
            }
            RaftRequest::Vote(req) | RaftRequest::PreVote(req) => {
                self.sender_vote(sender, &req.vote)?;
                self.log(req.last_log_id.as_ref())?;
            }
            RaftRequest::AppendEntries(req) => {
                self.sender_vote(sender, &req.vote)?;
                self.log(req.prev_log_id.as_ref())?;
                self.log(req.leader_commit.as_ref())?;
                if let Some(history) = &self.replication {
                    return history.validate(self, req);
                }
                for entry in &req.entries {
                    self.log(Some(&entry.log_id))?;
                    match &entry.payload {
                        EntryPayload::Membership(membership) => self.membership(membership)?,
                        EntryPayload::Normal(command) => self.command(command, &entry.log_id)?,
                        EntryPayload::Blank => {}
                    }
                }
            }
            RaftRequest::Snapshot(req) => {
                self.sender_vote(sender, &req.vote)?;
                self.snapshot(req)?;
            }
        }
        Ok(())
    }

    pub(super) fn validate_response(&self, bytes: &[u8]) -> anyhow::Result<()> {
        self.session.check()?;
        match serde_json::from_slice::<RaftResponse>(bytes)? {
            RaftResponse::Vote(response) | RaftResponse::PreVote(response) => {
                self.vote(&response.vote)?;
                self.log(response.last_log_id.as_ref())?;
            }
            RaftResponse::InstallSnapshot(response) => self.vote(&response.vote)?,
            RaftResponse::AppendEntries(response) => match response {
                AppendEntriesResponse::HigherVote(vote) => self.vote(&vote)?,
                AppendEntriesResponse::PartialSuccess(log) => self.log(log.as_ref())?,
                AppendEntriesResponse::Success | AppendEntriesResponse::Conflict => {}
            },
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum RaftResponse {
    PreVote(VoteResponse<TypeConfig>),
    Vote(VoteResponse<TypeConfig>),
    AppendEntries(AppendEntriesResponse<TypeConfig>),
    InstallSnapshot(SnapshotResponse<TypeConfig>),
}
