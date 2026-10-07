//! Validate snapshot data and ordered admission barriers without granting peer authority.
//!
//! Catch-up across an unknown compacted membership transition requires independently
//! verified chain evidence; completion records supplied by the snapshot are not that evidence.

use super::*;
use crate::raft::admission::CompletedJoin;

impl RaftAuthorization {
    pub(super) fn snapshot_prepared_join(
        &self,
        prepared: &PreparedJoin,
        membership: &StoredMembershipOf<TypeConfig>,
        last_applied: Option<&LogIdOf<TypeConfig>>,
    ) -> anyhow::Result<()> {
        self.join_plan_data(&prepared.plan)?;
        self.log(Some(&prepared.prepared))?;
        self.log(prepared.learner_applied.as_ref())?;
        let applied =
            last_applied.ok_or_else(|| anyhow::anyhow!("prepared snapshot has no applied log"))?;
        anyhow::ensure!(
            prepared.prepared.index <= applied.index,
            "snapshot preparation is not applied"
        );
        let plan = &prepared.plan;
        let configs = membership.membership().get_joint_config();
        let stable = Self::stable_previous(membership.membership(), plan);
        let joint = configs.len() == 2
            && configs[0] == plan.previous_voters
            && configs[1] == plan.next_voters;
        anyhow::ensure!(
            stable || joint,
            "snapshot preparation differs from its membership"
        );
        let learner = membership
            .membership()
            .learner_ids()
            .any(|id| id == plan.consumer);
        let membership_log = membership.log_id().as_ref();
        if learner || joint {
            anyhow::ensure!(
                membership_log.is_some_and(|log| log.index > prepared.prepared.index),
                "snapshot learner membership must follow preparation"
            );
        }
        if let Some(ack) = &prepared.learner_applied {
            anyhow::ensure!(
                ack.index > prepared.prepared.index && ack.index <= applied.index,
                "snapshot learner acknowledgement is not applied after preparation"
            );
            anyhow::ensure!(
                membership_log.is_some_and(|log| {
                    (stable && learner && log.index < ack.index)
                        || ((joint || (stable && !learner)) && ack.index < log.index)
                }),
                "snapshot acknowledgement differs from its committed learner stage"
            );
        } else {
            anyhow::ensure!(!joint, "snapshot promotion lacks a learner acknowledgement");
        }
        Ok(())
    }

    pub(super) fn snapshot(
        &self,
        req: &crate::raft::network::SnapshotTransfer,
    ) -> anyhow::Result<()> {
        self.session.check()?;
        self.log(req.meta.last_log_id.as_ref())?;
        self.log(req.meta.last_membership.log_id().as_ref())?;
        let snapshot: SnapshotIdentities = serde_json::from_slice(&req.data)?;
        anyhow::ensure!(
            snapshot.last_applied == req.meta.last_log_id
                && snapshot.last_membership == req.meta.last_membership,
            "snapshot body and metadata identities differ"
        );
        anyhow::ensure!(
            snapshot.genesis.as_ref() == Some(&self.session.context().genesis)
                && snapshot.cluster_epoch == Some(self.session.context().genesis.epoch),
            "snapshot genesis differs from admission"
        );
        if let Some(membership_log) = snapshot.last_membership.log_id() {
            anyhow::ensure!(
                snapshot.last_applied.as_ref().is_some_and(|applied| {
                    membership_log.index < applied.index || membership_log == applied
                }),
                "snapshot membership does not match applied history"
            );
        }
        for (physical_id, progress) in &snapshot.applied_progress {
            self.log(Some(&progress.log_id))?;
            self.command(
                &KafRequest::HealthProgress(progress.request.clone()),
                &progress.log_id,
            )?;
            anyhow::ensure!(
                *physical_id == progress.request.node_id
                    && snapshot
                        .last_applied
                        .as_ref()
                        .is_some_and(|applied| progress.log_id.index <= applied.index)
                    && snapshot
                        .last_membership
                        .membership()
                        .nodes()
                        .any(|(replica, _)| *replica == progress.request.replica),
                "snapshot progress differs from its applied membership"
            );
        }
        if let Some(prepared) = &snapshot.prepared_join {
            self.snapshot_prepared_join(
                prepared,
                &snapshot.last_membership,
                snapshot.last_applied.as_ref(),
            )?;
        }
        anyhow::ensure!(
            snapshot.completed_joins.len() <= self.configured_physical_ids.len(),
            "snapshot completion history exceeds the physical roster"
        );
        for (physical_id, completed) in &snapshot.completed_joins {
            let prepared = &completed.prepared;
            self.join_plan_data(&prepared.plan)?;
            self.log(Some(&prepared.prepared))?;
            self.log(prepared.learner_applied.as_ref())?;
            self.log(Some(&completed.promoted))?;
            let ack = prepared
                .learner_applied
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("completed join has no learner acknowledgement"))?;
            anyhow::ensure!(
                *physical_id == prepared.plan.consumer.physical_id
                    && self.configured_physical_ids.contains(physical_id)
                    && prepared.prepared.index < ack.index
                    && log_at_or_before(&prepared.prepared, ack)
                    && ack.index < completed.promoted.index
                    && log_at_or_before(ack, &completed.promoted)
                    && snapshot
                        .last_applied
                        .as_ref()
                        .is_some_and(|log| log_at_or_before(&completed.promoted, log))
                    && snapshot
                        .last_membership
                        .log_id()
                        .as_ref()
                        .is_some_and(|log| log_at_or_before(&completed.promoted, log))
                    && snapshot
                        .last_membership
                        .membership()
                        .voter_ids()
                        .any(|id| id == prepared.plan.consumer),
                "snapshot completion differs from its committed consumer or barriers"
            );
            if snapshot
                .last_membership
                .log_id()
                .as_ref()
                .is_some_and(|log| log.index == completed.promoted.index)
            {
                anyhow::ensure!(
                    snapshot.last_membership.log_id().as_ref() == Some(&completed.promoted)
                        && snapshot.last_membership.membership().get_joint_config()
                            == &vec![prepared.plan.next_voters.clone()],
                    "snapshot final membership differs from completed promotion"
                );
            }
        }
        let bootstrap = self.snapshot_bootstrap_operation(&snapshot)?;
        match bootstrap.filter(|_| {
            !self
                .memberships
                .contains(snapshot.last_membership.membership())
        }) {
            Some(proof) => self.snapshot_bootstrap_membership(&snapshot, proof)?,
            None => self.stored_membership(&snapshot.last_membership)?,
        }
        self.session.check()?;
        Ok(())
    }

    fn snapshot_bootstrap_operation(
        &self,
        snapshot: &SnapshotIdentities,
    ) -> anyhow::Result<Option<&PreparedJoin>> {
        let Some(proof) = &self.verified_bootstrap else {
            return Ok(None);
        };
        let proof = proof.prepared();
        self.join_plan_data(&proof.plan)?;
        anyhow::ensure!(
            proof.plan.consumer == self.session.context().local_replica,
            "snapshot bootstrap belongs to another consumer"
        );
        if snapshot
            .last_applied
            .is_none_or(|log| log.index < proof.prepared.index)
        {
            return Ok(None);
        }
        let pending = snapshot
            .prepared_join
            .as_ref()
            .is_some_and(|pending| same_operation(pending, proof));
        let completed = snapshot
            .completed_joins
            .get(&proof.plan.consumer.physical_id)
            .is_some_and(|completed| same_operation(&completed.prepared, proof));
        anyhow::ensure!(
            pending != completed,
            "snapshot must contain the exact verified pending or completed join"
        );
        Ok(Some(proof))
    }

    fn snapshot_bootstrap_membership(
        &self,
        snapshot: &SnapshotIdentities,
        proof: &PreparedJoin,
    ) -> anyhow::Result<()> {
        let plan = &proof.plan;
        let completed = snapshot
            .completed_joins
            .get(&plan.consumer.physical_id)
            .is_some_and(|receipt| same_operation(&receipt.prepared, proof));
        let configs = if completed {
            vec![plan.next_voters.clone()]
        } else {
            vec![plan.previous_voters.clone(), plan.next_voters.clone()]
        };
        // Derive addresses and retained learners from local authorization, never snapshot data.
        for previous in &self.memberships {
            if !Self::stable_previous(previous, plan) {
                continue;
            }
            let mut nodes: BTreeMap<_, _> = previous
                .nodes()
                .map(|(id, node)| (*id, node.clone()))
                .collect();
            if !nodes.contains_key(&plan.consumer) {
                continue;
            }
            if completed {
                for old in plan.previous_voters.difference(&plan.next_voters) {
                    nodes.remove(old);
                }
            }
            let expected = Membership::new(configs.clone(), nodes)?;
            if snapshot.last_membership.membership() == &expected {
                return Ok(());
            }
        }
        anyhow::bail!("snapshot membership is outside the verified bootstrap transition")
    }
}

fn same_operation(left: &PreparedJoin, right: &PreparedJoin) -> bool {
    left.plan == right.plan && left.prepared == right.prepared
}

fn log_at_or_before(earlier: &LogIdOf<TypeConfig>, later: &LogIdOf<TypeConfig>) -> bool {
    earlier.index <= later.index
        && earlier.leader_id.term() <= later.leader_id.term()
        && (earlier.index != later.index || earlier == later)
}

#[derive(Deserialize)]
struct SnapshotIdentities {
    genesis: Option<Genesis>,
    cluster_epoch: Option<u128>,
    last_applied: Option<LogIdOf<TypeConfig>>,
    last_membership: StoredMembershipOf<TypeConfig>,
    applied_progress: BTreeMap<u64, AppliedHealthProgress>,
    prepared_join: Option<PreparedJoin>,
    #[serde(default)]
    completed_joins: BTreeMap<u64, CompletedJoin>,
}
