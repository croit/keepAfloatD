//! Replication validation outcomes, separate from current voting permission.

use super::*;
use openraft::alias::EntryOf;
use openraft::raft::AppendEntriesRequest;

/// An authorized sender must backtrack because its replication prefix is unavailable.
#[derive(Debug)]
pub struct ReplicationConflict;

impl std::fmt::Display for ReplicationConflict {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("replication prefix is unavailable")
    }
}

impl std::error::Error for ReplicationConflict {}

/// A local log prefix used only to validate replication data, never to authorize a sender.
pub(crate) struct ReplicationHistory {
    nodes: BTreeMap<u64, BasicNode>,
    entries: BTreeMap<u64, EntryOf<TypeConfig>>,
    committed: Option<LogIdOf<TypeConfig>>,
}

struct SourceState {
    membership: StoredMembershipOf<TypeConfig>,
    pending: Option<PreparedJoin>,
    cursor: Option<LogIdOf<TypeConfig>>,
}

impl ReplicationHistory {
    pub(super) fn from_state(
        cfg: &crate::config::Config,
        state: &crate::raft::KafStorageState,
    ) -> Self {
        Self {
            nodes: cfg
                .peers
                .iter()
                .map(|peer| (peer.id, BasicNode::new(peer.raft_address.clone())))
                .collect(),
            entries: state
                .log
                .range((
                    state
                        .last_applied_log
                        .map_or(std::ops::Bound::Unbounded, |log| {
                            std::ops::Bound::Excluded(log.index)
                        }),
                    std::ops::Bound::Unbounded,
                ))
                .map(|(index, entry)| (*index, entry.clone()))
                .collect(),
            committed: state.committed,
        }
    }

    pub(super) fn validate(
        &self,
        auth: &RaftAuthorization,
        request: &AppendEntriesRequest<TypeConfig>,
    ) -> anyhow::Result<()> {
        let mut next = request
            .prev_log_id
            .as_ref()
            .map_or(Some(0), |log| log.index.checked_add(1));
        for entry in &request.entries {
            anyhow::ensure!(
                Some(entry.log_id.index) == next,
                "replicated entries are not consecutive"
            );
            next = entry.log_id.index.checked_add(1);
            self.data(auth, entry)?;
        }
        let mut source = if auth.last_applied.is_some() {
            SourceState {
                membership: auth.committed_membership.clone(),
                pending: auth.prepared_join.clone(),
                cursor: auth.last_applied,
            }
        } else {
            let voters = auth.session.context().genesis.voters.clone();
            let membership = self.membership(vec![voters.clone()], voters)?;
            SourceState {
                membership: StoredMembershipOf::<TypeConfig>::new(None, membership),
                pending: None,
                cursor: None,
            }
        };
        let committed = [self.committed, auth.last_applied, request.leader_commit]
            .into_iter()
            .flatten()
            .max_by_key(|log| log.index);
        if let Some(previous) = request.prev_log_id {
            if source.cursor.is_none_or(|log| previous.index > log.index) {
                let start = source.cursor.map_or(0, |log| log.index + 1);
                for index in start..=previous.index {
                    let entry = self.entries.get(&index).ok_or(ReplicationConflict)?;
                    self.data(auth, entry)?;
                    self.apply(auth, &mut source, entry, committed.as_ref())?;
                }
                anyhow::ensure!(source.cursor == Some(previous), ReplicationConflict);
            } else if source.cursor.is_some_and(|log| previous.index == log.index) {
                anyhow::ensure!(source.cursor == Some(previous), ReplicationConflict);
            }
        }
        for entry in &request.entries {
            if auth
                .last_applied
                .is_some_and(|applied| entry.log_id.index <= applied.index)
            {
                if auth
                    .last_applied
                    .is_some_and(|applied| entry.log_id.index == applied.index)
                {
                    anyhow::ensure!(auth.last_applied == Some(entry.log_id), ReplicationConflict);
                }
                continue;
            }
            anyhow::ensure!(
                entry.log_id.index == source.cursor.map_or(0, |log| log.index + 1),
                ReplicationConflict
            );
            self.apply(auth, &mut source, entry, committed.as_ref())?;
        }
        Ok(())
    }

    fn data(&self, auth: &RaftAuthorization, entry: &EntryOf<TypeConfig>) -> anyhow::Result<()> {
        auth.log(Some(&entry.log_id))?;
        match &entry.payload {
            EntryPayload::Membership(membership) => {
                let configs = membership.get_joint_config();
                anyhow::ensure!(
                    !configs.is_empty() && configs.len() <= 2,
                    "invalid historical membership shape"
                );
                for voters in configs {
                    let physical: BTreeSet<_> = voters.iter().map(|id| id.physical_id).collect();
                    anyhow::ensure!(
                        !voters.is_empty() && physical.len() == voters.len(),
                        "invalid historical voter identities"
                    );
                }
                let expected = self.membership(
                    configs.clone(),
                    membership.nodes().map(|(id, _)| *id).collect(),
                )?;
                anyhow::ensure!(
                    expected == *membership,
                    "historical membership differs from configured addresses"
                );
            }
            EntryPayload::Normal(KafRequest::AdmissionMembership(command)) => match command {
                AdmissionCommand::PrepareJoin(plan) => auth.join_plan_data(plan)?,
                AdmissionCommand::CancelJoin { plan, prepared } => {
                    auth.join_plan_data(plan)?;
                    auth.log(Some(prepared))?;
                    anyhow::ensure!(
                        prepared.index < entry.log_id.index,
                        "cancellation precedes preparation"
                    );
                }
                AdmissionCommand::LearnerApplied {
                    consumer, prepared, ..
                } => {
                    anyhow::ensure!(
                        self.nodes.contains_key(&consumer.physical_id),
                        "learner is not configured"
                    );
                    auth.log(Some(prepared))?;
                    anyhow::ensure!(
                        prepared.index < entry.log_id.index,
                        "acknowledgement precedes preparation"
                    );
                }
            },
            EntryPayload::Normal(command) => auth.command(command, &entry.log_id)?,
            EntryPayload::Blank => {}
        }
        Ok(())
    }

    fn membership(
        &self,
        configs: Vec<BTreeSet<ReplicaId>>,
        ids: BTreeSet<ReplicaId>,
    ) -> anyhow::Result<Membership<ReplicaId, BasicNode>> {
        let nodes = ids
            .into_iter()
            .map(|id| {
                self.nodes
                    .get(&id.physical_id)
                    .cloned()
                    .map(|node| (id, node))
                    .ok_or_else(|| anyhow::anyhow!("historical member is not configured"))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        Ok(Membership::new(configs, nodes)?)
    }

    fn committed(log: &LogIdOf<TypeConfig>, committed: Option<&LogIdOf<TypeConfig>>) -> bool {
        committed.is_some_and(|frontier| log.index < frontier.index || log == frontier)
    }

    fn apply(
        &self,
        auth: &RaftAuthorization,
        source: &mut SourceState,
        entry: &EntryOf<TypeConfig>,
        committed: Option<&LogIdOf<TypeConfig>>,
    ) -> anyhow::Result<()> {
        match &entry.payload {
            EntryPayload::Membership(membership) => {
                self.transition(source, membership, entry.log_id, committed)?
            }
            EntryPayload::Normal(KafRequest::AdmissionMembership(command)) => match command {
                AdmissionCommand::PrepareJoin(plan) => {
                    if let Some(pending) = &source.pending {
                        anyhow::ensure!(pending.plan == *plan, "source has another pending join");
                    } else {
                        anyhow::ensure!(
                            RaftAuthorization::stable_previous(
                                source.membership.membership(),
                                plan
                            ),
                            "source preparation differs from stable previous voters"
                        );
                        source.pending = Some(PreparedJoin {
                            plan: plan.clone(),
                            prepared: entry.log_id,
                            learner_applied: None,
                        });
                    }
                }
                AdmissionCommand::CancelJoin { plan, prepared } => {
                    let pending = source.pending.as_ref().ok_or_else(|| {
                        anyhow::anyhow!("source cancellation has no pending join")
                    })?;
                    anyhow::ensure!(
                        pending.plan == *plan
                            && pending.prepared == *prepared
                            && Self::committed(prepared, committed)
                            && RaftAuthorization::stable_previous(
                                source.membership.membership(),
                                plan
                            )
                            && !source
                                .membership
                                .membership()
                                .nodes()
                                .any(|(id, _)| *id == plan.consumer),
                        "source cancellation differs from removed committed learner"
                    );
                    source.pending = None;
                }
                AdmissionCommand::LearnerApplied {
                    consumer,
                    request_nonce,
                    prepared,
                } => {
                    let pending = source.pending.as_mut().ok_or_else(|| {
                        anyhow::anyhow!("source acknowledgement has no pending join")
                    })?;
                    anyhow::ensure!(
                        pending.plan.consumer == *consumer
                            && pending.plan.request_nonce == *request_nonce
                            && pending.prepared == *prepared
                            && Self::committed(prepared, committed),
                        "source acknowledgement differs from committed preparation"
                    );
                    if pending.learner_applied.is_none() {
                        anyhow::ensure!(
                            RaftAuthorization::stable_previous(
                                source.membership.membership(),
                                &pending.plan
                            ) && source
                                .membership
                                .membership()
                                .learner_ids()
                                .any(|id| id == *consumer)
                                && source
                                    .membership
                                    .log_id()
                                    .as_ref()
                                    .is_some_and(|log| Self::committed(log, committed)),
                            "source acknowledgement requires a committed learner"
                        );
                        pending.learner_applied = Some(entry.log_id);
                    }
                }
            },
            EntryPayload::Normal(command) => auth.command(command, &entry.log_id)?,
            EntryPayload::Blank => {}
        }
        source.cursor = Some(entry.log_id);
        Ok(())
    }

    fn transition(
        &self,
        source: &mut SourceState,
        membership: &Membership<ReplicaId, BasicNode>,
        log: LogIdOf<TypeConfig>,
        committed: Option<&LogIdOf<TypeConfig>>,
    ) -> anyhow::Result<()> {
        if membership == source.membership.membership() {
            source.membership =
                StoredMembershipOf::<TypeConfig>::new(Some(log), membership.clone());
            return Ok(());
        }
        let pending = source
            .pending
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("source membership has no prepared transition"))?;
        anyhow::ensure!(
            Self::committed(&pending.prepared, committed),
            "source preparation is not committed"
        );
        let plan = &pending.plan;
        let current = source.membership.membership();
        let ids: BTreeSet<_> = current.nodes().map(|(id, _)| *id).collect();
        let mut permitted = Vec::new();
        let mut final_stage = false;
        if RaftAuthorization::stable_previous(current, plan) {
            let mut learners = ids.clone();
            learners.insert(plan.consumer);
            permitted.push(self.membership(vec![plan.previous_voters.clone()], learners.clone())?);
            let mut removed = ids;
            removed.remove(&plan.consumer);
            permitted.push(self.membership(vec![plan.previous_voters.clone()], removed)?);
            if pending
                .learner_applied
                .as_ref()
                .is_some_and(|ack| Self::committed(ack, committed))
                && current.learner_ids().any(|id| id == plan.consumer)
            {
                permitted.push(self.membership(
                    vec![plan.previous_voters.clone(), plan.next_voters.clone()],
                    learners,
                )?);
            }
        } else {
            anyhow::ensure!(
                current.get_joint_config()
                    == &vec![plan.previous_voters.clone(), plan.next_voters.clone()]
                    && pending
                        .learner_applied
                        .as_ref()
                        .is_some_and(|ack| Self::committed(ack, committed))
                    && source
                        .membership
                        .log_id()
                        .as_ref()
                        .is_some_and(|log| Self::committed(log, committed)),
                "source final promotion requires committed joint membership"
            );
            let mut promoted = ids;
            for old in plan.previous_voters.difference(&plan.next_voters) {
                promoted.remove(old);
            }
            permitted.push(self.membership(vec![plan.next_voters.clone()], promoted)?);
            final_stage = true;
        }
        anyhow::ensure!(
            permitted.contains(membership),
            "source membership skips a committed transition barrier"
        );
        source.membership = StoredMembershipOf::<TypeConfig>::new(Some(log), membership.clone());
        if final_stage {
            source.pending = None;
        }
        Ok(())
    }
}
