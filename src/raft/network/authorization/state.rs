//! Construct current permissions from local admission and committed state.

use super::*;

impl RaftAuthorization {
    /// Capture local authority; an unbound bootstrap payload cannot supply a voter roster.
    pub(crate) fn from_state(
        cfg: &crate::config::Config,
        state: &crate::raft::KafStorageState,
        peer: ReplicaId,
        bootstrap: Option<&crate::raft::admission::VerifiedJoinBootstrap>,
    ) -> Result<Self, AdmissionDenied> {
        let session = state
            .admission
            .as_ref()
            .ok_or(AdmissionDenied("no active admission session"))?;
        session.check()?;
        let context = session.context();
        let verified_bootstrap = bootstrap.cloned();
        let bootstrap = bootstrap.map(|proof| proof.prepared());
        let configured_physical_ids: BTreeSet<_> = cfg.peers.iter().map(|node| node.id).collect();
        context.genesis.validate_roster(&configured_physical_ids)?;
        if context.local_replica.physical_id != cfg.node_id
            || context.genesis.config
                != cfg
                    .cluster_config_fingerprint()
                    .map_err(|_| AdmissionDenied("invalid local configuration"))?
            || state
                .genesis
                .as_ref()
                .is_some_and(|genesis| genesis != &context.genesis)
            || state
                .cluster_epoch
                .is_some_and(|epoch| epoch != context.genesis.epoch)
            || bootstrap.is_some_and(|pending| {
                pending.plan.consumer != context.local_replica
                    || pending.plan.genesis != context.genesis
                    || pending.learner_applied.is_some()
            })
        {
            return Err(AdmissionDenied(
                "authorization context or bootstrap is not bound to applied state",
            ));
        }
        if let Some(pending) = bootstrap {
            pending.plan.validate()?;
        }
        let bootstrapping = bootstrap.filter(|pending| {
            state
                .last_applied_log
                .is_none_or(|log| log.index < pending.prepared.index)
        });
        if let Some(proof) = bootstrap.filter(|_| bootstrapping.is_none()) {
            let applied = state.prepared_join.as_ref().is_some_and(|pending| {
                pending.plan == proof.plan && pending.prepared == proof.prepared
            });
            let completed = state
                .completed_joins
                .get(&context.local_replica.physical_id)
                .is_some_and(|receipt| {
                    receipt.prepared.plan == proof.plan
                        && receipt.prepared.prepared == proof.prepared
                });
            if !applied && !completed {
                return Err(AdmissionDenied(
                    "applied history does not contain the verified join",
                ));
            }
        }
        let make_membership = |configs: Vec<BTreeSet<ReplicaId>>, ids: BTreeSet<ReplicaId>| {
            let nodes = ids
                .into_iter()
                .map(|id| {
                    cfg.get_peer(id.physical_id)
                        .map(|node| (id, BasicNode::new(node.raft_address.clone())))
                        .ok_or(AdmissionDenied(
                            "membership contains an unconfigured physical member",
                        ))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            Membership::new(configs, nodes)
                .map_err(|_| AdmissionDenied("invalid authorized membership"))
        };
        let mut committed_membership = state.last_membership.clone();
        if committed_membership
            .membership()
            .voter_ids()
            .next()
            .is_none()
        {
            if bootstrapping.is_none() && !context.genesis.voters.contains(&context.local_replica) {
                return Err(AdmissionDenied(
                    "bootstrap requires a verified exact join roster",
                ));
            }
            committed_membership = StoredMembershipOf::<TypeConfig>::new(
                None,
                make_membership(
                    vec![context.genesis.voters.clone()],
                    context.genesis.voters.clone(),
                )?,
            );
        }
        if committed_membership.log_id().as_ref().is_some_and(|log| {
            state
                .last_applied_log
                .as_ref()
                .is_none_or(|applied| log.index > applied.index)
        }) {
            return Err(AdmissionDenied("membership is not committed locally"));
        }
        let current = committed_membership.membership().clone();
        let ids: BTreeSet<_> = current.nodes().map(|(id, _)| *id).collect();
        if make_membership(current.get_joint_config().clone(), ids.clone())? != current {
            return Err(AdmissionDenied(
                "membership addresses differ from configured roster",
            ));
        }
        let mut result = Self {
            session: session.clone(),
            peer,
            configured_physical_ids,
            voters: current.voter_ids().collect(),
            log_replicas: context.genesis.voters.clone(),
            memberships: vec![current.clone()],
            committed_membership,
            last_applied: state.last_applied_log,
            prepared_join: state.prepared_join.clone(),
            verified_bootstrap,
            replication: Some(super::replay::ReplicationHistory::from_state(cfg, state)),
        };
        result.log_replicas.extend(ids.iter().copied());
        if let Some(proof) = bootstrapping {
            result
                .join_plan_data(&proof.plan)
                .map_err(|_| AdmissionDenied("invalid verified bootstrap roster"))?;
            result.voters = proof.plan.previous_voters.clone();
            let previous = proof.plan.previous_voters.clone();
            result
                .memberships
                .push(make_membership(vec![previous.clone()], previous.clone())?);
            let mut learners = previous.clone();
            learners.insert(proof.plan.consumer);
            result
                .memberships
                .push(make_membership(vec![previous], learners)?);
        }
        if let Some(pending) = &state.prepared_join {
            result
                .snapshot_prepared_join(
                    pending,
                    &result.committed_membership,
                    result.last_applied.as_ref(),
                )
                .map_err(|_| {
                    AdmissionDenied("pending join has no consistent committed barriers")
                })?;
            let plan = &pending.plan;
            result
                .log_replicas
                .extend(plan.previous_voters.union(&plan.next_voters).copied());
            let mut learner_ids = ids.clone();
            learner_ids.insert(plan.consumer);
            if Self::stable_previous(&current, plan) {
                result.memberships.push(make_membership(
                    vec![plan.previous_voters.clone()],
                    learner_ids.clone(),
                )?);
                let mut removed = ids.clone();
                removed.remove(&plan.consumer);
                result.memberships.push(make_membership(
                    vec![plan.previous_voters.clone()],
                    removed,
                )?);
                if pending.learner_applied.is_some()
                    && current.learner_ids().any(|id| id == plan.consumer)
                {
                    result.memberships.push(make_membership(
                        vec![plan.previous_voters.clone(), plan.next_voters.clone()],
                        learner_ids,
                    )?);
                }
            } else if pending.learner_applied.is_some() {
                let mut promoted = ids.clone();
                for old in plan.previous_voters.difference(&plan.next_voters) {
                    promoted.remove(old);
                }
                result
                    .memberships
                    .push(make_membership(vec![plan.next_voters.clone()], promoted)?);
            }
        } else if current.get_joint_config().len() != 1 {
            return Err(AdmissionDenied(
                "joint membership lacks a committed join operation",
            ));
        }
        for log in [
            state.last_purged_log_id,
            state.last_applied_log,
            state.committed,
            *state.last_membership.log_id(),
        ]
        .into_iter()
        .flatten()
        {
            result.log_replicas.insert(*log.leader_id.node_id());
        }
        let admitted_peer = match bootstrapping {
            Some(proof) => proof.plan.previous_voters.contains(&peer),
            None => ids.contains(&peer),
        };
        if !admitted_peer
            && result
                .prepared_join
                .as_ref()
                .is_none_or(|pending| pending.plan.consumer != peer)
        {
            return Err(AdmissionDenied(
                "peer is not an exact committed member or prepared learner",
            ));
        }
        Ok(result)
    }
}
