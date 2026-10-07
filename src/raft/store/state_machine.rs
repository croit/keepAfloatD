//! State-machine half of the keepafloatd Raft store (`RaftStateMachine` + `RaftSnapshotBuilder`).
//!
//! Applies committed entries (health updates, VIP release acks, cluster-formation, membership) to
//! the replicated VIP-ownership state and produces/installs snapshots. The per-entry logic and the
//! recompute/reconcile feedback loop are unchanged from openraft 0.9; only the trait shape moved
//! (apply now consumes a stream of `EntryResponder` and answers per entry via the responder).

use super::super::types::{KafRequest, KafResponse, KafSnapshotData, TypeConfig};
use super::authority::check_mutation;
use super::state::{FailoverSemantics, KafSnapshot, KafStorageState};
use super::vip_logic::{
    EligibilityInputs, is_node_base_eligible, next_probe_tick, recompute_vip_holder,
    recompute_vip_holder_v2, reconcile_vip_assignments,
};
use futures::{Stream, TryStreamExt};
use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(test)]
#[path = "authority_tests.rs"]
mod authority_tests;

/// State-machine handle over the shared in-memory Raft state.
///
/// Cloneable: `get_snapshot_builder` hands openraft another handle onto the same `Arc`.
#[derive(Clone)]
pub struct KafStateMachine {
    pub(super) state: Arc<RwLock<KafStorageState>>,
}

impl KafStateMachine {
    pub(crate) fn shared_state(&self) -> Arc<RwLock<KafStorageState>> {
        self.state.clone()
    }
    pub(super) fn new(state: Arc<RwLock<KafStorageState>>) -> Self {
        Self { state }
    }

    fn activate_v2(state: &mut KafStorageState) {
        if state.failover_semantics == FailoverSemantics::V2 {
            return;
        }
        state.failover_semantics = FailoverSemantics::V2;
        if state.failback {
            state
                .node_recovery_pending
                .extend(state.node_recovery_tick.keys().copied());
        } else {
            state
                .node_nopreempt
                .extend(state.node_failback_blocked.iter().copied());
            state.node_recovery_tick.clear();
        }
        state.node_failback_blocked.clear();
    }

    /// Update V2 recovery history from actual failed VIP owners, including silently stale ones.
    fn update_v2_failback_tracking(state: &mut KafStorageState) {
        let members = super::membership::unique_voters(state.last_membership.membership());
        let failed_owners: BTreeSet<u64> = state
            .vip_assignments
            .values()
            .map(|assignment| assignment.holder)
            .filter(|node_id| {
                !members.contains_key(node_id)
                    || !is_node_base_eligible(
                        *node_id,
                        &state.node_health,
                        &state.node_probe_ticks,
                        state.latest_probe_tick,
                        state.stale_missed_probes,
                    )
            })
            .collect();

        if !state.failback {
            state.node_nopreempt.extend(failed_owners);
            state.node_recovery_pending.clear();
            state.node_recovery_tick.clear();
            return;
        }

        state.node_nopreempt.clear();
        for node_id in failed_owners {
            state.node_recovery_pending.insert(node_id);
            state.node_recovery_tick.remove(&node_id);
        }

        let pending: Vec<u64> = state.node_recovery_pending.iter().copied().collect();
        let mut completed = Vec::new();
        for node_id in pending {
            if !members.contains_key(&node_id)
                || !is_node_base_eligible(
                    node_id,
                    &state.node_health,
                    &state.node_probe_ticks,
                    state.latest_probe_tick,
                    state.stale_missed_probes,
                )
            {
                state.node_recovery_tick.remove(&node_id);
                continue;
            }
            let recovery_tick = *state
                .node_recovery_tick
                .entry(node_id)
                .or_insert(state.latest_probe_tick);
            if state.latest_probe_tick.saturating_sub(recovery_tick) >= state.failback_delay_ticks {
                completed.push(node_id);
            }
        }
        for node_id in completed {
            state.node_recovery_pending.remove(&node_id);
            state.node_recovery_tick.remove(&node_id);
        }
    }

    /// Apply one already-committed entry to the in-memory state machine, returning the per-entry
    /// response. Shared by the `apply` stream loop and the test-only `apply_entries` shim so both
    /// paths run identical logic.
    ///
    /// `entry` is taken by reference because the recompute/reconcile feedback reads `state`, not the
    /// entry, after dispatch.
    fn apply_one(state: &mut KafStorageState, entry: &EntryOf<TypeConfig>) -> KafResponse {
        state.last_applied_log = Some(entry.log_id);
        let mut should_recompute = false;
        let resp = match &entry.payload {
            EntryPayload::Blank => KafResponse::Ok,
            EntryPayload::Normal(req) => match req {
                KafRequest::AdmissionMembership(command) => {
                    state.apply_admission_membership(command, entry.log_id)
                }
                KafRequest::AdmissionGenesis(genesis) => {
                    if state
                        .genesis
                        .as_ref()
                        .is_some_and(|current| current != genesis)
                        || state
                            .cluster_epoch
                            .is_some_and(|epoch| epoch != genesis.epoch)
                    {
                        KafResponse::Rejected("immutable genesis mismatch".into())
                    } else {
                        if state.genesis.is_none() {
                            state.node_health.clear();
                            state.node_probe_ticks.clear();
                            state.node_recovery_tick.clear();
                            state.applied_progress.clear();
                            Self::activate_v2(state);
                            state.config_identity_enforced = true;
                            should_recompute = true;
                        }
                        state.genesis = Some(genesis.clone());
                        state.cluster_epoch = Some(genesis.epoch);
                        KafResponse::Ok
                    }
                }
                KafRequest::HealthProgress(progress) => {
                    if progress.validate().is_err()
                        || state.genesis.as_ref() != Some(&progress.genesis)
                        || !state
                            .last_membership
                            .membership()
                            .nodes()
                            .any(|(replica, _)| *replica == progress.replica)
                    {
                        return KafResponse::Rejected(
                            "progress context differs from committed genesis".into(),
                        );
                    }
                    let voters =
                        super::membership::unique_voters(state.last_membership.membership());
                    state.applied_progress.insert(
                        progress.node_id,
                        super::super::admission::AppliedHealthProgress {
                            request: progress.clone(),
                            log_id: entry.log_id,
                        },
                    );
                    if let Some(healthy) = progress.healthy {
                        let eligible = voters.get(&progress.node_id) == Some(&progress.replica);
                        Self::apply_health(state, progress.node_id, healthy && eligible);
                        should_recompute = true;
                    }
                    KafResponse::Ok
                }
                KafRequest::HealthUpdate { node_id, healthy } => {
                    if state.genesis.is_some() {
                        return KafResponse::Rejected(
                            "physical-only health has no boot authority".into(),
                        );
                    }
                    Self::apply_health(state, *node_id, *healthy);
                    should_recompute = true;
                    KafResponse::Ok
                }
                KafRequest::VipReleased {
                    node_id,
                    vip,
                    generation,
                } => {
                    if let Some(assignment) = state.vip_assignments.get_mut(vip)
                        && assignment.generation == *generation
                        && assignment.previous_holder == Some(*node_id)
                    {
                        assignment.previous_holder_released = true;
                    }
                    KafResponse::Ok
                }
                KafRequest::ClusterFormed {
                    cluster_id,
                    failover_semantics,
                    config_identity_enforced,
                } => {
                    // Set-once: the first committed incarnation wins; later ones (e.g. a second
                    // leader that submitted before observing the first) are ignored, so every
                    // node converges deterministically on the same value.
                    if state.cluster_epoch.is_none() {
                        state.cluster_epoch = Some(*cluster_id);
                        state.config_identity_enforced |= *config_identity_enforced;
                        if *failover_semantics == FailoverSemantics::V2 {
                            Self::activate_v2(state);
                            should_recompute = true;
                        }
                    }
                    KafResponse::Ok
                }
                KafRequest::EnableFailoverSemanticsV2 => {
                    Self::activate_v2(state);
                    should_recompute = true;
                    KafResponse::Ok
                }
                KafRequest::EnableConfigIdentityV1 => {
                    state.config_identity_enforced = true;
                    KafResponse::Ok
                }
            },
            EntryPayload::Membership(mem) => {
                state.apply_membership(mem.clone(), entry.log_id);
                should_recompute = true;
                KafResponse::Ok
            }
        };

        if should_recompute {
            if state.failover_semantics == FailoverSemantics::V2 {
                Self::update_v2_failback_tracking(state);
            }
            let latest = state.latest_probe_tick;
            let eligibility = EligibilityInputs::from(&*state);
            // Prior committed holders keep recomputation stable across topology changes.
            let current_holders: BTreeMap<IpAddr, u64> = state
                .vip_assignments
                .iter()
                .map(|(ip, a)| (*ip, a.holder))
                .collect();
            let mut vip_holder = BTreeMap::new();
            match state.failover_semantics {
                FailoverSemantics::Legacy => recompute_vip_holder(
                    &state.last_membership,
                    &eligibility,
                    &state.node_failback_blocked,
                    &state.vip_list,
                    &current_holders,
                    &mut vip_holder,
                ),
                FailoverSemantics::V2 => recompute_vip_holder_v2(
                    &state.last_membership,
                    &eligibility,
                    &state.node_nopreempt,
                    &state.vip_list,
                    &current_holders,
                    &mut vip_holder,
                ),
            }
            reconcile_vip_assignments(
                &vip_holder,
                latest,
                &state.vip_list,
                &mut state.vip_assignments,
                &mut state.vip_generation,
                &mut state.vip_last_holder,
            );
        }

        resp
    }

    fn apply_health(state: &mut KafStorageState, node_id: u64, healthy: bool) {
        let previous_health = state.node_health.insert(node_id, healthy);
        let next_tick = next_probe_tick(
            state.node_probe_ticks.get(&node_id).copied(),
            state.latest_probe_tick,
        );
        state.node_probe_ticks.insert(node_id, next_tick);
        // `next_probe_tick` never trails the frontier.
        state.latest_probe_tick = next_tick;
        if state.failover_semantics == FailoverSemantics::Legacy && healthy {
            if state.failback && previous_health == Some(false) {
                state.node_recovery_tick.insert(node_id, next_tick);
            }
        } else if state.failover_semantics == FailoverSemantics::Legacy {
            state.node_recovery_tick.remove(&node_id);
            if !state.failback && previous_health == Some(true) {
                state.node_failback_blocked.insert(node_id);
            }
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for KafStateMachine {
    type SnapshotData = KafSnapshotData;

    async fn build_snapshot(
        &mut self,
    ) -> Result<SnapshotOf<TypeConfig, Self::SnapshotData>, io::Error> {
        let state = self.state.read().await;
        let last_applied = state
            .last_applied_log
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no applied logs"))?;
        state.snapshot_at(last_applied)
    }
}

impl RaftStateMachine<TypeConfig> for KafStateMachine {
    type SnapshotData = KafSnapshotData;
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        let state = self.state.read().await;
        Ok((state.last_applied_log, state.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        while let Some((entry, responder)) = entries.try_next().await? {
            let resp = {
                let mut state = self.state.write().await;
                check_mutation(&state)?;
                Self::apply_one(&mut state, &entry)
            };
            if let Some(responder) = responder {
                responder.send(resp);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        let snap: KafSnapshot = serde_json::from_slice(&data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("install_snapshot {}: {e}", meta.snapshot_id),
            )
        })?;

        let mut state = self.state.write().await;
        if state
            .genesis
            .as_ref()
            .is_some_and(|genesis| snap.genesis.as_ref() != Some(genesis))
            || state
                .admission
                .as_ref()
                .is_some_and(|session| snap.genesis.as_ref() != Some(&session.context().genesis))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot admission genesis mismatch",
            ));
        }
        check_mutation(&state)?;
        snap.restore_into(&mut state);

        // Keep the local log view consistent with the installed snapshot. After a snapshot covers
        // indices up to N the core advances last-applied to N and reads ranges starting at N+1; any
        // entry <= N must therefore be purged and `last_purged_log_id` set, or the next read returns
        // `Defensive(LogIndexNotFound)` and RaftCore quits. Upholds the storage invariant
        // `last_purged_log_id <= last_applied <= last_log_id`.
        if let Some(snap_last) = snap.last_applied {
            let stale: Vec<u64> = state
                .log
                .range(..=snap_last.index())
                .map(|(k, _)| *k)
                .collect();
            for idx in stale {
                state.log.remove(&idx);
            }
            // `last_purged_log_id` is monotonic; never move it backwards.
            if state.last_purged_log_id.map(|l| l.index()) < Some(snap_last.index()) {
                state.last_purged_log_id = Some(snap_last);
            }
        }

        state.current_snapshot = Some(snap);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, Self::SnapshotData>>, io::Error> {
        let state = self.state.read().await;
        let Some(last_applied) = state.last_applied_log else {
            return Ok(None);
        };
        Ok(Some(state.snapshot_at(last_applied)?))
    }
}
