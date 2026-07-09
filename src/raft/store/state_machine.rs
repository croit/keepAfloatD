//! State-machine half of the keepafloatd Raft store (`RaftStateMachine` + `RaftSnapshotBuilder`).
//!
//! Applies committed entries (health updates, VIP release acks, cluster-formation, membership) to
//! the replicated VIP-ownership state and produces/installs snapshots. The per-entry logic and the
//! recompute/reconcile feedback loop are unchanged from openraft 0.9; only the trait shape moved
//! (apply now consumes a stream of `EntryResponder` and answers per entry via the responder).

use super::super::types::{KafRequest, KafResponse, TypeConfig};
use super::state::{KafSnapshot, KafStorageState};
use super::vip_logic::{next_probe_tick, recompute_vip_holder, reconcile_vip_assignments};
use futures::{Stream, TryStreamExt};
use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, StoredMembership};
use std::collections::HashMap;
use std::io::{self, Cursor};
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

/// State-machine handle over the shared in-memory Raft state.
///
/// Cloneable: `get_snapshot_builder` hands openraft another handle onto the same `Arc`.
#[derive(Clone)]
pub struct KafStateMachine {
    pub(super) state: Arc<RwLock<KafStorageState>>,
}

impl KafStateMachine {
    pub(super) fn new(state: Arc<RwLock<KafStorageState>>) -> Self {
        Self { state }
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
                KafRequest::HealthUpdate { node_id, healthy } => {
                    let was_unhealthy = state.node_health.get(node_id) == Some(&false);
                    state.node_health.insert(*node_id, *healthy);
                    let next_tick = next_probe_tick(
                        state.node_probe_ticks.get(node_id).copied(),
                        state.latest_probe_tick,
                    );
                    state.node_probe_ticks.insert(*node_id, next_tick);
                    if next_tick > state.latest_probe_tick {
                        state.latest_probe_tick = next_tick;
                    }
                    if *healthy {
                        // Transitioning from unhealthy → healthy: start the failback timer.
                        if was_unhealthy {
                            state.node_recovery_tick.insert(*node_id, next_tick);
                        }
                        // First-time healthy (was_unhealthy=false, no prior entry): no timer.
                    } else {
                        // Becoming unhealthy: reset any recovery timer.
                        state.node_recovery_tick.remove(node_id);
                        // If failback is disabled, permanently block this node.
                        if !state.failback {
                            state.node_failback_blocked.insert(*node_id);
                        }
                    }
                    should_recompute = true;
                    KafResponse::Ok
                }
                KafRequest::VipReleased {
                    node_id,
                    vip,
                    generation,
                } => {
                    if let Some(assignment) = state.vip_assignments.get_mut(vip) {
                        if assignment.generation == *generation
                            && assignment.previous_holder == Some(*node_id)
                        {
                            assignment.previous_holder_released = true;
                        }
                    }
                    KafResponse::Ok
                }
                KafRequest::ClusterFormed { cluster_id } => {
                    // Set-once: the first committed incarnation wins; later ones (e.g. a second
                    // leader that submitted before observing the first) are ignored, so every
                    // node converges deterministically on the same value.
                    state.cluster_epoch.get_or_insert(*cluster_id);
                    KafResponse::Ok
                }
            },
            EntryPayload::Membership(mem) => {
                state.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                should_recompute = true;
                KafResponse::Ok
            }
        };

        if should_recompute {
            let membership = state.last_membership.clone();
            let health = state.node_health.clone();
            let ticks = state.node_probe_ticks.clone();
            let latest = state.latest_probe_tick;
            let stale = state.stale_missed_probes;
            let failback_delay = state.failback_delay_ticks;
            let recovery_ticks = state.node_recovery_tick.clone();
            let failback_blocked = state.node_failback_blocked.clone();
            let vips = Arc::clone(&state.vip_list);
            // Prior committed holders feed the minimal-movement recompute. Read before the
            // `mem::take` below so it reflects the assignment state as of this entry; it is
            // deterministic because `vip_assignments` is itself replicated committed state.
            let current_holders: HashMap<IpAddr, u64> = state
                .vip_assignments
                .iter()
                .map(|(ip, a)| (*ip, a.holder))
                .collect();
            let mut vip_holder = HashMap::new();
            recompute_vip_holder(
                &membership,
                &health,
                &ticks,
                latest,
                stale,
                failback_delay,
                &recovery_ticks,
                &failback_blocked,
                vips.as_ref(),
                &current_holders,
                &mut vip_holder,
            );
            let mut assignments = std::mem::take(&mut state.vip_assignments);
            let mut generations = std::mem::take(&mut state.vip_generation);
            reconcile_vip_assignments(
                &vip_holder,
                latest,
                vips.as_ref(),
                &mut assignments,
                &mut generations,
            );
            state.vip_assignments = assignments;
            state.vip_generation = generations;
        }

        resp
    }
}

impl RaftSnapshotBuilder<TypeConfig> for KafStateMachine {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig>, io::Error> {
        let state = self.state.read().await;
        let last_applied = state
            .last_applied_log
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no applied logs"))?;
        state.snapshot_at(last_applied)
    }
}

impl RaftStateMachine<TypeConfig> for KafStateMachine {
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
        let mut state = self.state.write().await;
        while let Some((entry, responder)) = entries.try_next().await? {
            let resp = Self::apply_one(&mut state, &entry);
            if let Some(responder) = responder {
                responder.send(resp);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        let snap: KafSnapshot = serde_json::from_slice(&data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("install_snapshot {}: {e}", meta.snapshot_id),
            )
        })?;

        let mut state = self.state.write().await;
        state.last_applied_log = snap.last_applied;
        state.last_membership = snap.last_membership.clone();
        state.node_health = snap.node_health.clone();
        state.node_probe_ticks = snap.node_probe_ticks.clone();
        state.latest_probe_tick = snap.latest_probe_tick;
        state.vip_assignments = snap.vip_assignments.clone();
        state.vip_generation = snap.vip_generation.clone();
        // Adopt the incarnation carried by the snapshot. A node catching up via InstallSnapshot
        // thereby takes on the sender's cluster identity (set-once already held when the snapshot
        // was built).
        state.cluster_epoch = snap.cluster_epoch;
        // Restore failback tracking from snapshot.
        state.node_recovery_tick = snap.node_recovery_tick.clone();
        // `node_failback_blocked` is only ever populated under `failback: false`
        // (apply path). `failback` is config-derived and not replicated, and must be identical on
        // every member (see the KafStorageState doc). Reconcile against the local config rather than
        // copying verbatim, so a `failback: true` node never adopts a blocked set its own config
        // could not produce; a `failback: false` node keeps the set.
        if state.failback {
            state.node_failback_blocked.clear();
        } else {
            state.node_failback_blocked = snap.node_failback_blocked.clone();
        }

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

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<TypeConfig>>, io::Error> {
        let state = self.state.read().await;
        let Some(last_applied) = state.last_applied_log else {
            return Ok(None);
        };
        Ok(Some(state.snapshot_at(last_applied)?))
    }
}
