//! Shared in-memory Raft state plus its serializable snapshot.
//!
//! Determinism
//! -----------
//! VIP ownership is recomputed from:
//! - the committed membership,
//! - committed `healthy` flags,
//! - a per-node committed probe counter incremented on every applied `HealthUpdate`,
//! - the largest committed probe round observed anywhere in the cluster,
//! - the configured VIP list.
//!
//! Every input is either fixed at startup (`vip_list`, `stale_missed_probes`) or replicated in the
//! Raft log, so every node converges to the same holder map after it has applied the same committed
//! prefix.
//!
//! Safe handoff
//! ------------
//! The state machine also records a per-VIP assignment generation plus the previous holder that
//! must release the VIP before the new holder may bind while the old node is still eligible. This
//! is what eliminates the old "two healthy lagging nodes can both bind" window.

use super::super::admission::{
    AdmissionDenied, AdmissionSession, AppliedHealthProgress, CompletedJoin, Genesis, PreparedJoin,
};
pub use super::super::types::FailoverSemantics;
use super::super::types::{KafSnapshotData, TypeConfig};
use crate::config::VipAddr;
use openraft::SnapshotMeta;
use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::net::IpAddr;
use std::sync::Arc;

/// One additional committed probe round must pass after a holder change before a replacement
/// owner may bind if it is taking over from an ineligible node without an explicit release ack.
///
/// This gives the previous holder a deterministic extra window to observe its own local-health or
/// consensus-freshness gate and remove the VIP before another node attaches it.
pub const OWNERSHIP_ACTIVATION_HOLDOFF_TICKS: u64 = 1;

/// Committed VIP assignment with enough fencing metadata to drive safe local bind decisions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VipAssignment {
    /// Current committed holder for this VIP.
    pub holder: u64,
    /// Monotonic generation bumped every time the committed holder changes.
    pub generation: u64,
    /// The holder from the immediately previous generation, if any.
    #[serde(default)]
    pub previous_holder: Option<u64>,
    /// Whether the previous holder has committed a matching [`super::super::types::KafRequest::VipReleased`] ack.
    #[serde(default)]
    pub previous_holder_released: bool,
    /// The cluster-wide committed probe round after which the replacement holder may activate if
    /// it is waiting on an ineligible previous holder rather than an explicit release ack.
    #[serde(default)]
    pub activation_tick: u64,
}

/// Serializable snapshot with maps and sets ordered by node id or VIP address.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KafSnapshot {
    #[serde(default)]
    pub prepared_join: Option<PreparedJoin>,
    #[serde(default)]
    pub completed_joins: BTreeMap<u64, CompletedJoin>,
    #[serde(default)]
    pub genesis: Option<Genesis>,
    #[serde(default)]
    pub applied_progress: BTreeMap<u64, AppliedHealthProgress>,
    pub last_applied: Option<LogIdOf<TypeConfig>>,
    pub last_membership: StoredMembershipOf<TypeConfig>,
    pub node_health: BTreeMap<u64, bool>,
    /// Per-node committed probe round. On every applied `HealthUpdate` it advances by one but never
    /// trails the cluster frontier (`latest_probe_tick`), so a node returning after downtime
    /// regains freshness in a single update; see [`super::vip_logic::next_probe_tick`].
    #[serde(default)]
    pub node_probe_ticks: BTreeMap<u64, u64>,
    /// Maximum committed probe round observed across all nodes.
    #[serde(default)]
    pub latest_probe_tick: u64,
    /// Committed fenced assignment state per VIP.
    #[serde(default)]
    pub vip_assignments: BTreeMap<IpAddr, VipAssignment>,
    /// Last generation number used per VIP, including removed assignments.
    #[serde(default)]
    pub vip_generation: BTreeMap<IpAddr, u64>,
    /// Last assigned holder, retained while a VIP has no eligible owner.
    #[serde(default)]
    pub vip_last_holder: BTreeMap<IpAddr, u64>,
    /// Per-formation cluster incarnation (see [`super::super::types::KafRequest::ClusterFormed`]).
    /// `None` until the first leader commits it; a snapshot built before that point carries `None`.
    #[serde(default)]
    pub cluster_epoch: Option<u128>,
    /// Probe round at which a node first reported healthy after a prior unhealthy period.
    /// Cleared when the node goes unhealthy again. Used to enforce `failback_delay_ticks`.
    /// Absent means the node was never unhealthy (no delay applies on its current healthy streak).
    #[serde(default)]
    pub node_recovery_tick: BTreeMap<u64, u64>,
    /// Legacy-semantics nodes permanently blocked by `failback: false`. Legacy does not record
    /// ownership evidence, so activation migrates every entry conservatively to `node_nopreempt`.
    #[serde(default)]
    pub node_failback_blocked: BTreeSet<u64>,
    /// Active behavior version. Missing from an old snapshot means legacy.
    #[serde(default)]
    pub failover_semantics: FailoverSemantics,
    /// Once true, peers without a concrete matching config identity are fenced.
    #[serde(default)]
    pub config_identity_enforced: bool,
    /// V2 nodes that lost an owned VIP and must not receive proactive rebalancing assignments.
    #[serde(default)]
    pub node_nopreempt: BTreeSet<u64>,
    /// V2 failed owners waiting to begin or complete their continuous-health recovery delay.
    #[serde(default)]
    pub node_recovery_pending: BTreeSet<u64>,
}

impl From<&KafStorageState> for KafSnapshot {
    fn from(state: &KafStorageState) -> Self {
        Self {
            prepared_join: state.prepared_join.clone(),
            completed_joins: state.completed_joins.clone(),
            genesis: state.genesis.clone(),
            applied_progress: state.applied_progress.clone(),
            last_applied: state.last_applied_log,
            last_membership: state.last_membership.clone(),
            node_health: state.node_health.clone(),
            node_probe_ticks: state.node_probe_ticks.clone(),
            latest_probe_tick: state.latest_probe_tick,
            vip_assignments: state.vip_assignments.clone(),
            vip_generation: state.vip_generation.clone(),
            vip_last_holder: state.vip_last_holder.clone(),
            cluster_epoch: state.cluster_epoch,
            node_recovery_tick: state.node_recovery_tick.clone(),
            node_failback_blocked: state.node_failback_blocked.clone(),
            failover_semantics: state.failover_semantics,
            config_identity_enforced: state.config_identity_enforced,
            node_nopreempt: state.node_nopreempt.clone(),
            node_recovery_pending: state.node_recovery_pending.clone(),
        }
    }
}

impl KafSnapshot {
    /// Restore replicated fields without changing the local config, vote, or log.
    pub(super) fn restore_into(&self, state: &mut KafStorageState) {
        state.prepared_join.clone_from(&self.prepared_join);
        state.completed_joins.clone_from(&self.completed_joins);
        state.genesis.clone_from(&self.genesis);
        state.applied_progress.clone_from(&self.applied_progress);
        state.last_applied_log = self.last_applied;
        state.last_membership = self.last_membership.clone();
        state.node_health.clone_from(&self.node_health);
        state.node_probe_ticks.clone_from(&self.node_probe_ticks);
        state.latest_probe_tick = self.latest_probe_tick;
        state.vip_assignments.clone_from(&self.vip_assignments);
        state.vip_generation.clone_from(&self.vip_generation);
        state.vip_last_holder.clone_from(&self.vip_last_holder);
        // Adopt the incarnation carried by the snapshot. A node catching up via InstallSnapshot
        // thereby takes on the sender's cluster identity (set-once already held when the snapshot
        // was built).
        state.cluster_epoch = self.cluster_epoch;
        // Restore failback tracking from snapshot.
        state
            .node_recovery_tick
            .clone_from(&self.node_recovery_tick);
        state.failover_semantics = self.failover_semantics;
        state.config_identity_enforced = self.config_identity_enforced;
        state.node_nopreempt.clone_from(&self.node_nopreempt);
        state
            .node_recovery_pending
            .clone_from(&self.node_recovery_pending);
        // `node_failback_blocked` is only ever populated under `failback: false`
        // (apply path). `failback` is config-derived and not replicated, and must be identical on
        // every member (see the KafStorageState doc). Reconcile against the local config rather than
        // copying verbatim, so a `failback: true` node never adopts a blocked set its own config
        // could not produce; a `failback: false` node keeps the set.
        match state.failover_semantics {
            FailoverSemantics::Legacy if state.failback => {
                state.node_failback_blocked.clear();
            }
            FailoverSemantics::Legacy => {
                state
                    .node_failback_blocked
                    .clone_from(&self.node_failback_blocked);
            }
            FailoverSemantics::V2 if state.failback => {
                state.node_failback_blocked.clear();
                state.node_nopreempt.clear();
            }
            FailoverSemantics::V2 => {
                state.node_failback_blocked.clear();
                state.node_recovery_pending.clear();
                state.node_recovery_tick.clear();
            }
        }
    }
}

/// Shared Raft state (log + replicated state-machine fields).
///
/// `vip_list`, `stale_missed_probes`, `failback`, `failback_delay_ticks` and activation holdoff
/// are constants of the local config and never enter the Raft log; they must be **identical** on
/// every member. Note: `failback` is especially critical - it controls replicated legacy blocks
/// or V2 nopreempt history, so a mismatch causes state-machine divergence.
///
/// The log-storage half ([`super::log::KafLogStore`]) and the state-machine half
/// ([`super::state_machine::KafStateMachine`]) both hold an `Arc<RwLock<KafStorageState>>` pointing
/// at one shared instance, so the openraft trait split is along method lines only - the data is not
/// duplicated.
pub struct KafStorageState {
    pub prepared_join: Option<PreparedJoin>,
    pub completed_joins: BTreeMap<u64, CompletedJoin>,
    pub genesis: Option<Genesis>,
    pub applied_progress: BTreeMap<u64, AppliedHealthProgress>,
    pub(crate) admission: Option<AdmissionSession>,
    pub(super) admission_required: bool,
    pub last_purged_log_id: Option<LogIdOf<TypeConfig>>,
    pub log: BTreeMap<u64, EntryOf<TypeConfig>>,
    pub vote: Option<VoteOf<TypeConfig>>,
    /// Highest committed log id, as reported by openraft via `save_committed`. Tracked so that
    /// `read_committed` can resume the engine's commit frontier after a restart instead of
    /// returning `None`. Without this, a restarted node boots with `committed = None`, and the
    /// first commit-driven apply reads `get_log_entries(0..)` against a backfilled log whose floor
    /// is non-zero - tripping `Defensive(LogIndexNotFound { want: 0 })`.
    pub committed: Option<LogIdOf<TypeConfig>>,
    pub last_applied_log: Option<LogIdOf<TypeConfig>>,
    pub last_membership: StoredMembershipOf<TypeConfig>,
    pub node_health: BTreeMap<u64, bool>,
    pub node_probe_ticks: BTreeMap<u64, u64>,
    pub latest_probe_tick: u64,
    pub vip_assignments: BTreeMap<IpAddr, VipAssignment>,
    pub vip_generation: BTreeMap<IpAddr, u64>,
    pub vip_last_holder: BTreeMap<IpAddr, u64>,
    /// Per-formation cluster incarnation; `None` until the first leader commits
    /// [`super::super::types::KafRequest::ClusterFormed`]. Read by the transport to fence
    /// foreign-incarnation peers and by `run_cluster_guard` to detect that this node is a stale
    /// survivor.
    pub cluster_epoch: Option<u128>,
    pub vip_list: Arc<Vec<(VipAddr, String)>>,
    pub stale_missed_probes: u64,
    /// Whether a recovered node may reclaim VIPs. Config-derived; not replicated.
    pub failback: bool,
    /// Minimum consecutive committed probe rounds a recovered node must accumulate before it is
    /// eligible for VIP assignment again. Config-derived from `failback_delay_secs / interval_ms`.
    /// Zero means immediate re-eligibility on recovery. Not replicated.
    pub failback_delay_ticks: u64,
    /// Probe round when a node first became healthy after an unhealthy period. Cleared on each
    /// `healthy: false` update. Replicated via snapshot so it survives log compaction.
    pub node_recovery_tick: BTreeMap<u64, u64>,
    /// Legacy-semantics permanent block set for `failback: false`. Replicated for compatibility and
    /// migrated to V2 `node_nopreempt` state at activation.
    pub node_failback_blocked: BTreeSet<u64>,
    pub failover_semantics: FailoverSemantics,
    pub config_identity_enforced: bool,
    pub node_nopreempt: BTreeSet<u64>,
    pub node_recovery_pending: BTreeSet<u64>,
    pub current_snapshot: Option<KafSnapshot>,
}

impl KafStorageState {
    /// Install process-local authority once; snapshots cannot restore or replace it.
    pub fn bind_admission(&mut self, admission: AdmissionSession) -> Result<(), AdmissionDenied> {
        admission.check()?;
        if self.admission.is_some() {
            return Err(AdmissionDenied("admission context is already bound"));
        }
        if self
            .genesis
            .as_ref()
            .is_some_and(|genesis| genesis != &admission.context().genesis)
        {
            return Err(AdmissionDenied(
                "admission genesis differs from applied history",
            ));
        }
        self.admission = Some(admission);
        Ok(())
    }

    /// Capture the current replicated state as a serialized [`openraft::Snapshot`] anchored at
    /// `last_applied`.
    ///
    /// Shared by `build_snapshot` and `get_current_snapshot` so the snapshot payload and metadata
    /// are produced identically in both paths. The `snapshot_id` is derived from the full
    /// `last_applied` log id (`<leader>-<index>` via its `Display`), not the index alone, so two
    /// snapshots taken at the same index in different terms get distinct ids - openraft uses the id
    /// for snapshot identity and de-dup.
    pub(super) fn snapshot_at(
        &self,
        last_applied: LogIdOf<TypeConfig>,
    ) -> std::io::Result<SnapshotOf<TypeConfig, KafSnapshotData>> {
        let mut snap = KafSnapshot::from(self);
        snap.last_applied = Some(last_applied);

        let data = serde_json::to_vec(&snap)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let meta: SnapshotMetaOf<TypeConfig> = SnapshotMeta {
            last_log_id: Some(last_applied),
            last_membership: self.last_membership.clone(),
            snapshot_id: format!("snapshot-{last_applied}"),
        };

        Ok(openraft::Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }

    /// Construct an empty state with the externally supplied `vip_list`, probe staleness window,
    /// and failback configuration.
    pub(super) fn new(
        vip_list: Arc<Vec<(VipAddr, String)>>,
        stale_missed_probes: u64,
        failback: bool,
        failback_delay_ticks: u64,
    ) -> Self {
        Self {
            prepared_join: None,
            completed_joins: BTreeMap::new(),
            genesis: None,
            applied_progress: BTreeMap::new(),
            admission: None,
            admission_required: true,
            last_purged_log_id: None,
            log: BTreeMap::new(),
            vote: None,
            committed: None,
            last_applied_log: None,
            last_membership: StoredMembershipOf::<TypeConfig>::default(),
            node_health: BTreeMap::new(),
            node_probe_ticks: BTreeMap::new(),
            latest_probe_tick: 0,
            vip_assignments: BTreeMap::new(),
            vip_generation: BTreeMap::new(),
            vip_last_holder: BTreeMap::new(),
            cluster_epoch: None,
            vip_list,
            stale_missed_probes,
            failback,
            failback_delay_ticks,
            node_recovery_tick: BTreeMap::new(),
            node_failback_blocked: BTreeSet::new(),
            failover_semantics: FailoverSemantics::Legacy,
            config_identity_enforced: false,
            node_nopreempt: BTreeSet::new(),
            node_recovery_pending: BTreeSet::new(),
            current_snapshot: None,
        }
    }
}
