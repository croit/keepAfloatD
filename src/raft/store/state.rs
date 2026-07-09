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

use super::super::types::TypeConfig;
use crate::config::VipAddr;
use openraft::SnapshotMeta;
use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
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

/// Serializable snapshot of the VIP daemon state machine.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KafSnapshot {
    pub last_applied: Option<LogIdOf<TypeConfig>>,
    pub last_membership: StoredMembershipOf<TypeConfig>,
    pub node_health: HashMap<u64, bool>,
    /// Per-node committed probe round. On every applied `HealthUpdate` it advances by one but never
    /// trails the cluster frontier (`latest_probe_tick`), so a node returning after downtime
    /// regains freshness in a single update; see [`super::vip_logic::next_probe_tick`].
    #[serde(default)]
    pub node_probe_ticks: HashMap<u64, u64>,
    /// Maximum committed probe round observed across all nodes.
    #[serde(default)]
    pub latest_probe_tick: u64,
    /// Committed fenced assignment state per VIP.
    #[serde(default)]
    pub vip_assignments: HashMap<IpAddr, VipAssignment>,
    /// Last generation number used per VIP, including removed assignments.
    #[serde(default)]
    pub vip_generation: HashMap<IpAddr, u64>,
    /// Per-formation cluster incarnation (see [`super::super::types::KafRequest::ClusterFormed`]).
    /// `None` until the first leader commits it; a snapshot built before that point carries `None`.
    #[serde(default)]
    pub cluster_epoch: Option<u128>,
    /// Probe round at which a node first reported healthy after a prior unhealthy period.
    /// Cleared when the node goes unhealthy again. Used to enforce `failback_delay_ticks`.
    /// Absent means the node was never unhealthy (no delay applies on its current healthy streak).
    #[serde(default)]
    pub node_recovery_tick: HashMap<u64, u64>,
    /// Nodes permanently blocked from eligibility until the cluster restarts (`failback: false`).
    /// Only populated when the config has `failback: false`; always empty when `failback: true`.
    #[serde(default)]
    pub node_failback_blocked: HashSet<u64>,
}

/// Shared Raft state (log + replicated state-machine fields).
///
/// `vip_list`, `stale_missed_probes`, `failback`, `failback_delay_ticks` and activation holdoff
/// are constants of the local config and never enter the Raft log; they must be **identical** on
/// every member. Note: `failback` is especially critical — it controls what gets written into
/// replicated state (`node_failback_blocked`), so a mismatch causes state-machine divergence.
///
/// The log-storage half ([`super::log::KafLogStore`]) and the state-machine half
/// ([`super::state_machine::KafStateMachine`]) both hold an `Arc<RwLock<KafStorageState>>` pointing
/// at one shared instance, so the openraft trait split is along method lines only — the data is not
/// duplicated.
pub struct KafStorageState {
    pub last_purged_log_id: Option<LogIdOf<TypeConfig>>,
    pub log: BTreeMap<u64, EntryOf<TypeConfig>>,
    pub vote: Option<VoteOf<TypeConfig>>,
    /// Highest committed log id, as reported by openraft via `save_committed`. Tracked so that
    /// `read_committed` can resume the engine's commit frontier after a restart instead of
    /// returning `None`. Without this, a restarted node boots with `committed = None`, and the
    /// first commit-driven apply reads `get_log_entries(0..)` against a backfilled log whose floor
    /// is non-zero — tripping `Defensive(LogIndexNotFound { want: 0 })`.
    pub committed: Option<LogIdOf<TypeConfig>>,
    pub last_applied_log: Option<LogIdOf<TypeConfig>>,
    pub last_membership: StoredMembershipOf<TypeConfig>,
    pub node_health: HashMap<u64, bool>,
    pub node_probe_ticks: HashMap<u64, u64>,
    pub latest_probe_tick: u64,
    pub vip_assignments: HashMap<IpAddr, VipAssignment>,
    pub vip_generation: HashMap<IpAddr, u64>,
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
    pub node_recovery_tick: HashMap<u64, u64>,
    /// Nodes permanently ineligible until the cluster restarts (`failback: false`). Only populated
    /// when `failback` is `false`. Replicated via snapshot.
    pub node_failback_blocked: HashSet<u64>,
    pub current_snapshot: Option<KafSnapshot>,
}

impl KafStorageState {
    /// Capture the current replicated state as a serialized [`openraft::Snapshot`] anchored at
    /// `last_applied`.
    ///
    /// Shared by `build_snapshot` and `get_current_snapshot` so the snapshot payload and metadata
    /// are produced identically in both paths. The `snapshot_id` is derived from the full
    /// `last_applied` log id (`<leader>-<index>` via its `Display`), not the index alone, so two
    /// snapshots taken at the same index in different terms get distinct ids — openraft uses the id
    /// for snapshot identity and de-dup.
    pub(super) fn snapshot_at(
        &self,
        last_applied: LogIdOf<TypeConfig>,
    ) -> std::io::Result<SnapshotOf<TypeConfig>> {
        let snap = KafSnapshot {
            last_applied: Some(last_applied),
            last_membership: self.last_membership.clone(),
            node_health: self.node_health.clone(),
            node_probe_ticks: self.node_probe_ticks.clone(),
            latest_probe_tick: self.latest_probe_tick,
            vip_assignments: self.vip_assignments.clone(),
            vip_generation: self.vip_generation.clone(),
            cluster_epoch: self.cluster_epoch,
            node_recovery_tick: self.node_recovery_tick.clone(),
            node_failback_blocked: self.node_failback_blocked.clone(),
        };

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
            last_purged_log_id: None,
            log: BTreeMap::new(),
            vote: None,
            committed: None,
            last_applied_log: None,
            last_membership: StoredMembershipOf::<TypeConfig>::default(),
            node_health: HashMap::new(),
            node_probe_ticks: HashMap::new(),
            latest_probe_tick: 0,
            vip_assignments: HashMap::new(),
            vip_generation: HashMap::new(),
            cluster_epoch: None,
            vip_list,
            stale_missed_probes,
            failback,
            failback_delay_ticks,
            node_recovery_tick: HashMap::new(),
            node_failback_blocked: HashSet::new(),
            current_snapshot: None,
        }
    }
}
