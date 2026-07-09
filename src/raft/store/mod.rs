//! In-memory Raft log and state machine: node health, committed probe rounds and fenced VIP handoff.
//!
//! Under openraft 0.10 the storage surface is two traits, not one. The log half
//! ([`log::KafLogStore`]: `RaftLogReader` + `RaftLogStorage`) and the state-machine half
//! ([`state_machine::KafStateMachine`]: `RaftStateMachine` + `RaftSnapshotBuilder`) both hold an
//! `Arc<RwLock<`[`state::KafStorageState`]`>>` pointing at one shared instance, so the split is along
//! method lines only — the in-memory data is not duplicated. [`new_store`] builds the pair plus a
//! third handle on the shared state for the transport/reconciliation layers.

mod log;
mod state;
mod state_machine;
mod vip_logic;

pub use log::KafLogStore;
pub use state::{KafStorageState, VipAssignment};
pub use state_machine::KafStateMachine;
pub use vip_logic::is_node_eligible;
// `recompute_vip_holder`/`reconcile_vip_assignments` are part of the replicated-path public surface
// but, outside the state machine itself, are only re-derived in `bind_policy`'s tests; gate the
// re-export to test builds so a non-test build does not warn on the unused public alias.
#[cfg(test)]
pub use vip_logic::{recompute_vip_holder, reconcile_vip_assignments};

use crate::config::VipAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Build the log-storage half, the state-machine half, and a shared-state handle from one volatile
/// in-memory [`KafStorageState`]. The two storage halves go to `Raft::new`; the `state_ref` handle
/// is read by the transport (epoch fencing) and the VIP reconciliation loop.
pub fn new_store(
    vip_list: Arc<Vec<(VipAddr, String)>>,
    stale_missed_probes: u64,
    failback: bool,
    failback_delay_ticks: u64,
) -> (KafLogStore, KafStateMachine, Arc<RwLock<KafStorageState>>) {
    let state = Arc::new(RwLock::new(KafStorageState::new(
        vip_list,
        stale_missed_probes,
        failback,
        failback_delay_ticks,
    )));
    let log_store = KafLogStore::new(state.clone());
    let state_machine = KafStateMachine::new(state.clone());
    (log_store, state_machine, state)
}

/// End-to-end exercises of the apply path and the log/snapshot trait surface: the integrated
/// frontier-advance + recompute + reconcile path that the pure-function tests in [`vip_logic`] cover
/// only in isolation. Assertions are invariant-based (every holder is eligible; release acks are
/// fenced by generation + previous holder) so they remain valid under the sticky/min-move placement.
#[cfg(test)]
mod apply_tests;
