//! Consensus-to-kernel VIP reconciliation.

#[cfg(test)]
use super::VipState;
use super::{
    LocalVip, release_notify_state, should_reannounce_after_release, startup_effects_may_arm,
};
use crate::config::{Config, VipAddr};
use crate::consensus_freshness::{ConsensusFreshness, FailureKind, failure_kind};
use crate::health::LocalHealth;
use crate::raft::store::VipAssignment;
use crate::raft::{KafRaft, KafRequest, KafStorageState};
use crate::submit;
use openraft::async_runtime::WatchReceiver;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
pub(super) mod intent;
mod worker;
use intent::EffectIntent;

/// Reconciliation tick period.
pub(crate) const RECONCILE_TICK: tokio::time::Duration = tokio::time::Duration::from_millis(250);

struct ReconcileMemory {
    released_generations: HashMap<IpAddr, u64>,
    activated_generations: HashMap<IpAddr, u64>,
    announced_release_generations: HashMap<IpAddr, u64>,
    vip_effects_armed: bool,
    takeover_delay: super::takeover::TakeoverDelay,
}

impl ReconcileMemory {
    fn new(proof_lifetime: tokio::time::Duration, vip_count: usize) -> Self {
        Self {
            released_generations: HashMap::new(),
            activated_generations: HashMap::new(),
            announced_release_generations: HashMap::new(),
            vip_effects_armed: false,
            takeover_delay: super::takeover::TakeoverDelay::new(takeover_lifetime(
                proof_lifetime,
                vip_count,
            )),
        }
    }
}

fn takeover_lifetime(
    proof_lifetime: tokio::time::Duration,
    vip_count: usize,
) -> tokio::time::Duration {
    // #26: reserve a reconcile tick plus bounded address/marker deletion per VIP.
    let cleanup_work = u32::try_from(vip_count).map_or(tokio::time::Duration::MAX, |count| {
        super::SHUTDOWN_VIP_BUDGET.saturating_mul(count)
    });
    proof_lifetime.saturating_add(RECONCILE_TICK.saturating_add(cleanup_work))
}

/// Reconcile each VIP independently so one cleanup cannot delay another deadline.
#[allow(clippy::too_many_arguments)]
pub async fn run_reconcile_loop(
    cfg: Arc<Config>,
    raft: KafRaft,
    sm: Arc<RwLock<KafStorageState>>,
    vip_local: Arc<LocalVip>,
    vip_table: Arc<Vec<(VipAddr, String)>>,
    local_healthy: Arc<LocalHealth>,
    consensus_fresh: Arc<ConsensusFreshness>,
    node_id: u64,
) {
    use futures::stream::{FuturesUnordered, StreamExt};
    let mut workers = FuturesUnordered::new();
    for (vip, iface) in vip_table.iter().cloned() {
        workers.push(worker::run(
            cfg.clone(),
            raft.clone(),
            sm.clone(),
            vip_local.clone(),
            vip,
            iface,
            vip_table.len(),
            local_healthy.clone(),
            consensus_fresh.clone(),
            node_id,
        ));
    }
    while workers.next().await.is_some() {}
}

/// Whether this node still owes a `VipReleased` ack for `vip`: it is the recorded previous holder,
/// the assignment has not yet been marked released, and it has not already submitted an ack for this
/// generation. Pure so the dedup/fencing decision can be unit-tested without a live `KafRaft`.
#[must_use]
pub(super) fn should_publish_release(
    assignment: &VipAssignment,
    node_id: u64,
    vip: IpAddr,
    released_generations: &HashMap<IpAddr, u64>,
    unbind_succeeded: bool,
) -> bool {
    unbind_succeeded
        && assignment.previous_holder == Some(node_id)
        && !assignment.previous_holder_released
        && released_generations.get(&vip).copied() != Some(assignment.generation)
}

async fn maybe_publish_release(
    cfg: &Arc<Config>,
    raft: &KafRaft,
    consensus_fresh: &Arc<ConsensusFreshness>,
    node_id: u64,
    vip: IpAddr,
    assignment: &VipAssignment,
    released_generations: &mut HashMap<IpAddr, u64>,
) {
    if !should_publish_release(assignment, node_id, vip, released_generations, true) {
        // Not (or no longer) our obligation. Clear any stale dedup marker only when this node is
        // no longer the previous holder (or it has already been released), mirroring the original
        // two-stage guard; an already-acked-this-generation case leaves the marker in place.
        if assignment.previous_holder != Some(node_id) || assignment.previous_holder_released {
            released_generations.remove(&vip);
        }
        return;
    }

    let req = KafRequest::VipReleased {
        node_id,
        vip,
        generation: assignment.generation,
    };
    match submit::submit_request(cfg, raft, req).await {
        Ok(()) => {
            // #26: a release does not refresh this node's replicated health tick.
            // Extending the health lease here could outlive a stale-holder takeover.
            released_generations.insert(vip, assignment.generation);
        }
        Err(e) => {
            match failure_kind(&e) {
                FailureKind::Unavailable => consensus_fresh.mark_unavailable(),
                FailureKind::Rejected => consensus_fresh.invalidate(),
            }
            tracing::warn!(
                "vip release submit {} gen {}: {}",
                vip,
                assignment.generation,
                e
            );
        }
    }
}

#[cfg(test)]
mod release_tests;

#[cfg(test)]
mod continuity_tests;

#[cfg(test)]
mod cleanup_budget_tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn assignment() -> VipAssignment {
        VipAssignment {
            holder: 2,
            generation: 2,
            previous_holder: Some(1),
            previous_holder_released: false,
            activation_tick: 0,
        }
    }

    fn ready(
        memory: &mut ReconcileMemory,
        assignment: &VipAssignment,
        activated_generation: Option<u64>,
    ) -> bool {
        memory.takeover_delay.ready(
            "192.0.2.1".parse().unwrap(),
            2,
            Some(assignment),
            &BTreeMap::from([(1, 1)]),
            10,
            1,
            activated_generation,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn takeover_wait_includes_cleanup_verification_and_retries() {
        let mut memory = ReconcileMemory::new(Duration::from_secs(1), 1);
        let assignment = assignment();
        assert!(!ready(&mut memory, &assignment, None));
        tokio::time::advance(Duration::from_millis(1_750)).await;
        assert!(
            !ready(&mut memory, &assignment, None),
            "two bare command timeouts do not cover verified cleanup retries"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn takeover_wait_scales_with_the_full_configured_vip_count() {
        for count in [0_u32, 1, 2] {
            let mut memory = ReconcileMemory::new(Duration::from_secs(1), count as usize);
            let assignment = assignment();
            let expected =
                Duration::from_millis(1_250) + Duration::from_secs(12 * u64::from(count));
            assert!(!ready(&mut memory, &assignment, None));
            tokio::time::advance(expected - Duration::from_millis(1)).await;
            assert!(!ready(&mut memory, &assignment, None), "count {count}");
            tokio::time::advance(Duration::from_millis(1)).await;
            assert!(ready(&mut memory, &assignment, None), "count {count}");
        }
    }

    #[test]
    fn maximum_supported_vip_count_preserves_full_cleanup_budget() {
        assert_eq!(
            takeover_lifetime(Duration::from_secs(1), u32::MAX as usize),
            Duration::from_millis(1_250) + Duration::from_secs(12 * u64::from(u32::MAX))
        );
    }

    #[test]
    fn oversized_vip_counts_cannot_truncate_the_cleanup_wait() {
        if let Some(oversized) = (u32::MAX as usize).checked_add(1) {
            for count in [oversized, usize::MAX] {
                assert_eq!(takeover_lifetime(Duration::ZERO, count), Duration::MAX);
            }
        }
    }

    #[test]
    fn maximum_proof_lifetime_does_not_wrap_the_cleanup_wait() {
        assert_eq!(takeover_lifetime(Duration::MAX, 2), Duration::MAX);
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_allowance_preserves_release_and_activation_shortcuts() {
        let mut memory = ReconcileMemory::new(Duration::from_secs(1), 2);
        let mut assignment = assignment();
        assert!(ready(&mut memory, &assignment, Some(assignment.generation)));
        assignment.previous_holder_released = true;
        assert!(ready(&mut memory, &assignment, None));
    }
}
