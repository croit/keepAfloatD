//! Consensus-to-kernel VIP reconciliation.

use super::{
    LocalVip, VipState, release_notify_state, should_reannounce_after_release,
    startup_effects_may_arm,
};
use crate::config::{Config, VipAddr};
use crate::consensus_freshness::ConsensusFreshness;
use crate::health::LocalHealth;
use crate::raft::store::VipAssignment;
use crate::raft::{KafRaft, KafRequest, KafStorageState};
use crate::submit;
use openraft::async_runtime::WatchReceiver;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
mod intent;
use intent::EffectIntent;

/// Reconciliation tick period.
pub(crate) const RECONCILE_TICK: tokio::time::Duration = tokio::time::Duration::from_millis(250);

struct ReconcileMemory {
    released_generations: HashMap<IpAddr, u64>,
    activated_generations: HashMap<IpAddr, u64>,
    announced_release_generations: HashMap<IpAddr, u64>,
    vip_effects_armed: bool,
    takeover_delay: super::takeover::TakeoverDelay,
    next_vip: usize,
}

impl ReconcileMemory {
    fn new(proof_lifetime: tokio::time::Duration, vip_count: usize) -> Self {
        Self {
            released_generations: HashMap::new(),
            activated_generations: HashMap::new(),
            announced_release_generations: HashMap::new(),
            vip_effects_armed: false,
            next_vip: 0,
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

/// Fence effects independently of a stalled health probe, bind, or release submit (#26).
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
    let mut memory = ReconcileMemory::new(consensus_fresh.lifetime(), vip_table.len());
    let (retained, _) = tokio::sync::watch::channel(HashMap::<IpAddr, EffectIntent>::new());
    loop {
        if !consensus_fresh.is_fresh() {
            loop {
                let state = release_notify_state(local_healthy.is_healthy());
                match vip_local
                    .unbind_all(
                        vip_table.as_ref(),
                        cfg.notify.as_deref(),
                        cfg.dry_run,
                        state,
                    )
                    .await
                {
                    Ok(()) => break,
                    Err(error) => {
                        tracing::error!("consensus fencing VIP cleanup failed: {error}");
                        tokio::time::sleep(RECONCILE_TICK).await;
                    }
                }
            }
            memory = ReconcileMemory::new(consensus_fresh.lifetime(), vip_table.len());
            retained.send_modify(HashMap::clear);
            // #26: an upgraded, fenced follower must still acknowledge its proven releases.
            let assignments = sm.read().await.vip_assignments.clone();
            for (vip, _) in vip_table.iter() {
                if let Some(assignment) = assignments.get(&vip.addr) {
                    maybe_publish_release(
                        &cfg,
                        &raft,
                        &consensus_fresh,
                        node_id,
                        vip.addr,
                        assignment,
                        &mut memory.released_generations,
                    )
                    .await;
                }
            }
            tokio::select! {
                result = consensus_fresh.wait_until_fresh() => {
                    if let Err(error) = result { tracing::error!("consensus freshness watch failed: {error}"); return; }
                }
                _ = tokio::time::sleep(RECONCILE_TICK) => {}
            }
            continue;
        }
        let (intent, _) = tokio::sync::watch::channel::<Option<EffectIntent>>(None);
        let (revoked, mut revocations) = tokio::sync::watch::channel(HashSet::<IpAddr>::new());
        let active = async {
            loop {
                tokio::select! {
                    biased;
                    changed = revocations.changed() => {
                        if changed.is_err() { return; }
                    }
                    _ = run_active_reconcile_loop(
                        cfg.clone(), raft.clone(), sm.clone(), vip_local.clone(),
                        vip_table.clone(), local_healthy.clone(), consensus_fresh.clone(),
                        node_id, &mut memory, &intent, &retained,
                    ) => return,
                }
                // Cancel captured work before cleanup; keep unchanged effects under the same proof guard.
                intent.send_replace(None);
                let addresses = revocations.borrow_and_update().clone();
                revoked.send_modify(HashSet::clear);
                revocations.borrow_and_update();
                let cleanup: Vec<_> = vip_table
                    .iter()
                    .filter(|(vip, _)| addresses.contains(&vip.addr))
                    .cloned()
                    .collect();
                for addr in &addresses {
                    memory.activated_generations.remove(addr);
                    memory.announced_release_generations.remove(addr);
                }
                loop {
                    let state = release_notify_state(local_healthy.is_healthy());
                    match vip_local
                        .unbind_all(&cleanup, cfg.notify.as_deref(), cfg.dry_run, state)
                        .await
                    {
                        Ok(()) => break,
                        Err(error) => {
                            tracing::error!("revoked VIP effects cleanup failed: {error}");
                            tokio::time::sleep(RECONCILE_TICK).await;
                        }
                    }
                }
                retained.send_modify(|bound| bound.retain(|addr, _| !addresses.contains(addr)));
            }
        };
        if consensus_fresh
            .run_while_fresh_if(active, || async {
                let captured = intent.borrow().clone();
                let state = sm.read().await;
                let gates = crate::bind_policy::BindGates {
                    has_leader: raft.metrics().borrow_watched().current_leader.is_some(),
                    local_healthy: local_healthy.is_healthy(),
                    consensus_fresh: consensus_fresh.is_fresh(),
                };
                if !gates.has_leader || !gates.local_healthy || !gates.consensus_fresh {
                    return false;
                }
                let bound = retained.borrow();
                let invalid: HashSet<_> = captured
                    .iter()
                    .chain(bound.values())
                    .filter(|effect| !effect.remains_valid(&state, gates))
                    .map(|effect| effect.vip)
                    .collect();
                if !invalid.is_empty() {
                    tracing::debug!(
                        ?invalid,
                        "restarting reconciliation for revoked VIP effects"
                    );
                    revoked.send_modify(|pending| pending.extend(invalid));
                }
                true
            })
            .await
            .is_some()
        {
            return;
        }
        // Proof loss or a closed global gate requires full cleanup, even after a coalesced renewal.
        loop {
            let state = release_notify_state(local_healthy.is_healthy());
            match vip_local
                .unbind_all(
                    vip_table.as_ref(),
                    cfg.notify.as_deref(),
                    cfg.dry_run,
                    state,
                )
                .await
            {
                Ok(()) => break,
                Err(error) => {
                    tracing::error!("canceled VIP effects cleanup failed: {error}");
                    tokio::time::sleep(RECONCILE_TICK).await;
                }
            }
        }
        retained.send_modify(HashMap::clear);
        memory.activated_generations.clear();
        memory.announced_release_generations.clear();
        if !consensus_fresh.is_fresh() {
            tracing::warn!("consensus proof expired or failed; withdrawing local VIPs");
        }
    }
}

/// Run the VIP reconciliation loop until cancelled (typically via task abort on shutdown).
#[allow(clippy::too_many_arguments)]
async fn run_active_reconcile_loop(
    cfg: Arc<Config>,
    raft: KafRaft,
    sm: Arc<RwLock<KafStorageState>>,
    vip_local: Arc<LocalVip>,
    vip_table: Arc<Vec<(VipAddr, String)>>,
    local_healthy: Arc<LocalHealth>,
    consensus_fresh: Arc<ConsensusFreshness>,
    node_id: u64,
    memory: &mut ReconcileMemory,
    intent: &tokio::sync::watch::Sender<Option<EffectIntent>>,
    retained: &tokio::sync::watch::Sender<HashMap<IpAddr, EffectIntent>>,
) {
    let mut tick = tokio::time::interval(RECONCILE_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let ReconcileMemory {
        released_generations,
        activated_generations,
        announced_release_generations,
        vip_effects_armed,
        takeover_delay,
        next_vip,
    } = memory;

    loop {
        intent.send_replace(None);
        tick.tick().await;

        let (has_leader, last_applied_index, cluster_committed_index) = {
            let metrics = raft.metrics();
            let metrics = metrics.borrow_watched();
            (
                metrics.current_leader.is_some(),
                metrics.last_applied.as_ref().map(|log_id| log_id.index),
                metrics
                    .cluster_committed
                    .as_ref()
                    .map(|log_id| log_id.index),
            )
        };
        if !*vip_effects_armed
            && startup_effects_may_arm(has_leader, last_applied_index, cluster_committed_index)
        {
            *vip_effects_armed = true;
            tracing::info!(
                target: "keepafloatd::vip",
                last_applied_index,
                "VIP effects armed after diskless replay reached the committed frontier"
            );
        }
        if !*vip_effects_armed {
            continue;
        }

        for _ in 0..vip_table.len() {
            let (vip, iface) = &vip_table[*next_vip];
            *next_vip = (*next_vip + 1) % vip_table.len();
            let addr = vip.addr;
            let prefix = vip.prefix;
            let already_bound = vip_local.is_confirmed_bound(addr).await;
            let activated_generation = activated_generations.get(&addr).copied();
            let local_ok = local_healthy.is_healthy();
            let (captured, takeover_ready) = {
                let state = sm.read().await;
                let captured = EffectIntent::capture(
                    addr,
                    node_id,
                    crate::bind_policy::BindGates {
                        has_leader: raft.metrics().borrow_watched().current_leader.is_some(),
                        local_healthy: local_ok,
                        consensus_fresh: consensus_fresh.is_fresh(),
                    },
                    &state,
                    activated_generation,
                );
                let ready = takeover_delay.ready(
                    addr,
                    node_id,
                    captured.assignment.as_ref(),
                    &state.node_probe_ticks,
                    state.latest_probe_tick,
                    state.stale_missed_probes,
                    activated_generation,
                );
                (captured, ready)
            };
            let want_bind = captured.bind_policy && takeover_ready;
            intent.send_replace(Some(captured.clone()));
            let assignment = captured.assignment.as_ref();

            if want_bind {
                released_generations.remove(&addr);
                // #26: a failed syscall can leave an unproven address/marker behind. Keep its
                // captured intent until verified unbind, and grant activation only on success.
                retained.send_modify(|bound| {
                    bound.insert(addr, captured.clone());
                });
                // Snapshot before bind so we detect genuine first-bind transitions only.
                let was_not_bound = cfg.notify.is_some() && !already_bound;
                if let Err(e) = vip_local.bind(iface, addr, prefix).await {
                    local_healthy.fail_binding();
                    tracing::error!(%addr, %iface, error = %format_args!("{e:#}"),
                        "VIP bind failed; node remains unhealthy until restart");
                } else {
                    retained.send_modify(|bound| {
                        bound.insert(addr, captured.clone().activated());
                    });
                    intent.send_replace(Some(captured.clone().activated()));
                    if let Some(assignment) = assignment {
                        activated_generations.insert(addr, assignment.generation);
                        if !already_bound && assignment.previous_holder_released {
                            // The first bind already emitted GARP for this released generation.
                            announced_release_generations.insert(addr, assignment.generation);
                        } else if should_reannounce_after_release(
                            assignment,
                            addr,
                            already_bound,
                            announced_release_generations,
                        ) {
                            // A stale/orphaned previous kernel may have re-poisoned neighbor caches
                            // after our first bind. Once its replicated release arrives, refresh the
                            // surviving holder's MAC exactly once for this handoff generation.
                            vip_local.announce(iface, addr).await;
                            announced_release_generations.insert(addr, assignment.generation);
                        }
                    }
                    if was_not_bound && let Some(script) = cfg.notify.as_deref() {
                        vip_local
                            .notify_transition(
                                script,
                                &addr.to_string(),
                                VipState::Master,
                                cfg.dry_run,
                            )
                            .await;
                    }
                }
                continue;
            }

            // A failed mandatory gate or a different assignment generation invalidates local proof
            // that this process safely crossed the current handoff fence. Clear it before unbind so
            // even a failed `ip addr del` cannot authorize a later generation.
            activated_generations.remove(&addr);
            announced_release_generations.remove(&addr);
            // Snapshot before unbind so we detect genuine last-release transitions only.
            let was_bound = cfg.notify.is_some() && already_bound;
            let unbind_succeeded = match vip_local.unbind(iface, addr, prefix).await {
                Ok(()) => {
                    retained.send_modify(|bound| {
                        bound.remove(&addr);
                    });
                    if was_bound {
                        // FAULT is only for local health failures; cluster events use BACKUP.
                        if let Some(script) = cfg.notify.as_deref() {
                            vip_local
                                .notify_transition(
                                    script,
                                    &addr.to_string(),
                                    release_notify_state(local_ok),
                                    cfg.dry_run,
                                )
                                .await;
                        }
                    }
                    true
                }
                Err(e) => {
                    tracing::warn!("unbind {}: {}", addr, e);
                    false
                }
            };

            match (unbind_succeeded, assignment) {
                (true, Some(assignment)) => {
                    maybe_publish_release(
                        &cfg,
                        &raft,
                        &consensus_fresh,
                        node_id,
                        addr,
                        assignment,
                        released_generations,
                    )
                    .await;
                }
                (_, None) => {
                    released_generations.remove(&addr);
                }
                (false, Some(_)) => {}
            }
        }
    }
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
            consensus_fresh.invalidate();
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
