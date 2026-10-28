//! Independent effect reconciliation and cleanup for one configured VIP.

use super::*;
use crate::vip::witness::{CompletedBindingWitness, EffectAuthorization, RetainedEffect};

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    cfg: Arc<Config>,
    raft: KafRaft,
    state: Arc<RwLock<KafStorageState>>,
    local: Arc<LocalVip>,
    vip: VipAddr,
    iface: String,
    vip_count: usize,
    health: Arc<LocalHealth>,
    freshness: Arc<ConsensusFreshness>,
    node_id: u64,
) {
    let mut memory = ReconcileMemory::new(freshness.lifetime(), vip_count);
    let mut witness: Option<CompletedBindingWitness> = None;
    let mut cleanup_needed = local
        .bound
        .try_read()
        .map_or(true, |bound| bound.contains(&vip.addr));
    let mut assignment = None;
    loop {
        let observed_health = health.observe_continuity();
        let has_leader = raft.metrics().borrow_watched().current_leader.is_some();
        if !memory.vip_effects_armed {
            let metrics = raft.metrics();
            let metrics = metrics.borrow_watched();
            memory.vip_effects_armed = startup_effects_may_arm(
                has_leader,
                metrics.last_applied.as_ref().map(|log| log.index),
                metrics.cluster_committed.as_ref().map(|log| log.index),
            );
            if memory.vip_effects_armed {
                tracing::info!(
                    target: "keepafloatd::vip",
                    address = %vip.addr,
                    last_applied_index = metrics.last_applied.as_ref().map(|log| log.index),
                    "VIP effects armed after diskless replay reached the committed frontier"
                );
            }
        }
        let retained = witness
            .as_ref()
            .and_then(|w| w.retain(&freshness, &vip, &iface));
        if witness.is_some() && retained.is_none() {
            tracing::debug!(address = %vip.addr, "completed VIP retention expired or was revoked");
            cleanup_needed = true;
        }
        if let Ok(current) = state.try_read() {
            assignment = current.vip_assignments.get(&vip.addr).cloned();
            if witness.as_ref().is_some_and(|w| !w.matches_state(&current)) {
                tracing::debug!(address = %vip.addr, "completed VIP assignment no longer matches");
                cleanup_needed = true;
            }
            if memory.vip_effects_armed
                && witness.is_none()
                && assignment.as_ref().is_some_and(|assignment| {
                    should_publish_release(
                        assignment,
                        node_id,
                        vip.addr,
                        &memory.released_generations,
                        true,
                    )
                })
            {
                cleanup_needed = true;
            }
        }
        if cleanup_needed {
            let retry = freshness.acquire(raft.metrics().borrow_watched().current_leader.is_some());
            witness = None;
            memory.activated_generations.remove(&vip.addr);
            memory.announced_release_generations.remove(&vip.addr);
            let table = [(vip, iface.clone())];
            match local
                .unbind_all(
                    &table,
                    cfg.notify.as_deref(),
                    cfg.dry_run,
                    release_notify_state(health.is_healthy()),
                )
                .await
            {
                Ok(()) => {
                    cleanup_needed = false;
                    if memory.vip_effects_armed
                        && let Some(assignment) = assignment.as_ref()
                    {
                        maybe_publish_release(
                            &cfg,
                            &raft,
                            &freshness,
                            node_id,
                            vip.addr,
                            assignment,
                            &mut memory.released_generations,
                        )
                        .await;
                    }
                }
                Err(error) => {
                    tracing::error!(%error, address = %vip.addr, "VIP cleanup remains pending")
                }
            }
            let backoff = tokio::time::sleep(RECONCILE_TICK);
            if let Some(retry) = retry {
                retry.run(backoff).await;
            } else {
                backoff.await;
            }
            continue;
        }

        let acquisition = freshness.acquire(has_leader);
        let candidate =
            if memory.vip_effects_armed && observed_health.check() && acquisition.is_some() {
                state.try_read().ok().map(|current| {
                    let activated = memory.activated_generations.get(&vip.addr).copied();
                    let intent = EffectIntent::capture(
                        vip.addr,
                        node_id,
                        crate::bind_policy::BindGates {
                            has_leader,
                            local_healthy: true,
                            consensus_fresh: true,
                        },
                        &current,
                        activated,
                    );
                    let ready = memory.takeover_delay.ready(
                        vip.addr,
                        node_id,
                        intent.assignment.as_ref(),
                        &current.node_probe_ticks,
                        current.latest_probe_tick,
                        current.stale_missed_probes,
                        activated,
                    );
                    (intent, ready)
                })
            } else {
                None
            };

        if let (Some(permit), Some((intent, ready))) = (acquisition, candidate) {
            if intent.bind_policy && ready {
                let was_bound = local.is_confirmed_bound(vip.addr).await;
                let authorization = Arc::new(EffectAuthorization {
                    permit: Arc::new(permit),
                    health: observed_health,
                    intent: intent.clone(),
                    raft: raft.clone(),
                    state: state.clone(),
                });
                let mutation_started = std::sync::atomic::AtomicBool::new(false);
                let result = authorization
                    .run(local.bind_kernel(
                        &iface,
                        vip.addr,
                        vip.prefix,
                        || authorization.check_applied(),
                        &mutation_started,
                    ))
                    .await;
                #[cfg(test)]
                local.after_bind_result(&result);
                let can_retain = |w: &CompletedBindingWitness| {
                    w.retain(&freshness, &vip, &iface).is_some()
                        && state
                            .try_read()
                            .map_or(true, |current| w.matches_state(&current))
                };
                match result {
                    Some(Ok(Some(first_bind))) => {
                        let completed = CompletedBindingWitness::completed(
                            vip,
                            &iface,
                            intent.clone(),
                            authorization.health.clone(),
                            &authorization.permit,
                        );
                        let Some(completed) = completed.filter(|_| authorization.check_live())
                        else {
                            let retained = witness.as_ref().is_some_and(can_retain);
                            cleanup_needed = !retained;
                            tracing::debug!(address = %vip.addr, retained, cleanup_needed, "VIP bind completion lost authorization");
                            continue;
                        };
                        let effect = Arc::new(RetainedEffect::new(
                            completed.clone(),
                            freshness.clone(),
                            state.clone(),
                        ));
                        witness = Some(completed);
                        memory.released_generations.remove(&vip.addr);
                        if let Some(assignment) = intent.assignment.as_ref() {
                            memory
                                .activated_generations
                                .insert(vip.addr, assignment.generation);
                            let reannounce = should_reannounce_after_release(
                                assignment,
                                vip.addr,
                                was_bound,
                                &memory.announced_release_generations,
                            );
                            if first_bind || reannounce {
                                local
                                    .announce_authorized(&iface, vip.addr, effect.clone())
                                    .await;
                            }
                            if assignment.previous_holder_released && (!was_bound || reannounce) {
                                memory
                                    .announced_release_generations
                                    .insert(vip.addr, assignment.generation);
                            }
                        }
                        if !was_bound && let Some(script) = cfg.notify.as_deref() {
                            local
                                .notifications
                                .send_authorized(script, &vip.addr.to_string(), cfg.dry_run, effect)
                                .await;
                        }
                    }
                    Some(Err(error)) => {
                        health.fail_binding();
                        cleanup_needed = true;
                        tracing::error!(error = %format_args!("{error:#}"), address = %vip.addr, "VIP bind failed; node remains unhealthy until restart");
                    }
                    None | Some(Ok(None)) => {
                        let unchanged = !mutation_started.load(std::sync::atomic::Ordering::SeqCst);
                        let retained = witness.as_ref().is_some_and(can_retain);
                        // Canceling an idempotent reassert does not revoke a completed binding.
                        cleanup_needed = !retained;
                        tracing::debug!(address = %vip.addr, unchanged, retained, cleanup_needed, "VIP operation lost acquisition authorization");
                    }
                }
            } else if witness.is_none()
                && let Some(assignment) = intent.assignment.as_ref()
            {
                cleanup_needed = should_publish_release(
                    assignment,
                    node_id,
                    vip.addr,
                    &memory.released_generations,
                    true,
                );
            }
        }
        if cleanup_needed {
            continue;
        }
        if let Some(witness) = witness.as_ref()
            && let Some(permit) = witness.retain(&freshness, &vip, &iface)
        {
            let tick = tokio::time::sleep(RECONCILE_TICK);
            tokio::pin!(tick);
            let wait = std::future::poll_fn(|cx| {
                if state
                    .try_read()
                    .is_ok_and(|current| !witness.matches_state(&current))
                {
                    return std::task::Poll::Ready(());
                }
                std::future::Future::poll(tick.as_mut(), cx)
            });
            permit.run(witness.health.run(wait)).await;
        } else {
            tokio::time::sleep(RECONCILE_TICK).await;
        }
    }
}
