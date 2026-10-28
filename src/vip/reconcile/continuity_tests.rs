use super::*;
use std::time::Duration;

async fn completed_effect_authorization(
    local: &Arc<LocalVip>,
    raft: KafRaft,
    state: Arc<RwLock<KafStorageState>>,
    freshness: &Arc<ConsensusFreshness>,
    health: &LocalHealth,
) -> Arc<crate::vip::witness::EffectAuthorization> {
    use std::os::unix::process::ExitStatusExt;
    let address = "192.0.2.99".parse().unwrap();
    let intent = EffectIntent::capture(
        address,
        1,
        crate::bind_policy::BindGates {
            has_leader: true,
            local_healthy: true,
            consensus_fresh: true,
        },
        &*state.read().await,
        None,
    );
    assert!(intent.bind_policy);
    let authorization = Arc::new(crate::vip::witness::EffectAuthorization {
        permit: Arc::new(freshness.acquire(true).unwrap()),
        health: health.observe_continuity(),
        intent,
        raft,
        state,
    });
    local
        .force_next_bind_result(
            ("lo", address, 32),
            Ok(std::process::ExitStatus::from_raw(0)),
        )
        .await;
    let started = std::sync::atomic::AtomicBool::new(false);
    assert_eq!(
        authorization
            .run(local.bind_kernel(
                "lo",
                address,
                32,
                || authorization.check_applied(),
                &started
            ))
            .await
            .unwrap()
            .unwrap(),
        Some(true)
    );
    assert!(local.is_confirmed_bound(address).await);
    authorization
}

/// The effect authorization the worker builds from a completed binding of 192.0.2.99.
fn retained_effect(
    authorization: &crate::vip::witness::EffectAuthorization,
    freshness: &Arc<ConsensusFreshness>,
    state: Arc<RwLock<KafStorageState>>,
) -> Arc<crate::vip::witness::RetainedEffect> {
    let witness = crate::vip::witness::CompletedBindingWitness::completed(
        crate::config::VipAddr::host("192.0.2.99".parse().unwrap()),
        "lo",
        authorization.intent.clone(),
        authorization.health.clone(),
        &authorization.permit,
    )
    .expect("a completed bind yields a witness");
    Arc::new(crate::vip::witness::RetainedEffect::new(
        witness,
        freshness.clone(),
        state,
    ))
}

#[tokio::test(start_paused = true)]
async fn completed_bind_announcement_survives_state_contention() {
    use std::os::unix::process::ExitStatusExt;
    let (_, raft, network, state, mut controls) = release_tests::cluster().await;
    let local = LocalVip::new(false);
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
    verified_proof(&freshness, &controls, &network).await;
    let health = LocalHealth::new(true);
    let authorization =
        completed_effect_authorization(&local, raft.clone(), state.clone(), &freshness, &health)
            .await;
    let effect = retained_effect(&authorization, &freshness, state.clone());
    let address = "192.0.2.99".parse().unwrap();
    let (complete, pending) = tokio::sync::oneshot::channel();
    *local.next_announcement.lock().await = Some(pending);
    let held_state = state.write().await;
    local.announce_authorized("lo", address, effect).await;
    let queued = local.next_announcement.lock().await.is_none();
    tokio::task::yield_now().await;
    let waiting = !complete.is_closed();
    let retained = local.is_confirmed_bound(address).await;
    drop(held_state);
    complete.send(()).unwrap();
    local.announcements.cancel(address).await;
    local
        .force_unbind_results(
            ("lo", address, 32),
            vec![Ok(std::process::ExitStatus::from_raw(0))],
        )
        .await;
    local
        .force_marker_delete_results(address, vec![Ok(std::process::ExitStatus::from_raw(0))])
        .await;
    local.unbind("lo", address, 32).await.unwrap();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(waiting && retained);
    assert!(
        queued,
        "state contention discarded the completed bind announcement"
    );
}

#[tokio::test(start_paused = true)]
async fn completed_bind_master_waits_for_state_and_honors_revocation() {
    use std::os::unix::process::ExitStatusExt;
    use tokio::sync::oneshot::error::TryRecvError;
    let drain_timeout = Duration::from_secs(1);
    for (dry_run, outcome) in [true, false]
        .into_iter()
        .flat_map(|dry_run| ["resume", "expire", "seal"].map(|outcome| (dry_run, outcome)))
    {
        let (_, raft, network, state, mut controls) = release_tests::cluster().await;
        let local = LocalVip::new(false);
        let freshness = Arc::new(ConsensusFreshness::new(drain_timeout / 2));
        verified_proof(&freshness, &controls, &network).await;
        let health = LocalHealth::new(true);
        let authorization = completed_effect_authorization(
            &local,
            raft.clone(),
            state.clone(),
            &freshness,
            &health,
        )
        .await;
        let deadline = freshness
            .retain(&authorization.permit.stamp())
            .unwrap()
            .deadline();
        let effect = retained_effect(&authorization, &freshness, state.clone());
        let address = "192.0.2.99".parse().unwrap();
        let (started, mut observed) = tokio::sync::oneshot::channel();
        *local.notifications.next_authorized_hook.lock().await = Some(Box::pin(async move {
            started.send(()).unwrap();
        }));
        let held_state = state.write().await;
        let drain_deadline = tokio::time::Instant::now() + drain_timeout;
        let mut notification = Box::pin(async {
            local
                .notifications
                .send_authorized(
                    "/nonexistent/completed-effect-notify",
                    "192.0.2.99",
                    dry_run,
                    effect,
                )
                .await;
            if !dry_run {
                local.notifications.shutdown(drain_timeout).await.unwrap();
            }
        });
        let waiting = futures::poll!(notification.as_mut()).is_pending();
        let before_unlock = observed.try_recv();
        match outcome {
            "expire" => {
                tokio::time::advance(deadline - tokio::time::Instant::now()).await;
            }
            "seal" => controls.shutdown().await.unwrap(),
            _ => (),
        }
        let stopped_while_locked = if waiting && outcome != "resume" {
            tokio::time::timeout(Duration::from_millis(1), notification.as_mut())
                .await
                .expect("revocation must cancel a MASTER hook while state remains locked");
            true
        } else {
            false
        };
        drop(held_state);
        if waiting && !stopped_while_locked {
            notification.as_mut().await;
        }
        drop(notification);
        assert!(
            tokio::time::Instant::now() < drain_deadline,
            "authorization must finish without relying on the forced queue drain"
        );
        let after_completion = observed.try_recv();
        local
            .force_unbind_results(
                ("lo", address, 32),
                vec![Ok(std::process::ExitStatus::from_raw(0))],
            )
            .await;
        local
            .force_marker_delete_results(address, vec![Ok(std::process::ExitStatus::from_raw(0))])
            .await;
        local.unbind("lo", address, 32).await.unwrap();
        controls.shutdown().await.unwrap();
        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
        assert_eq!(
            before_unlock,
            Err(TryRecvError::Empty),
            "MASTER notification must remain queued until state can be validated"
        );
        assert!(
            waiting,
            "state contention discarded the completed bind MASTER hook"
        );
        assert_eq!(stopped_while_locked, outcome != "resume");
        assert_eq!(
            after_completion,
            if outcome == "resume" {
                Ok(())
            } else {
                Err(TryRecvError::Closed)
            },
            "only an authorized notification may start"
        );
    }
}

async fn verified_proof(
    freshness: &ConsensusFreshness,
    controls: &crate::raft::RaftControlTasks,
    network: &crate::raft::RaftNetworkImpl,
) {
    let runtime = controls.runtime();
    let deadline = runtime.current().unwrap().check().unwrap();
    tokio::time::advance(Duration::from_nanos(1)).await;
    let mut proof = Box::pin(runtime.submit_health_proof(network, true));
    loop {
        if let std::task::Poll::Ready(proof) = futures::poll!(proof.as_mut()) {
            freshness.record_applied(proof.unwrap()).unwrap();
            return;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::advance(Duration::from_millis(1)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn release_timeout_keeps_another_completed_vip_until_its_old_deadline() {
    let mut cfg = (*release_tests::cluster_config()).clone();
    cfg.submit_timeout_ms = 10;
    let (cfg, raft, network, state, mut controls) =
        release_tests::cluster_from_config(Arc::new(cfg)).await;
    let table = Arc::new(cfg.sorted_vips());
    let address = table[0].0.addr;
    let local = LocalVip::new(true);
    let health = Arc::new(LocalHealth::new(true));
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
    verified_proof(&freshness, &controls, &network).await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let deadline = freshness.retain(&stamp).unwrap().deadline();
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg.clone(),
        raft.clone(),
        state.clone(),
        local.clone(),
        table,
        health.clone(),
        freshness.clone(),
        1,
    ));
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    assert!(local.is_confirmed_bound(address).await);
    let lock = state.write().await;
    let assignment = VipAssignment {
        holder: 2,
        generation: 2,
        previous_holder: Some(1),
        previous_holder_released: false,
        activation_tick: 0,
    };
    let mut released = HashMap::new();
    let release_address = "192.0.2.100".parse().unwrap();
    {
        {
            let release = maybe_publish_release(
                &cfg,
                &raft,
                &freshness,
                1,
                release_address,
                &assignment,
                &mut released,
            );
            tokio::pin!(release);
            assert!(futures::poll!(&mut release).is_pending());
            tokio::time::advance(Duration::from_millis(10)).await;
            release.await;
        }
    }
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let retained = local.is_confirmed_bound(address).await;
    let retained_deadline = freshness.retain(&stamp).map(|permit| permit.deadline());
    let acquire_closed = freshness.acquire(true).is_none();
    drop(lock);
    drop(reconcile);
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        released.is_empty(),
        "a timed out release must not be acknowledged"
    );
    assert!(health.is_healthy());
    assert!(acquire_closed);
    assert!(
        retained,
        "one release timeout removed another completed VIP"
    );
    assert_eq!(
        retained_deadline,
        Some(deadline),
        "release must not extend the old proof"
    );
}

#[tokio::test(start_paused = true)]
async fn recovered_health_cannot_retain_a_completed_witness() {
    let (cfg, raft, network, state, mut controls) = release_tests::cluster().await;
    let table = Arc::new(cfg.sorted_vips());
    let address = table[0].0.addr;
    let local = LocalVip::new(true);
    let health = Arc::new(LocalHealth::new(true));
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
    verified_proof(&freshness, &controls, &network).await;
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state,
        local.clone(),
        table,
        health.clone(),
        freshness.clone(),
        1,
    ));
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    assert!(local.is_confirmed_bound(address).await);
    let before = local.bind_attempts(address).await;
    freshness.mark_unavailable();
    health.observe_probe(false);
    health.observe_probe(true);
    assert!(health.is_healthy());
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let removed = local.bound_addrs().await.is_empty();
    let after = local.bind_attempts(address).await;
    drop(reconcile);
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        removed,
        "coalesced health recovery revived a completed witness"
    );
    assert_eq!(before, after, "soft loss must not start a replacement bind");
}

#[tokio::test(start_paused = true)]
async fn release_authority_rejection_revokes_completed_retention() {
    let (cfg, raft, network, _, mut controls) = release_tests::cluster().await;
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
    verified_proof(&freshness, &controls, &network).await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let mut mismatched = (*cfg).clone();
    mismatched.node_id = 2;
    let assignment = VipAssignment {
        holder: 3,
        generation: 2,
        previous_holder: Some(2),
        previous_holder_released: false,
        activation_tick: 0,
    };
    let mut released = HashMap::new();
    maybe_publish_release(
        &Arc::new(mismatched),
        &raft,
        &freshness,
        2,
        "192.0.2.100".parse().unwrap(),
        &assignment,
        &mut released,
    )
    .await;
    let revoked = freshness.retain(&stamp).is_none();
    let acquire_closed = freshness.acquire(true).is_none();
    let session_live = controls.runtime().current().unwrap().check().is_ok();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        session_live,
        "fixture accidentally revoked the underlying runtime"
    );
    assert!(
        revoked,
        "an observed release authority rejection must revoke retention"
    );
    assert!(acquire_closed);
    assert!(released.is_empty());
}

#[tokio::test(start_paused = true)]
async fn soft_loss_before_reassert_syscall_preserves_only_the_completed_witness() {
    assert_pre_mutation_revocation(false).await;
}

#[tokio::test(start_paused = true)]
async fn state_contention_before_reassert_preserves_only_the_old_deadline() {
    assert_pre_mutation_revocation(true).await;
}

async fn assert_pre_mutation_revocation(block_state: bool) {
    for hard in [false, true] {
        let (cfg, raft, network, state, mut controls) = release_tests::cluster().await;
        let table = Arc::new(cfg.sorted_vips());
        let address = table[0].0.addr;
        let local = LocalVip::new(true);
        let health = Arc::new(LocalHealth::new(true));
        let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
        verified_proof(&freshness, &controls, &network).await;
        let mut reconcile = Box::pin(run_reconcile_loop(
            cfg,
            raft.clone(),
            state.clone(),
            local.clone(),
            table,
            health.clone(),
            freshness.clone(),
            1,
        ));
        assert!(futures::poll!(reconcile.as_mut()).is_pending());
        assert!(local.is_confirmed_bound(address).await);
        let prevent_syscall = local.bind_starts.lock().await;
        tokio::time::advance(RECONCILE_TICK).await;
        tokio::time::timeout(Duration::from_millis(10), async {
            while local
                .bind_invocations
                .load(std::sync::atomic::Ordering::SeqCst)
                < 2
            {
                assert!(futures::poll!(reconcile.as_mut()).is_pending());
                tokio::time::advance(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the reassert must enter before its syscall is blocked");
        let held_state = if block_state {
            Some(state.write().await)
        } else {
            None
        };
        if hard {
            freshness.invalidate();
        } else {
            freshness.mark_unavailable();
        }
        assert!(futures::poll!(reconcile.as_mut()).is_pending());
        let retained = local.is_confirmed_bound(address).await;
        let still_healthy = health.is_healthy();
        tokio::time::advance(freshness.lifetime()).await;
        assert!(futures::poll!(reconcile.as_mut()).is_pending());
        let expired_and_removed = local.bound_addrs().await.is_empty();
        drop(reconcile);
        drop(prevent_syscall);
        drop(held_state);
        controls.shutdown().await.unwrap();
        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
        assert_eq!(
            retained, !hard,
            "incorrect retention for hard rejection={hard}"
        );
        assert!(
            still_healthy,
            "revoked permission is not a sticky bind syscall fault"
        );
        assert!(
            expired_and_removed,
            "state contention cannot postpone proof expiry"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn soft_loss_during_started_marker_reassert_retains_vip_until_old_deadline() {
    assert_started_bind_revocation(true, StartedBindCase::CompletedSoftLoss).await;
}

#[tokio::test(start_paused = true)]
async fn soft_loss_during_started_address_reassert_retains_vip_until_old_deadline() {
    assert_started_bind_revocation(false, StartedBindCase::CompletedSoftLoss).await;
}

#[tokio::test(start_paused = true)]
async fn hard_loss_during_started_reassert_removes_completed_vip() {
    for pause_marker in [true, false] {
        assert_started_bind_revocation(pause_marker, StartedBindCase::CompletedHardLoss).await;
    }
}

#[tokio::test(start_paused = true)]
async fn local_health_loss_during_started_reassert_removes_completed_vip() {
    for pause_marker in [true, false] {
        assert_started_bind_revocation(pause_marker, StartedBindCase::CompletedHealthLoss).await;
    }
}

#[tokio::test(start_paused = true)]
async fn assignment_loss_during_started_reassert_removes_completed_vip() {
    for pause_marker in [true, false] {
        assert_started_bind_revocation(pause_marker, StartedBindCase::CompletedReassignment).await;
    }
}

#[tokio::test(start_paused = true)]
async fn soft_loss_during_started_first_bind_requires_cleanup() {
    for pause_marker in [true, false] {
        assert_started_bind_revocation(pause_marker, StartedBindCase::FirstBindSoftLoss).await;
    }
}

#[derive(Clone, Copy, Debug)]
enum StartedBindCase {
    CompletedSoftLoss,
    CompletedHardLoss,
    CompletedHealthLoss,
    CompletedReassignment,
    FirstBindSoftLoss,
}

async fn assert_started_bind_revocation(pause_marker: bool, case: StartedBindCase) {
    use crate::vip::effects::{
        IP_COMMAND_TIMEOUT, bind_command_arguments, marker_route_replace_arguments,
    };
    use std::os::unix::process::ExitStatusExt;
    use std::process::ExitStatus;
    use tokio::process::Command;

    let (cfg, raft, network, state, mut controls) = release_tests::cluster().await;
    let table = Arc::new(cfg.sorted_vips());
    let (vip, iface) = table[0].clone();
    let local = LocalVip::new(false);
    let health = Arc::new(LocalHealth::new(true));
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
    verified_proof(&freshness, &controls, &network).await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let deadline = freshness.retain(&stamp).unwrap().deadline();
    let (announced, announcement) = tokio::sync::oneshot::channel();
    *local.next_announcement.lock().await = Some(announcement);
    announced.send(()).unwrap();
    local
        .force_next_bind_result((&iface, vip.addr, vip.prefix), Ok(ExitStatus::from_raw(0)))
        .await;
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state.clone(),
        local.clone(),
        table,
        health.clone(),
        freshness.clone(),
        1,
    ));
    let completed_binding = !matches!(case, StartedBindCase::FirstBindSoftLoss);
    if completed_binding {
        assert!(futures::poll!(reconcile.as_mut()).is_pending());
        assert!(local.is_confirmed_bound(vip.addr).await);
        local.announcements.cancel(vip.addr).await;
        local
            .force_next_bind_result((&iface, vip.addr, vip.prefix), Ok(ExitStatus::from_raw(0)))
            .await;
    }
    local
        .force_unbind_results(
            (&iface, vip.addr, vip.prefix),
            vec![Ok(ExitStatus::from_raw(0))],
        )
        .await;
    local
        .force_marker_delete_results(vip.addr, vec![Ok(ExitStatus::from_raw(0))])
        .await;
    let mut command = Command::new("ip");
    if pause_marker {
        command.args(marker_route_replace_arguments(
            vip.addr,
            local.ownership_marker.protocol(),
        ));
    } else {
        command.args(bind_command_arguments(vip.addr, vip.prefix, &iface));
    }
    let finish_command = local
        .commands
        .pause_next(&command, IP_COMMAND_TIMEOUT, false);
    if completed_binding {
        tokio::time::advance(RECONCILE_TICK).await;
    }
    tokio::time::timeout(Duration::from_millis(10), async {
        loop {
            assert!(futures::poll!(reconcile.as_mut()).is_pending());
            if local
                .commands
                .remaining(&command, IP_COMMAND_TIMEOUT, false)
                == 0
            {
                break;
            }
            tokio::time::advance(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the kernel command must start before publication becomes unavailable");
    assert!(!finish_command.is_closed());
    assert_eq!(local.is_confirmed_bound(vip.addr).await, completed_binding);
    let mutation_count = || {
        local
            .commands
            .calls()
            .iter()
            .filter(|call| call.arguments.iter().any(|arg| arg == "replace"))
            .count()
    };
    let mutations_started = mutation_count();
    match case {
        StartedBindCase::CompletedHealthLoss => {
            health.observe_probe(false);
        }
        StartedBindCase::CompletedReassignment => {
            let mut current = state.write().await;
            let assignment = current.vip_assignments.get_mut(&vip.addr).unwrap();
            assignment.holder = 2;
            assignment.generation += 1;
        }
        _ => (),
    }
    let hard = matches!(case, StartedBindCase::CompletedHardLoss);
    if hard {
        freshness.invalidate();
    } else {
        freshness.mark_unavailable();
    }
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let canceled = finish_command.is_closed();
    let retained = local.is_confirmed_bound(vip.addr).await;
    let delete_deferred = local.remaining_forced_unbind_results().await == 1;
    let acquire_closed = freshness.acquire(true).is_none();
    let retained_deadline = freshness.retain(&stamp).map(|permit| permit.deadline());
    tokio::time::advance(deadline - tokio::time::Instant::now() - Duration::from_millis(1)).await;
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let retained_before_deadline = local.is_confirmed_bound(vip.addr).await;
    let delete_deferred_before_deadline = local.remaining_forced_unbind_results().await == 1;
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(
        freshness.retain(&stamp).is_none(),
        "retention must expire at the original proof deadline"
    );
    // Tokio rounds timer wakeups to milliseconds, separately from proof expiry.
    tokio::time::sleep_until(deadline).await;
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let removed_at_deadline = local.bound_addrs().await.is_empty();
    let delete_completed = local.remaining_forced_unbind_results().await == 0;
    let mutations_after_loss = mutation_count();
    drop(reconcile);
    drop(finish_command);
    local.unbind(&iface, vip.addr, vip.prefix).await.unwrap();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();

    assert!(canceled && acquire_closed);
    assert_eq!(
        health.is_healthy(),
        !matches!(case, StartedBindCase::CompletedHealthLoss),
        "cancellation must preserve the actual local probe result"
    );
    assert_eq!(retained_deadline, if hard { None } else { Some(deadline) });
    assert_eq!(mutations_after_loss, mutations_started);
    assert!(removed_at_deadline && delete_completed);
    let should_retain = matches!(case, StartedBindCase::CompletedSoftLoss);
    assert_eq!(
        (
            retained,
            delete_deferred,
            retained_before_deadline,
            delete_deferred_before_deadline
        ),
        (should_retain, should_retain, should_retain, should_retain),
        "started command cancellation violated retention: {case:?}, pause_marker={pause_marker}"
    );
}

#[tokio::test(start_paused = true)]
async fn soft_loss_after_successful_reassert_retains_vip_until_old_deadline() {
    assert_successful_bind_revocation(BindCompletionCase::ReassertSoftLoss).await;
}

#[tokio::test(start_paused = true)]
async fn soft_loss_after_successful_first_bind_requires_cleanup() {
    assert_successful_bind_revocation(BindCompletionCase::FirstBindSoftLoss).await;
}

#[tokio::test(start_paused = true)]
async fn hard_loss_after_successful_reassert_removes_completed_vip() {
    assert_successful_bind_revocation(BindCompletionCase::ReassertHardLoss).await;
}

#[tokio::test(start_paused = true)]
async fn local_health_loss_after_successful_reassert_removes_completed_vip() {
    assert_successful_bind_revocation(BindCompletionCase::ReassertHealthLoss).await;
}

#[tokio::test(start_paused = true)]
async fn assignment_loss_after_successful_reassert_removes_completed_vip() {
    assert_successful_bind_revocation(BindCompletionCase::ReassertReassignment).await;
}

#[derive(Clone, Copy, Debug)]
enum BindCompletionCase {
    ReassertSoftLoss,
    FirstBindSoftLoss,
    ReassertHardLoss,
    ReassertHealthLoss,
    ReassertReassignment,
}

async fn assert_successful_bind_revocation(case: BindCompletionCase) {
    use std::os::unix::process::ExitStatusExt;
    use std::process::ExitStatus;

    let (cfg, raft, network, state, mut controls) = release_tests::cluster().await;
    let table = Arc::new(cfg.sorted_vips());
    let (vip, iface) = table[0].clone();
    let local = LocalVip::new(false);
    let health = Arc::new(LocalHealth::new(true));
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
    verified_proof(&freshness, &controls, &network).await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let deadline = freshness.retain(&stamp).unwrap().deadline();
    let (announced, announcement) = tokio::sync::oneshot::channel();
    *local.next_announcement.lock().await = Some(announcement);
    announced.send(()).unwrap();
    local
        .force_next_bind_result((&iface, vip.addr, vip.prefix), Ok(ExitStatus::from_raw(0)))
        .await;
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state.clone(),
        local.clone(),
        table,
        health.clone(),
        freshness.clone(),
        1,
    ));
    let completed_binding = !matches!(case, BindCompletionCase::FirstBindSoftLoss);
    if completed_binding {
        assert!(futures::poll!(reconcile.as_mut()).is_pending());
        assert!(local.is_confirmed_bound(vip.addr).await);
        local.announcements.cancel(vip.addr).await;
        local
            .force_next_bind_result((&iface, vip.addr, vip.prefix), Ok(ExitStatus::from_raw(0)))
            .await;
        let (announced, announcement) = tokio::sync::oneshot::channel();
        *local.next_announcement.lock().await = Some(announcement);
        announced.send(()).unwrap();
    }
    local
        .force_unbind_results(
            (&iface, vip.addr, vip.prefix),
            vec![Ok(ExitStatus::from_raw(0))],
        )
        .await;
    local
        .force_marker_delete_results(vip.addr, vec![Ok(ExitStatus::from_raw(0))])
        .await;
    let (observed, mut completion) = tokio::sync::oneshot::channel();
    let completion_freshness = freshness.clone();
    let completion_health = health.clone();
    let completion_state = state.clone();
    *local.next_bind_result_hook.lock().unwrap() = Some(Box::new(move |result| {
        let Some(Ok(Some(first_bind))) = result else {
            panic!("the authorized bind must succeed before revocation: {result:?}");
        };
        assert_eq!(*first_bind, !completed_binding);
        match case {
            BindCompletionCase::ReassertHealthLoss => {
                completion_health.observe_probe(false);
            }
            BindCompletionCase::ReassertReassignment => {
                let mut current = completion_state.try_write().unwrap();
                let assignment = current.vip_assignments.get_mut(&vip.addr).unwrap();
                assignment.holder = 2;
                assignment.generation += 1;
            }
            _ => (),
        }
        if matches!(case, BindCompletionCase::ReassertHardLoss) {
            completion_freshness.invalidate();
        } else {
            completion_freshness.mark_unavailable();
        }
        observed.send(()).unwrap();
    }));
    if completed_binding {
        tokio::time::sleep_until(tokio::time::Instant::now() + RECONCILE_TICK).await;
    }
    tokio::time::timeout(Duration::from_millis(10), async {
        loop {
            assert!(futures::poll!(reconcile.as_mut()).is_pending());
            if completion.try_recv().is_ok() {
                break;
            }
            tokio::time::advance(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the worker must reach the successful bind completion boundary");
    assert!(local.next_bind_result_hook.lock().unwrap().is_none());
    let retained = local.is_confirmed_bound(vip.addr).await;
    let delete_deferred = local.remaining_forced_unbind_results().await == 1;
    let marker_delete_deferred = local.remaining_marker_delete_results().await == 1;
    let acquire_closed = freshness.acquire(true).is_none();
    let retained_deadline = freshness.retain(&stamp).map(|permit| permit.deadline());
    tokio::time::advance(deadline - tokio::time::Instant::now() - Duration::from_millis(1)).await;
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let retained_before_deadline = local.is_confirmed_bound(vip.addr).await;
    let delete_deferred_before_deadline = local.remaining_forced_unbind_results().await == 1;
    let marker_delete_deferred_before_deadline = local.remaining_marker_delete_results().await == 1;
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(
        freshness.retain(&stamp).is_none(),
        "retention must expire at the original proof deadline"
    );
    // Tokio rounds timer wakeups to milliseconds, separately from proof expiry.
    tokio::time::sleep_until(deadline).await;
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let removed_at_deadline = local.bound_addrs().await.is_empty();
    let delete_completed = local.remaining_forced_unbind_results().await == 0
        && local.remaining_marker_delete_results().await == 0;
    let mutations = local
        .commands
        .calls()
        .iter()
        .filter(|call| call.arguments.iter().any(|arg| arg == "replace"))
        .count();
    let announcement_deferred = local.next_announcement.lock().await.is_some();
    drop(reconcile);
    local.unbind(&iface, vip.addr, vip.prefix).await.unwrap();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();

    assert!(acquire_closed && announcement_deferred);
    assert_eq!(
        health.is_healthy(),
        !matches!(case, BindCompletionCase::ReassertHealthLoss)
    );
    assert_eq!(
        retained_deadline,
        if matches!(case, BindCompletionCase::ReassertHardLoss) {
            None
        } else {
            Some(deadline)
        }
    );
    assert_eq!(mutations, if completed_binding { 4 } else { 2 });
    assert!(removed_at_deadline && delete_completed);
    let should_retain = matches!(case, BindCompletionCase::ReassertSoftLoss);
    assert_eq!(
        (
            retained,
            delete_deferred,
            marker_delete_deferred,
            retained_before_deadline,
            delete_deferred_before_deadline,
            marker_delete_deferred_before_deadline
        ),
        (
            should_retain,
            should_retain,
            should_retain,
            should_retain,
            should_retain,
            should_retain
        ),
        "successful bind completion violated retention: {case:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn slow_cleanup_of_one_vip_does_not_delay_another_revoked_witness() {
    let mut cfg = (*release_tests::cluster_config()).clone();
    let mut extra = cfg.vips[0].clone();
    extra.address = VipAddr::host("192.0.2.100".parse().unwrap());
    cfg.vips.push(extra);
    let (cfg, raft, network, state, mut controls) =
        release_tests::cluster_from_config(Arc::new(cfg)).await;
    let table = Arc::new(cfg.sorted_vips());
    let first = table[0].0.addr;
    let other = table[1].0.addr;
    let local = LocalVip::new(true);
    let health = Arc::new(LocalHealth::new(true));
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
    verified_proof(&freshness, &controls, &network).await;
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state,
        local.clone(),
        table,
        health,
        freshness.clone(),
        1,
    ));
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    assert!(local.is_confirmed_bound(first).await);
    assert!(local.is_confirmed_bound(other).await);
    let (release, pause) = tokio::sync::oneshot::channel();
    local.next_unbind.lock().await.insert(first, pause);
    freshness.invalidate();
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let remaining = local.bound_addrs().await;
    assert!(!release.is_closed(), "the slow cleanup was not held");
    drop(release);
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let finally_empty = local.bound_addrs().await.is_empty();
    drop(reconcile);
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert_eq!(
        remaining,
        [first],
        "another VIP waited behind the slow cleanup"
    );
    assert!(finally_empty);
}

#[tokio::test(start_paused = true)]
async fn state_contention_during_started_reassert_does_not_remove_completed_vip() {
    assert_started_reassert_contention(ReassertResolution::Resume).await;
}

#[tokio::test(start_paused = true)]
async fn changed_assignment_during_started_reassert_removes_completed_vip() {
    assert_started_reassert_contention(ReassertResolution::Reassigned).await;
}

#[tokio::test(start_paused = true)]
async fn old_deadline_removes_started_reassert_while_state_remains_locked() {
    assert_started_reassert_contention(ReassertResolution::Expired).await;
}

#[tokio::test(start_paused = true)]
async fn command_timeout_is_polled_while_applied_state_remains_locked() {
    assert_started_reassert_contention(ReassertResolution::CommandTimedOut).await;
}

#[derive(Clone, Copy)]
enum ReassertResolution {
    Resume,
    Reassigned,
    Expired,
    CommandTimedOut,
}

async fn assert_started_reassert_contention(resolution: ReassertResolution) {
    use crate::vip::effects::{IP_COMMAND_TIMEOUT, bind_command_arguments};
    use std::os::unix::process::ExitStatusExt;
    use std::process::ExitStatus;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::process::Command;

    let (cfg, raft, network, state, mut controls) = release_tests::cluster().await;
    let table = Arc::new(cfg.sorted_vips());
    let (vip, iface) = table[0].clone();
    let local = LocalVip::new(false);
    let health = Arc::new(LocalHealth::new(true));
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
    verified_proof(&freshness, &controls, &network).await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let deadline = freshness.retain(&stamp).unwrap().deadline();
    let (announced, announcement) = tokio::sync::oneshot::channel();
    *local.next_announcement.lock().await = Some(announcement);
    announced.send(()).unwrap();
    local
        .force_next_bind_result((&iface, vip.addr, vip.prefix), Ok(ExitStatus::from_raw(0)))
        .await;
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state.clone(),
        local.clone(),
        table,
        health.clone(),
        freshness.clone(),
        1,
    ));
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    assert!(local.is_confirmed_bound(vip.addr).await);
    local.announcements.cancel(vip.addr).await;

    local
        .force_next_bind_result((&iface, vip.addr, vip.prefix), Ok(ExitStatus::from_raw(0)))
        .await;
    local
        .force_unbind_results(
            (&iface, vip.addr, vip.prefix),
            vec![Ok(ExitStatus::from_raw(0))],
        )
        .await;
    local
        .force_marker_delete_results(vip.addr, vec![Ok(ExitStatus::from_raw(0))])
        .await;
    let mut address_command = Command::new("ip");
    address_command.args(bind_command_arguments(vip.addr, vip.prefix, &iface));
    local
        .commands
        .clear(&address_command, IP_COMMAND_TIMEOUT, false);
    let (finish_address, address_result) = tokio::sync::oneshot::channel();
    let mut finish_address = Some(finish_address);
    let address_completed = Arc::new(AtomicBool::new(false));
    let completed = address_completed.clone();
    let command_timed_out = Arc::new(AtomicBool::new(false));
    let timed_out = command_timed_out.clone();
    let (command_clock, command_deadline) = tokio::sync::oneshot::channel();
    local
        .commands
        .expect_status_future(&address_command, IP_COMMAND_TIMEOUT, async move {
            if matches!(resolution, ReassertResolution::CommandTimedOut) {
                let deadline = tokio::time::Instant::now() + IP_COMMAND_TIMEOUT;
                command_clock.send(deadline).unwrap();
                match tokio::time::timeout_at(deadline, address_result).await {
                    Ok(result) => result.map_err(std::io::Error::other)?,
                    Err(_) => {
                        timed_out.store(true, Ordering::SeqCst);
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "scripted address replace exceeded its command budget",
                        ));
                    }
                }
            } else {
                address_result.await.map_err(std::io::Error::other)?;
            }
            completed.store(true, Ordering::SeqCst);
            Ok(ExitStatus::from_raw(0))
        });
    let arguments: Vec<_> = address_command
        .as_std()
        .get_args()
        .map(std::ffi::OsString::from)
        .collect();
    tokio::time::advance(RECONCILE_TICK).await;
    tokio::time::timeout(Duration::from_millis(10), async {
        loop {
            assert!(futures::poll!(reconcile.as_mut()).is_pending());
            if local
                .commands
                .calls()
                .iter()
                .filter(|call| call.arguments == arguments)
                .count()
                == 2
            {
                break;
            }
            tokio::time::advance(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the periodic address replace must start before state contention");
    assert!(!finish_address.as_ref().unwrap().is_closed());

    let mut held_state = Some(state.write().await);
    if !matches!(resolution, ReassertResolution::CommandTimedOut) {
        finish_address.take().unwrap().send(()).unwrap();
    }
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let retained = local.is_confirmed_bound(vip.addr).await;
    let delete_not_started = local.remaining_forced_unbind_results().await == 1;
    let still_authorized = freshness.acquire(true).is_some();
    let unchanged_deadline = freshness.retain(&stamp).unwrap().deadline() == deadline;
    match resolution {
        ReassertResolution::Resume => drop(held_state.take()),
        ReassertResolution::Reassigned => {
            let assignment = held_state
                .as_mut()
                .unwrap()
                .vip_assignments
                .get_mut(&vip.addr)
                .unwrap();
            assignment.holder = 2;
            assignment.generation += 1;
            drop(held_state.take());
        }
        ReassertResolution::Expired => {
            tokio::time::advance(deadline - tokio::time::Instant::now()).await;
        }
        ReassertResolution::CommandTimedOut => {
            tokio::time::sleep_until(command_deadline.await.unwrap()).await;
        }
    }
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let finally_bound = local.is_confirmed_bound(vip.addr).await;
    let command_completed = address_completed.load(Ordering::SeqCst);
    let timeout_observed = command_timed_out.load(Ordering::SeqCst);
    let replace_count = local
        .commands
        .calls()
        .iter()
        .filter(|call| call.arguments == arguments)
        .count();
    drop(reconcile);
    drop(held_state);
    drop(finish_address);
    local.unbind(&iface, vip.addr, vip.prefix).await.unwrap();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(still_authorized && unchanged_deadline);
    if matches!(resolution, ReassertResolution::CommandTimedOut) {
        assert!(
            timeout_observed,
            "state contention prevented polling the 250ms command timeout"
        );
        assert!(
            !health.is_healthy(),
            "command timeout must remain a bind fault"
        );
    } else {
        assert!(health.is_healthy());
    }
    assert!(
        retained && delete_not_started,
        "unchanged state lock contention removed a completed VIP during reassert"
    );
    assert_eq!(
        replace_count, 2,
        "resuming must not start another address replace"
    );
    let resumed = matches!(resolution, ReassertResolution::Resume);
    assert_eq!(
        finally_bound, resumed,
        "reassignment and old expiry require cleanup"
    );
    if resumed {
        assert!(
            command_completed,
            "unlock must allow the pending replace to finish"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn first_bind_remains_pending_until_final_applied_state_check() {
    assert_first_bind_state_checkpoint(false, None).await;
}

#[tokio::test(start_paused = true)]
async fn marker_completion_does_not_start_address_replace_while_state_is_locked() {
    assert_first_bind_state_checkpoint(true, None).await;
}

#[tokio::test(start_paused = true)]
async fn tracking_lock_wait_revalidates_first_bind_before_confirmation() {
    assert_first_bind_state_checkpoint(false, Some(false)).await;
}

#[tokio::test(start_paused = true)]
async fn tracking_lock_wait_rechecks_assignment_before_first_bind_completion() {
    assert_first_bind_state_checkpoint(false, Some(true)).await;
}

async fn assert_first_bind_state_checkpoint(pause_marker: bool, tracking_race: Option<bool>) {
    use crate::vip::effects::{
        IP_COMMAND_TIMEOUT, bind_command_arguments, marker_route_replace_arguments,
    };
    use std::os::unix::process::ExitStatusExt;
    use std::process::ExitStatus;
    use tokio::process::Command;

    let (cfg, raft, network, state, mut controls) = release_tests::cluster().await;
    let table = Arc::new(cfg.sorted_vips());
    let (vip, iface) = table[0].clone();
    let local = LocalVip::new(false);
    let health = Arc::new(LocalHealth::new(true));
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
    verified_proof(&freshness, &controls, &network).await;
    let (announced, announcement) = tokio::sync::oneshot::channel();
    *local.next_announcement.lock().await = Some(announcement);
    local
        .force_next_bind_result((&iface, vip.addr, vip.prefix), Ok(ExitStatus::from_raw(0)))
        .await;
    local
        .force_unbind_results(
            (&iface, vip.addr, vip.prefix),
            vec![Ok(ExitStatus::from_raw(0))],
        )
        .await;
    local
        .force_marker_delete_results(vip.addr, vec![Ok(ExitStatus::from_raw(0))])
        .await;
    let mut command = Command::new("ip");
    if pause_marker {
        command.args(marker_route_replace_arguments(
            vip.addr,
            local.ownership_marker.protocol(),
        ));
    } else {
        command.args(bind_command_arguments(vip.addr, vip.prefix, &iface));
    }
    let mut finish_command = Some(
        local
            .commands
            .pause_next(&command, IP_COMMAND_TIMEOUT, false),
    );
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state.clone(),
        local.clone(),
        table,
        health,
        freshness,
        1,
    ));
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    assert_eq!(
        local
            .commands
            .remaining(&command, IP_COMMAND_TIMEOUT, false),
        0
    );
    assert!(!finish_command.as_ref().unwrap().is_closed());
    let held_tracking = if tracking_race.is_some() {
        let tracking = local.pending_first_bind.write().await;
        finish_command.take().unwrap().send(()).unwrap();
        assert!(futures::poll!(reconcile.as_mut()).is_pending());
        assert!(tracking.contains(&vip.addr));
        Some(tracking)
    } else {
        None
    };
    let mut held_state = state.write().await;
    if tracking_race == Some(true) {
        let assignment = held_state.vip_assignments.get_mut(&vip.addr).unwrap();
        assignment.holder = 2;
        assignment.generation += 1;
    }
    drop(held_tracking);
    if let Some(finish_command) = finish_command.take() {
        finish_command.send(()).unwrap();
    }
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let tracked = local.bound.read().await.contains(&vip.addr);
    let pending = local.pending_first_bind.read().await.contains(&vip.addr);
    let confirmed_early = local.is_confirmed_bound(vip.addr).await;
    let announcement_deferred = local.next_announcement.lock().await.is_some();
    let delete_deferred = local.remaining_forced_unbind_results().await == 1;
    let address_arguments: Vec<_> = bind_command_arguments(vip.addr, vip.prefix, &iface)
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();
    let address_starts_while_locked = local
        .commands
        .calls()
        .iter()
        .filter(|call| call.arguments == address_arguments)
        .count();
    drop(held_state);
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let confirmed_after_unlock = local.is_confirmed_bound(vip.addr).await;
    let announcement_started = local.next_announcement.lock().await.is_none();
    drop(reconcile);
    drop(announced);
    local.unbind(&iface, vip.addr, vip.prefix).await.unwrap();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        tracked && pending && !confirmed_early,
        "physical command completion must not finalize a bind without state validation"
    );
    assert!(
        announcement_deferred && delete_deferred,
        "busy state must neither announce nor clean up an otherwise live pending bind"
    );
    assert_eq!(
        address_starts_while_locked,
        usize::from(!pause_marker),
        "marker completion must not start an address replace before state validation"
    );
    let may_complete = tracking_race != Some(true);
    assert_eq!(
        confirmed_after_unlock, may_complete,
        "unlock may finalize only the unchanged assignment"
    );
    assert_eq!(
        announcement_started, may_complete,
        "a reassigned first bind must not announce completion"
    );
}

/// The authorization the worker hands to a completed binding's MASTER hook.
async fn post_bind_effect(
    local: &Arc<LocalVip>,
    raft: KafRaft,
    state: Arc<RwLock<KafStorageState>>,
    freshness: &Arc<ConsensusFreshness>,
    health: &LocalHealth,
) -> Arc<crate::vip::witness::RetainedEffect> {
    let authorization =
        completed_effect_authorization(local, raft, state.clone(), freshness, health).await;
    retained_effect(&authorization, freshness, state)
}

/// Runs a MASTER hook for a completed binding of 192.0.2.99 that waits for `finish`, applies
/// `meanwhile` once it started, and reports whether the hook ran to its end.
async fn master_hook_completes(
    prepare: impl AsyncFnOnce(&Arc<RwLock<KafStorageState>>),
    meanwhile: impl AsyncFnOnce(
        &Arc<ConsensusFreshness>,
        &crate::raft::RaftControlTasks,
        &crate::raft::RaftNetworkImpl,
        &Arc<RwLock<KafStorageState>>,
    ),
) -> bool {
    use std::os::unix::process::ExitStatusExt;
    let (_, raft, network, state, mut controls) = release_tests::cluster().await;
    prepare(&state).await;
    let local = LocalVip::new(false);
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
    verified_proof(&freshness, &controls, &network).await;
    let health = LocalHealth::new(true);
    let effect = post_bind_effect(&local, raft.clone(), state.clone(), &freshness, &health).await;
    let (started, observed_start) = tokio::sync::oneshot::channel();
    let (finish, wait_finish) = tokio::sync::oneshot::channel::<()>();
    let (done, observed_done) = tokio::sync::oneshot::channel();
    *local.notifications.next_authorized_hook.lock().await = Some(Box::pin(async move {
        started.send(()).unwrap();
        let _ = wait_finish.await;
        let _ = done.send(());
    }));
    local
        .notifications
        .send_authorized("/nonexistent/post-bind-notify", "192.0.2.99", false, effect)
        .await;
    tokio::time::timeout(Duration::from_millis(100), observed_start)
        .await
        .expect("the MASTER hook of a held VIP must start")
        .unwrap();
    meanwhile(&freshness, &controls, &network, &state).await;
    let _ = finish.send(());
    local
        .notifications
        .shutdown(Duration::from_secs(1))
        .await
        .unwrap();
    let completed = observed_done.await.is_ok();
    let address = "192.0.2.99".parse().unwrap();
    local
        .force_unbind_results(
            ("lo", address, 32),
            vec![Ok(std::process::ExitStatus::from_raw(0))],
        )
        .await;
    local
        .force_marker_delete_results(address, vec![Ok(std::process::ExitStatus::from_raw(0))])
        .await;
    local.unbind("lo", address, 32).await.unwrap();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    completed
}

#[tokio::test(start_paused = true)]
async fn running_master_hook_outlives_the_acquisition_deadline_of_a_renewed_vip() {
    let completed = master_hook_completes(
        async |_| {},
        async |freshness, controls, network, _| {
            let lifetime = freshness.lifetime();
            for _ in 0..3 {
                tokio::time::advance(lifetime / 2).await;
                verified_proof(freshness, controls, network).await;
            }
        },
    )
    .await;
    assert!(
        completed,
        "a started MASTER hook for a VIP that stays held must not be cut off at the acquisition deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn predecessor_release_does_not_cancel_the_new_holders_master_hook() {
    let completed = master_hook_completes(
        async |state| {
            let mut current = state.write().await;
            current.latest_probe_tick = 10;
            current.node_probe_ticks.insert(2, 1);
            current.vip_assignments.insert(
                "192.0.2.99".parse().unwrap(),
                VipAssignment {
                    holder: 1,
                    generation: 2,
                    previous_holder: Some(2),
                    previous_holder_released: false,
                    activation_tick: 0,
                },
            );
        },
        async |_, _, _, state| {
            state
                .write()
                .await
                .vip_assignments
                .get_mut(&"192.0.2.99".parse().unwrap())
                .unwrap()
                .previous_holder_released = true;
            tokio::time::advance(crate::vip::RECONCILE_TICK * 2).await;
        },
    )
    .await;
    assert!(
        completed,
        "the predecessor's release must not cancel the MASTER hook of the generation it released to"
    );
}
