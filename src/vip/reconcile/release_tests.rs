use super::*;

#[tokio::test]
async fn failed_selective_cleanup_stays_tracked_until_global_fencing_retries_it() {
    use std::os::unix::process::ExitStatusExt;
    let _lock = LOCK.lock().await;
    let (cfg, raft, network, state, mut controls) = cluster().await;
    let addr = "2001:db8::99".parse().unwrap();
    let table = Arc::new(vec![(VipAddr::host(addr), "lo".into())]);
    state.write().await.vip_assignments.insert(
        addr,
        VipAssignment {
            holder: 1,
            generation: 1,
            previous_holder: None,
            previous_holder_released: false,
            activation_tick: 0,
        },
    );
    let local = LocalVip::new(false);
    local
        .force_next_bind_result(Ok(std::process::ExitStatus::from_raw(0)))
        .await;
    local
        .force_unbind_results(vec![
            Err(std::io::Error::other("delete unavailable")),
            Err(std::io::Error::other("delete unavailable")),
            Err(std::io::Error::other("delete unavailable")),
            Ok(std::process::ExitStatus::from_raw(0)),
        ])
        .await;
    local
        .force_marker_delete_results(vec![Ok(std::process::ExitStatus::from_raw(0))])
        .await;
    let freshness = Arc::new(ConsensusFreshness::new(std::time::Duration::from_secs(1)));
    freshness.record_success(tokio::time::Instant::now());
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state.clone(),
        local.clone(),
        table,
        Arc::new(AtomicBool::new(true)),
        freshness.clone(),
        1,
    ));
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        while !local.is_confirmed_bound(addr).await {
            assert!(futures::poll!(reconcile.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    state
        .write()
        .await
        .vip_assignments
        .get_mut(&addr)
        .unwrap()
        .holder = 2;
    freshness.record_success(tokio::time::Instant::now());
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    assert_eq!(local.remaining_forced_unbind_results().await, 1);
    assert!(
        local.bound_addrs().await.contains(&addr),
        "failed cleanup must stay tracked"
    );
    freshness.invalidate();
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let cleaned = local.bound_addrs().await.is_empty();
    drop(reconcile);
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        cleaned,
        "global fencing must retry a failed selective cleanup"
    );
}

#[tokio::test]
async fn global_fencing_still_cleans_every_vip_after_selective_revocation() {
    let _lock = LOCK.lock().await;
    for fence in ["invalidated", "coalesced", "unhealthy", "expired"] {
        let (cfg, raft, network, state, mut controls) = cluster().await;
        let mut table = cfg.sorted_vips();
        table.push((VipAddr::host("192.0.2.100".parse().unwrap()), "lo".into()));
        {
            let mut state = state.write().await;
            for (vip, _) in &table {
                state.vip_assignments.insert(
                    vip.addr,
                    VipAssignment {
                        holder: 1,
                        generation: 1,
                        previous_holder: None,
                        previous_holder_released: false,
                        activation_tick: 0,
                    },
                );
            }
        }
        let first = table[0].0.addr;
        let tail = table[1].0.addr;
        let local = LocalVip::new(true);
        let freshness = Arc::new(ConsensusFreshness::new(std::time::Duration::from_secs(1)));
        freshness.record_success(tokio::time::Instant::now());
        let healthy = Arc::new(AtomicBool::new(true));
        let mut reconcile = Box::pin(run_reconcile_loop(
            cfg,
            raft.clone(),
            state.clone(),
            local.clone(),
            Arc::new(table),
            healthy.clone(),
            freshness.clone(),
            1,
        ));
        tokio::time::timeout(std::time::Duration::from_millis(100), async {
            while local.bound_addrs().await.len() != 2 {
                assert!(futures::poll!(reconcile.as_mut()).is_pending());
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let block_rebind = local.bind_starts.lock().await;
        state
            .write()
            .await
            .vip_assignments
            .get_mut(&tail)
            .unwrap()
            .holder = 2;
        freshness.record_success(tokio::time::Instant::now());
        assert!(futures::poll!(reconcile.as_mut()).is_pending());
        assert_eq!(local.bound_addrs().await, [first]);

        match fence {
            "expired" => {
                tokio::time::pause();
                tokio::time::advance(std::time::Duration::from_secs(1)).await;
            }
            "unhealthy" => {
                healthy.store(false, Ordering::SeqCst);
                freshness.record_success(tokio::time::Instant::now());
            }
            _ => {
                freshness.invalidate();
                if fence == "coalesced" {
                    freshness.record_success(tokio::time::Instant::now());
                }
            }
        }
        assert!(futures::poll!(reconcile.as_mut()).is_pending());
        let fenced = local.bound_addrs().await.is_empty();
        if fence == "expired" {
            tokio::time::resume();
        }
        drop(reconcile);
        drop(block_rebind);
        controls.shutdown().await.unwrap();
        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
        assert!(fenced, "{fence} must fence every retained VIP");
    }
}

#[tokio::test]
async fn new_revocations_during_cleanup_are_not_lost() {
    let _lock = LOCK.lock().await;
    let (cfg, raft, network, state, mut controls) = cluster().await;
    let mut table = cfg.sorted_vips();
    for last in [100, 101] {
        table.push((
            VipAddr::host(format!("192.0.2.{last}").parse().unwrap()),
            "lo".into(),
        ));
    }
    {
        let mut state = state.write().await;
        for (vip, _) in &table {
            state.vip_assignments.insert(
                vip.addr,
                VipAssignment {
                    holder: 1,
                    generation: 1,
                    previous_holder: None,
                    previous_holder_released: false,
                    activation_tick: 0,
                },
            );
        }
    }
    let first = table[0].0.addr;
    let middle = table[1].0.addr;
    let tail = table[2].0.addr;
    let local = LocalVip::new(true);
    let freshness = Arc::new(ConsensusFreshness::new(std::time::Duration::from_secs(1)));
    freshness.record_success(tokio::time::Instant::now());
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state.clone(),
        local.clone(),
        Arc::new(table),
        Arc::new(AtomicBool::new(true)),
        freshness.clone(),
        1,
    ));
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        while local.bound_addrs().await.len() != 3 {
            assert!(futures::poll!(reconcile.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let block_rebind = local.bind_starts.lock().await;
    let block_cleanup = local.bound.read().await;
    state
        .write()
        .await
        .vip_assignments
        .get_mut(&tail)
        .unwrap()
        .holder = 2;
    freshness.record_success(tokio::time::Instant::now());
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    state
        .write()
        .await
        .vip_assignments
        .get_mut(&middle)
        .unwrap()
        .holder = 2;
    freshness.record_success(tokio::time::Instant::now());
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    drop(block_cleanup);
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let remaining = local.bound_addrs().await;
    drop(reconcile);
    drop(block_rebind);
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert_eq!(
        remaining,
        [first],
        "both revoked VIPs must be cleaned without dropping the unchanged VIP"
    );
}

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn failed_ambiguous_bind_remains_fenced_after_advancing_to_another_vip() {
    use std::os::unix::process::ExitStatusExt;
    let _lock = LOCK.lock().await;
    let (cfg, raft, network, state, mut controls) = cluster().await;
    let mut table = cfg.sorted_vips();
    table.push((
        VipAddr {
            addr: "192.0.2.100".parse().unwrap(),
            prefix: 32,
        },
        "lo".into(),
    ));
    let first = table[0].0.addr;
    {
        let mut state = state.write().await;
        for (vip, _) in &table {
            state.vip_assignments.insert(
                vip.addr,
                VipAssignment {
                    holder: 1,
                    generation: 1,
                    previous_holder: None,
                    previous_holder_released: false,
                    activation_tick: 0,
                },
            );
        }
    }
    let local = LocalVip::new(false);
    local
        .force_next_bind_result(Err(std::io::Error::other(
            "address result lost after syscall",
        )))
        .await;
    local
        .force_next_bind_presence_result(Err(std::io::Error::other(
            "absence cannot be established",
        )))
        .await;
    local
        .force_unbind_results(vec![Ok(std::process::ExitStatus::from_raw(0))])
        .await;
    local
        .force_marker_delete_results(vec![Ok(std::process::ExitStatus::from_raw(0))])
        .await;
    // Pause the first address result after bound/pending tracking and marker installation.
    let first_result = local.next_bind_result.lock().await;
    let freshness = Arc::new(ConsensusFreshness::new(std::time::Duration::from_secs(1)));
    freshness.record_success(tokio::time::Instant::now());
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state.clone(),
        local.clone(),
        Arc::new(table),
        Arc::new(AtomicBool::new(true)),
        freshness.clone(),
        1,
    ));
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        loop {
            assert!(futures::poll!(reconcile.as_mut()).is_pending());
            if local.bound_addrs().await.contains(&first) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The next VIP can begin its captured operation, but cannot finish while this gate is held.
    let next_operation = local.bind_starts.lock().await;
    drop(first_result);
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    assert!(
        local.next_bind_result.lock().await.is_none(),
        "the first failed address result was consumed"
    );
    assert!(
        local.pending_first_bind.read().await.contains(&first),
        "failed absence proof must preserve pending ownership"
    );
    assert!(local.bound_addrs().await.contains(&first));
    state
        .write()
        .await
        .vip_assignments
        .get_mut(&first)
        .unwrap()
        .holder = 2;
    freshness.record_success(tokio::time::Instant::now());
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    let cleaned = !local.bound_addrs().await.contains(&first)
        && !local.pending_first_bind.read().await.contains(&first);
    drop(reconcile);
    drop(next_operation);
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        cleaned,
        "an ambiguous failed bind must remain in the proof validator after advancing to another VIP"
    );
}

#[tokio::test]
async fn revoked_retained_tail_is_cleaned_during_an_unchanged_slow_prefix_effect() {
    let _lock = LOCK.lock().await;
    let (cfg, raft, network, state, mut controls) = cluster().await;
    let mut table = cfg.sorted_vips();
    for last in [100, 101] {
        table.push((
            VipAddr {
                addr: format!("192.0.2.{last}").parse().unwrap(),
                prefix: 32,
            },
            "lo".into(),
        ));
    }
    let first = table[0].0.addr;
    let tail = table[2].0.addr;
    {
        let mut state = state.write().await;
        for (vip, _) in &table {
            state.vip_assignments.insert(
                vip.addr,
                VipAssignment {
                    holder: 1,
                    generation: 1,
                    previous_holder: None,
                    previous_holder_released: false,
                    activation_tick: 0,
                },
            );
        }
    }
    let local = LocalVip::new(true);
    let freshness = Arc::new(ConsensusFreshness::new(std::time::Duration::from_secs(1)));
    freshness.record_success(tokio::time::Instant::now());
    let task = tokio::spawn(run_reconcile_loop(
        cfg,
        raft.clone(),
        state.clone(),
        local.clone(),
        Arc::new(table),
        Arc::new(AtomicBool::new(true)),
        freshness.clone(),
        1,
    ));
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        while local.bind_completions.lock().await.len() < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    local.bind_command_delay_ms.store(40, Ordering::SeqCst);
    tokio::time::timeout(std::time::Duration::from_millis(300), async {
        loop {
            let started = local
                .bind_starts
                .lock()
                .await
                .get(&first)
                .copied()
                .unwrap_or(0);
            let completed = local
                .bind_completions
                .lock()
                .await
                .get(&first)
                .copied()
                .unwrap_or(0);
            if started > completed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    state
        .write()
        .await
        .vip_assignments
        .get_mut(&tail)
        .unwrap()
        .holder = 2;
    freshness.record_success(tokio::time::Instant::now());
    let tail_cleaned = tokio::time::timeout(std::time::Duration::from_millis(100), async {
        while local.bound_addrs().await.contains(&tail) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let unchanged_retained = local.bound_addrs().await.contains(&first);
    task.abort();
    let _ = task.await;
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        tail_cleaned,
        "retained tail revocation must interrupt unchanged prefix work and clean before accepting renewal"
    );
    assert!(
        unchanged_retained,
        "revoking the tail must not withdraw an unchanged VIP during its slow reassertion"
    );
}

#[tokio::test]
async fn revoked_in_flight_bind_is_cleaned_before_advancing_to_another_vip() {
    let _lock = LOCK.lock().await;
    let (cfg, raft, network, state, mut controls) = cluster().await;
    let mut table = cfg.sorted_vips();
    table.push((
        VipAddr {
            addr: "192.0.2.100".parse().unwrap(),
            prefix: 32,
        },
        "lo".into(),
    ));
    let first = table[0].0.addr;
    {
        let mut state = state.write().await;
        state.latest_probe_tick = 10;
        for (vip, _) in &table {
            state.vip_assignments.insert(
                vip.addr,
                VipAssignment {
                    holder: 1,
                    generation: 1,
                    previous_holder: None,
                    previous_holder_released: false,
                    activation_tick: 0,
                },
            );
        }
    }
    let local = LocalVip::new(true);
    local.bind_command_delay_ms.store(40, Ordering::SeqCst);
    let freshness = Arc::new(ConsensusFreshness::new(std::time::Duration::from_millis(
        500,
    )));
    freshness.record_success(tokio::time::Instant::now());
    let task = tokio::spawn(run_reconcile_loop(
        cfg,
        raft.clone(),
        state.clone(),
        local.clone(),
        Arc::new(table),
        Arc::new(AtomicBool::new(true)),
        freshness.clone(),
        1,
    ));
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        while !local.bound_addrs().await.contains(&first) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    state
        .write()
        .await
        .vip_assignments
        .get_mut(&first)
        .unwrap()
        .holder = 2;
    freshness.record_success(tokio::time::Instant::now());
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let first_cleaned = !local.bound_addrs().await.contains(&first);
    let completed = local
        .bind_completions
        .lock()
        .await
        .get(&first)
        .copied()
        .unwrap_or(0);
    task.abort();
    let _ = task.await;
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        first_cleaned,
        "a canceled ambiguous bind must be cleaned before rotating to another VIP"
    );
    assert_eq!(
        completed, 0,
        "revoked work must not complete across the renewal"
    );
}

#[tokio::test]
async fn stable_renewals_preserve_slow_effects_and_reach_every_vip() {
    let _lock = LOCK.lock().await;
    for delay_ms in [15, 40] {
        let (cfg, raft, network, state, mut controls) = cluster().await;
        let mut cfg = (*cfg).clone();
        cfg.vips = (1..=3)
            .map(|last| {
                let mut vip = cfg.vips[0].clone();
                vip.address = VipAddr {
                    addr: format!("192.0.2.{last}").parse().unwrap(),
                    prefix: 32,
                };
                vip
            })
            .collect();
        let cfg = Arc::new(cfg);
        let table = Arc::new(cfg.sorted_vips());
        {
            let mut state = state.write().await;
            state.latest_probe_tick = 10;
            for (vip, _) in table.iter() {
                state.vip_assignments.insert(
                    vip.addr,
                    VipAssignment {
                        holder: 1,
                        generation: 1,
                        previous_holder: None,
                        previous_holder_released: false,
                        activation_tick: 0,
                    },
                );
            }
        }
        let local = LocalVip::new(true);
        local
            .bind_command_delay_ms
            .store(delay_ms, Ordering::SeqCst);
        let freshness = Arc::new(ConsensusFreshness::new(std::time::Duration::from_millis(
            500,
        )));
        freshness.record_success(tokio::time::Instant::now());
        let task = tokio::spawn(run_reconcile_loop(
            cfg,
            raft.clone(),
            state.clone(),
            local.clone(),
            table.clone(),
            Arc::new(AtomicBool::new(true)),
            freshness.clone(),
            1,
        ));
        for _ in 0..12 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            freshness.record_success(tokio::time::Instant::now());
        }
        let completions = local.bind_completions.lock().await.clone();
        let all_reasserted = table
            .iter()
            .all(|(vip, _)| completions.get(&vip.addr).copied().unwrap_or(0) >= 2);
        let last = table.last().unwrap().0.addr;
        state
            .write()
            .await
            .vip_assignments
            .get_mut(&last)
            .unwrap()
            .holder = 2;
        for _ in 0..8 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            freshness.record_success(tokio::time::Instant::now());
        }
        let tail_released = !local.bound_addrs().await.contains(&last);
        task.abort();
        let _ = task.await;
        controls.shutdown().await.unwrap();
        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
        assert!(
            all_reasserted,
            "two {delay_ms}ms effects per VIP starved the tail or an individual effect: {completions:?}"
        );
        assert!(
            tail_released,
            "unchanged prefix binds starved the tail release"
        );
    }
}

fn cluster_config() -> Arc<Config> {
    // This single-node fixture has no remote peers or submit server, so no caller needs its port.
    let cfg: Config = serde_yaml::from_str(
        "node_id: 1\nraft_listen: '127.0.0.1:0'\nclient_submit_listen: '127.0.0.1:0'\n\
         peers:\n  - id: 1\n    raft_address: '127.0.0.1:0'\n    client_submit_address: '127.0.0.1:0'\n\
         vips:\n  - address: 192.0.2.99\n    interface: lo\n\
         health:\n  command: [/bin/true]\n  interval_ms: 1000\n  timeout_ms: 500\n\
         dry_run: true\n"
    ).unwrap();
    Arc::new(cfg)
}

async fn cluster() -> (
    Arc<Config>,
    KafRaft,
    Arc<crate::raft::RaftNetworkImpl>,
    Arc<RwLock<KafStorageState>>,
    crate::raft::RaftControlTasks,
) {
    cluster_from_config(cluster_config()).await
}

async fn cluster_from_config(
    cfg: Arc<Config>,
) -> (
    Arc<Config>,
    KafRaft,
    Arc<crate::raft::RaftNetworkImpl>,
    Arc<RwLock<KafStorageState>>,
    crate::raft::RaftControlTasks,
) {
    let table = Arc::new(cfg.sorted_vips());
    let (raft, network, state, _, _, controls) =
        crate::raft::start_raft(cfg.clone(), table).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while raft.current_leader().await != Some(1) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    (cfg, raft, network, state, controls)
}

#[tokio::test]
async fn cluster_starts_with_a_competing_listener() {
    let cfg = cluster_config();
    let competing_listener = std::net::TcpListener::bind(&cfg.raft_listen).unwrap();
    let (_, raft, network, _, mut controls) = cluster_from_config(cfg).await;
    raft.client_write(KafRequest::HealthUpdate {
        node_id: 1,
        healthy: true,
    })
    .await
    .unwrap();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    drop(competing_listener);
}

#[tokio::test]
async fn successful_release_does_not_create_a_health_proof() {
    let _lock = LOCK.lock().await;
    let (cfg, raft, network, _, mut controls) = cluster().await;
    let freshness = Arc::new(ConsensusFreshness::for_probe_cadence(1_000, 1));
    let vip = "192.0.2.99".parse().unwrap();
    let assignment = VipAssignment {
        holder: 2,
        generation: 2,
        previous_holder: Some(1),
        previous_holder_released: false,
        activation_tick: 2,
    };
    let mut released = HashMap::new();
    maybe_publish_release(&cfg, &raft, &freshness, 1, vip, &assignment, &mut released).await;
    let created_proof = freshness.is_fresh();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert_eq!(
        released.get(&vip),
        Some(&2),
        "the release must have committed"
    );
    assert!(
        !created_proof,
        "a release acknowledgement must not renew health freshness"
    );
}

#[tokio::test]
async fn same_holder_recovery_cleans_up_acknowledges_and_rebinds() {
    let _lock = LOCK.lock().await;
    let (cfg, raft, network, state, mut controls) = cluster().await;
    for healthy in [true, false] {
        raft.client_write(KafRequest::HealthUpdate {
            node_id: 1,
            healthy,
        })
        .await
        .unwrap();
    }
    for _ in 0..cfg.effective_failback_delay_ticks() + 2 {
        raft.client_write(KafRequest::HealthUpdate {
            node_id: 1,
            healthy: true,
        })
        .await
        .unwrap();
    }
    let vip = cfg.sorted_vips()[0].0.addr;
    {
        let state = state.read().await;
        let assignment = &state.vip_assignments[&vip];
        assert_eq!(assignment.previous_holder, Some(1));
        assert!(!assignment.previous_holder_released);
    }
    let local = LocalVip::new(true);
    local.bind("lo", vip, 32).await.unwrap();
    let freshness = Arc::new(ConsensusFreshness::for_probe_cadence(1_000, 1));
    freshness.record_success(tokio::time::Instant::now());
    let task = tokio::spawn(run_reconcile_loop(
        cfg.clone(),
        raft.clone(),
        state.clone(),
        local.clone(),
        Arc::new(cfg.sorted_vips()),
        Arc::new(AtomicBool::new(true)),
        freshness,
        1,
    ));
    let recovered = tokio::time::timeout(std::time::Duration::from_millis(500), async {
        loop {
            if state.read().await.vip_assignments[&vip].previous_holder_released
                && local.bound_addrs().await.contains(&vip)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    task.abort();
    let _ = task.await;
    local
        .unbind_all(&cfg.sorted_vips(), None, true, VipState::Backup)
        .await
        .unwrap();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        recovered.is_ok(),
        "the same holder must clear its own handoff fence"
    );
}

#[tokio::test]
async fn missing_health_proof_still_withdraws_and_commits_release_acknowledgements() {
    let _lock = LOCK.lock().await;
    let (cfg, raft, network, state, mut controls) = cluster().await;
    let vip = "192.0.2.99".parse().unwrap();
    state.write().await.vip_assignments.insert(
        vip,
        VipAssignment {
            holder: 2,
            generation: 2,
            previous_holder: Some(1),
            previous_holder_released: false,
            activation_tick: 2,
        },
    );
    let local = LocalVip::new(true);
    local.bind("lo", vip, 32).await.unwrap();
    let freshness = Arc::new(ConsensusFreshness::for_probe_cadence(1_000, 1));
    let task = tokio::spawn(run_reconcile_loop(
        cfg.clone(),
        raft.clone(),
        state.clone(),
        local.clone(),
        Arc::new(cfg.sorted_vips()),
        Arc::new(AtomicBool::new(false)),
        freshness.clone(),
        1,
    ));
    let maintained = tokio::time::timeout(std::time::Duration::from_millis(300), async {
        loop {
            if local.bound_addrs().await.is_empty()
                && state.read().await.vip_assignments[&vip].previous_holder_released
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    task.abort();
    let _ = task.await;
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        maintained,
        "fenced upgraded followers must release VIPs and acknowledge handoff to legacy owners"
    );
    assert!(!freshness.is_fresh());
}

#[tokio::test]
async fn repeated_proof_renewals_do_not_restart_the_pending_takeover_delay() {
    let _lock = LOCK.lock().await;
    let (cfg, raft, network, state, mut controls) = cluster().await;
    let vip = "192.0.2.99".parse().unwrap();
    {
        let mut state = state.write().await;
        state.node_health.insert(1, true);
        state.node_probe_ticks.insert(1, 10);
        state.latest_probe_tick = 10;
        state.vip_generation.insert(vip, 2);
        state.vip_assignments.insert(
            vip,
            VipAssignment {
                holder: 1,
                generation: 2,
                previous_holder: Some(2),
                previous_holder_released: false,
                activation_tick: 10,
            },
        );
    }
    let local = LocalVip::new(true);
    let freshness = Arc::new(ConsensusFreshness::new(std::time::Duration::from_millis(
        300,
    )));
    freshness.record_success(tokio::time::Instant::now());
    let task = tokio::spawn(run_reconcile_loop(
        cfg.clone(),
        raft.clone(),
        state,
        local.clone(),
        Arc::new(cfg.sorted_vips()),
        Arc::new(AtomicBool::new(true)),
        freshness.clone(),
        1,
    ));
    let mut acquired = false;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        freshness.record_success(tokio::time::Instant::now());
        acquired |= local.bound_addrs().await.contains(&vip);
    }
    task.abort();
    let _ = task.await;
    local
        .unbind_all(&cfg.sorted_vips(), None, true, VipState::Backup)
        .await
        .unwrap();
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        acquired,
        "renewals more frequent than takeover delay must not starve acquisition"
    );
}
