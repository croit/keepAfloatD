use super::*;
use openraft::async_runtime::WatchReceiver;
use std::sync::atomic::Ordering;

#[tokio::test(start_paused = true)]
async fn failed_bind_closes_the_local_health_gate() {
    for failure in ["exit", "spawn", "timeout", "marker", "reassert"] {
        assert_bind_fault(failure).await;
    }
}

async fn assert_bind_fault(failure: &str) {
    use std::os::unix::process::ExitStatusExt;
    let _lock = LOCK.lock().await;
    let (cfg, raft, network, state, mut controls) = cluster().await;
    let table = Arc::new(cfg.sorted_vips());
    let addr = table[0].0.addr;
    let (bound_vip, bound_iface) = table[0].clone();
    publish_probe(&controls, &network, true).await;
    let local = LocalVip::new(false);
    if failure == "reassert" {
        let (completed, announcement) = tokio::sync::oneshot::channel();
        *local.next_announcement.lock().await = Some(announcement);
        completed.send(()).unwrap();
        local
            .force_next_bind_result(
                (&table[0].1, addr, table[0].0.prefix),
                Ok(std::process::ExitStatus::from_raw(0)),
            )
            .await;
        local
            .bind(&table[0].1, addr, table[0].0.prefix)
            .await
            .unwrap();
        local
            .force_unbind_results(
                (&table[0].1, addr, table[0].0.prefix),
                vec![Ok(std::process::ExitStatus::from_raw(0))],
            )
            .await;
    }
    match failure {
        "marker" => {
            local
                .force_bind_results(
                    (&table[0].1, addr, table[0].0.prefix),
                    vec![Ok(std::process::ExitStatus::from_raw(1 << 8))],
                )
                .await
        }
        "spawn" | "timeout" => {
            let kind = if failure == "spawn" {
                std::io::ErrorKind::NotFound
            } else {
                std::io::ErrorKind::TimedOut
            };
            local
                .force_next_bind_result(
                    (&table[0].1, addr, table[0].0.prefix),
                    Err(std::io::Error::new(kind, "bind unavailable")),
                )
                .await;
        }
        _ => {
            local
                .force_next_bind_result(
                    (&table[0].1, addr, table[0].0.prefix),
                    Ok(std::process::ExitStatus::from_raw(1 << 8)),
                )
                .await
        }
    }
    let healthy = Arc::new(LocalHealth::new(true));
    let freshness = Arc::new(ConsensusFreshness::new(std::time::Duration::from_secs(1)));
    freshness.record_success(tokio::time::Instant::now());
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state,
        local.clone(),
        table,
        healthy.clone(),
        freshness,
        1,
    ));
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        loop {
            assert!(futures::poll!(reconcile.as_mut()).is_pending());
            if local.bind_starts.lock().await.contains_key(&addr)
                && local.remaining_bind_results((&bound_iface, addr, bound_vip.prefix)) == 0
            {
                break;
            }
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let faulted = !healthy.is_healthy();
    drop(reconcile);
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(
        faulted,
        "{failure}: failed VIP binding must make the holder unhealthy"
    );
}

#[tokio::test(start_paused = true)]
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
        .force_next_bind_result(("lo", addr, 128), Ok(std::process::ExitStatus::from_raw(0)))
        .await;
    local
        .force_unbind_results(
            ("lo", addr, 128),
            vec![
                Err(std::io::Error::other("delete unavailable")),
                Err(std::io::Error::other("delete unavailable")),
                Err(std::io::Error::other("delete unavailable")),
                Ok(std::process::ExitStatus::from_raw(0)),
            ],
        )
        .await;
    local
        .force_marker_delete_results(addr, vec![Ok(std::process::ExitStatus::from_raw(0))])
        .await;
    let freshness = Arc::new(ConsensusFreshness::new(std::time::Duration::from_secs(1)));
    freshness.record_success(tokio::time::Instant::now());
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state.clone(),
        local.clone(),
        table,
        Arc::new(LocalHealth::new(true)),
        freshness.clone(),
        1,
    ));
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        while !local.is_confirmed_bound(addr).await {
            assert!(futures::poll!(reconcile.as_mut()).is_pending());
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
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

#[tokio::test(start_paused = true)]
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
        let healthy = Arc::new(LocalHealth::new(true));
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
                tokio::time::advance(std::time::Duration::from_millis(1)).await;
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
                tokio::time::advance(std::time::Duration::from_secs(1)).await;
            }
            "unhealthy" => {
                healthy.observe_probe(false);
                tokio::time::advance(std::time::Duration::from_nanos(1)).await;
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
        drop(reconcile);
        drop(block_rebind);
        controls.shutdown().await.unwrap();
        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
        assert!(fenced, "{fence} must fence every retained VIP");
    }
}

#[tokio::test(start_paused = true)]
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
        Arc::new(LocalHealth::new(true)),
        freshness.clone(),
        1,
    ));
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        while local.bound_addrs().await.len() != 3 {
            assert!(futures::poll!(reconcile.as_mut()).is_pending());
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
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
    tokio::time::advance(std::time::Duration::from_nanos(1)).await;
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

#[tokio::test(start_paused = true)]
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
    let (bound_vip, bound_iface) = table[0].clone();
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
        .force_next_bind_result(
            (&table[0].1, first, table[0].0.prefix),
            Err(std::io::Error::other("address result lost after syscall")),
        )
        .await;
    local
        .force_next_bind_presence_result(
            first,
            Err(std::io::Error::other("absence cannot be established")),
        )
        .await;
    local
        .force_unbind_results(
            (&table[0].1, first, table[0].0.prefix),
            vec![Ok(std::process::ExitStatus::from_raw(0))],
        )
        .await;
    local
        .force_marker_delete_results(first, vec![Ok(std::process::ExitStatus::from_raw(0))])
        .await;
    // Pause the first address result after bound/pending tracking and marker installation.
    let first_result = local.pause_bind_result((&table[0].1, first, table[0].0.prefix));
    let freshness = Arc::new(ConsensusFreshness::new(std::time::Duration::from_secs(1)));
    freshness.record_success(tokio::time::Instant::now());
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg,
        raft.clone(),
        state.clone(),
        local.clone(),
        Arc::new(table),
        Arc::new(LocalHealth::new(true)),
        freshness.clone(),
        1,
    ));
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        loop {
            assert!(futures::poll!(reconcile.as_mut()).is_pending());
            if local.bound_addrs().await.contains(&first) {
                break;
            }
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    // The next VIP can begin its captured operation, but cannot finish while this gate is held.
    let next_operation = local.bind_starts.lock().await;
    drop(first_result);
    assert!(futures::poll!(reconcile.as_mut()).is_pending());
    assert!(
        local.remaining_bind_results((&bound_iface, first, bound_vip.prefix)) == 0,
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

#[tokio::test(start_paused = true)]
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
        Arc::new(LocalHealth::new(true)),
        freshness.clone(),
        1,
    ));
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        while local.bind_completions.lock().await.len() < 3 {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
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
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
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
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
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

#[tokio::test(start_paused = true)]
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
        Arc::new(LocalHealth::new(true)),
        freshness.clone(),
        1,
    ));
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        while !local.bound_addrs().await.contains(&first) {
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
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

#[tokio::test(start_paused = true)]
async fn stable_renewals_preserve_slow_effects_and_reach_every_vip() {
    let _lock = LOCK.lock().await;
    for delay_ms in [15, 40] {
        let mut cfg = (*cluster_config()).clone();
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
        let (cfg, raft, network, state, mut controls) = cluster_from_config(Arc::new(cfg)).await;
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
            Arc::new(LocalHealth::new(true)),
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
         cluster_secret: release-fixture-key-0123456789abcdef\n\
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

async fn publish_probe(
    controls: &crate::raft::RaftControlTasks,
    network: &crate::raft::RaftNetworkImpl,
    healthy: bool,
) -> openraft::alias::LogIdOf<crate::raft::types::TypeConfig> {
    let runtime = controls.runtime();
    let deadline = runtime.current().unwrap().check().unwrap();
    // Renewal must start after the previous challenge even with a paused clock.
    tokio::time::advance(std::time::Duration::from_nanos(1)).await;
    let mut probe = Box::pin(runtime.submit_health(network, healthy));
    loop {
        if let std::task::Poll::Ready(result) = futures::poll!(probe.as_mut()) {
            return result
                .expect("real probe progress must commit and renew admission")
                .1;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "probe outlived admission"
        );
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
    }
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
    let timing = crate::runtime_permission::LeaseTiming::for_config(&cfg, table.len()).unwrap();
    let (raft, network, state, _, _, controls) =
        crate::raft::start_raft(cfg.clone(), table, Default::default())
            .await
            .unwrap();
    let local_replica = raft.metrics().borrow_watched().id;
    let runtime = controls.runtime();
    assert_eq!(runtime.local_replica(), local_replica);
    assert!(runtime.current().is_none());
    assert!(!raft.is_initialized().await.unwrap());
    tokio::time::advance(timing.restart_quarantine()).await;
    raft.wait(Some(std::time::Duration::from_secs(2)))
        .current_leader(local_replica, "single-node leader after quarantine")
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut metrics = raft.metrics();
        loop {
            let _ = metrics.borrow_watched();
            if state.read().await.genesis.is_some() {
                break;
            }
            metrics.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let activation_limit = timing
        .vip_activation_deadline(tokio::time::Instant::now())
        .unwrap();
    while !runtime.vip_activation_ready().await {
        assert!(tokio::time::Instant::now() < activation_limit);
        publish_probe(&controls, &network, true).await;
        tokio::time::advance(timing.consumer_use() / 4).await;
    }
    let applied = publish_probe(&controls, &network, true).await;
    {
        let session = runtime.current().expect("real admission remains active");
        let state = state.read().await;
        assert_eq!(state.genesis.as_ref(), Some(&session.context().genesis));
        assert_eq!(
            state
                .last_membership
                .membership()
                .voter_ids()
                .collect::<Vec<_>>(),
            [local_replica]
        );
        assert!(state.last_applied_log.unwrap().index >= applied.index);
        let progress = &state.applied_progress[&cfg.node_id];
        assert_eq!(progress.log_id, applied);
        assert_eq!(progress.request.replica, local_replica);
        assert_eq!(progress.request.healthy, Some(true));
        assert_eq!(state.node_health.get(&cfg.node_id), Some(&true));
    }
    (cfg, raft, network, state, controls)
}

#[tokio::test(start_paused = true)]
async fn cluster_starts_with_a_competing_listener() {
    let cfg = cluster_config();
    let competing_listener = std::net::TcpListener::bind(&cfg.raft_listen).unwrap();
    let (_, raft, network, _, mut controls) = cluster_from_config(cfg).await;
    publish_probe(&controls, &network, true).await;
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    drop(competing_listener);
}

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
async fn same_holder_recovery_cleans_up_acknowledges_and_rebinds() {
    let _lock = LOCK.lock().await;
    let (cfg, raft, network, state, mut controls) = cluster().await;
    for healthy in [true, false] {
        publish_probe(&controls, &network, healthy).await;
    }
    for _ in 0..cfg.effective_failback_delay_ticks() + 2 {
        publish_probe(&controls, &network, true).await;
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
        Arc::new(LocalHealth::new(true)),
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
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
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

#[tokio::test(start_paused = true)]
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
        Arc::new(LocalHealth::new(false)),
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

#[tokio::test(start_paused = true)]
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
    let deadline = tokio::time::Instant::now()
        + takeover_lifetime(freshness.lifetime(), cfg.sorted_vips().len())
        + RECONCILE_TICK * 2;
    let mut reconcile = Box::pin(run_reconcile_loop(
        cfg.clone(),
        raft.clone(),
        state,
        local.clone(),
        Arc::new(cfg.sorted_vips()),
        Arc::new(LocalHealth::new(true)),
        freshness.clone(),
        1,
    ));
    let mut acquired = false;
    while tokio::time::Instant::now() < deadline {
        freshness.record_success(tokio::time::Instant::now());
        assert!(futures::poll!(reconcile.as_mut()).is_pending());
        acquired |= local.bound_addrs().await.contains(&vip);
        tokio::time::advance(std::time::Duration::from_millis(80)).await;
    }
    drop(reconcile);
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
