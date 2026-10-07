use super::*;
use futures::FutureExt;
use openraft::async_runtime::WatchReceiver;

async fn all_vips_bound(locals: &[Arc<LocalVip>], expected: &[IpAddr]) -> bool {
    let mut bound = Vec::new();
    for local in locals {
        bound.extend(local.bound_addrs().await);
    }
    bound.sort_unstable();
    bound == expected
}

async fn eventually(mut predicate: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        loop {
            tick.tick().await;
            if predicate().await {
                return;
            }
        }
    })
    .await
    .expect("VIP handoff waited for the sixty-second stale window");
}

#[tokio::test(start_paused = true)]
async fn clean_stop_hands_off_before_the_holder_becomes_stale() {
    let mut cluster = ClusterFixture::bind(3).await;
    let vips: Vec<_> = (1..=3)
        .map(|suffix| VipConfig {
            address: VipAddr::host(ip4(192, 0, 2, suffix)),
            interface: "lo".into(),
            vlan: None,
        })
        .collect();
    let expected: Vec<_> = vips.iter().map(|vip| vip.address.addr).collect();
    let mut locals = Vec::new();
    let mut stops = Vec::new();
    let mut handles = Vec::new();
    let mut budget = Duration::ZERO;
    for index in 0..3 {
        let mut cfg = (*cluster.config(index, &vips)).clone();
        cfg.health.interval_ms = 1_000;
        cfg.health.stale_secs = Some(60);
        cfg.failover_delay_secs = 30;
        budget = startup_budget(&cfg);
        let cfg = Arc::new(cfg);
        let local = LocalVip::new(true);
        let (stop, stopped) = oneshot::channel();
        handles.push(tokio::spawn(run_with_probe(
            cfg.clone(),
            Arc::new(cfg.sorted_vips()),
            local.clone(),
            async {
                let _ = stopped.await;
            },
            cluster.take_raft(index),
            cluster.take_submit(index),
            ControlledProbe::new(true),
        )));
        locals.push(local);
        stops.push(stop);
    }
    let outcome = std::panic::AssertUnwindSafe(async {
        assert!(
            advance_until(budget, async || {
                all_vips_bound(&locals, &expected).await && locals[0].bound_addrs().await.len() == 1
            })
            .await,
            "cluster did not reach the initial placement"
        );
        tokio::time::resume();
        stops.remove(0).send(()).unwrap();
        join_daemon(handles.remove(0)).await.unwrap();
        assert!(locals[0].bound_addrs().await.is_empty());
        eventually(async || all_vips_bound(&locals[1..], &expected).await).await;
    })
    .catch_unwind()
    .await;
    for stop in stops {
        let _ = stop.send(());
    }
    for handle in handles {
        join_daemon(handle).await.unwrap();
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(start_paused = true)]
async fn handoff_rejects_busy_admission_without_waiting_for_the_state_lock() {
    let mut cluster = ClusterFixture::bind(1).await;
    let mut cfg = (*cluster.config(0, &[])).clone();
    cfg.submit_timeout_ms = 50;
    let cfg = Arc::new(cfg);
    let budget = startup_budget(&cfg);
    let (raft, network, state, _, _, mut controls) =
        crate::raft::start_raft(cfg.clone(), Arc::new(Vec::new()), cluster.take_raft(0))
            .await
            .unwrap();
    let local_replica = raft.metrics().borrow_watched().id;
    assert!(
        advance_until(budget, async || {
            raft.current_leader().await == Some(local_replica)
                && state.read().await.genesis.is_some()
        })
        .await
    );
    let runtime = controls.runtime();
    assert!(runtime.current().is_some());
    let lock = state.write().await;
    let started = tokio::time::Instant::now();
    let result = crate::handoff::publish(&cfg, &raft, &state, &runtime, &network).await;
    assert_eq!(tokio::time::Instant::now(), started);
    drop(lock);
    assert!(runtime.current().is_some());
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    let error = format!("{:#}", result.unwrap_err());
    assert!(error.contains("requires active admission"), "{error}");
}

#[tokio::test]
async fn handoff_without_admission_returns_an_error() {
    let mut cluster = ClusterFixture::bind(3).await;
    let cfg = cluster.config(0, &[]);
    let (raft, network, state, _, _, mut controls) =
        crate::raft::start_raft(cfg.clone(), Arc::new(Vec::new()), cluster.take_raft(0))
            .await
            .unwrap();
    let runtime = controls.runtime();
    let result = crate::handoff::publish(&cfg, &raft, &state, &runtime, &network).await;
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    assert!(format!("{:#}", result.unwrap_err()).contains("requires active admission"));
}
