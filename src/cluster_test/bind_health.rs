use super::*;
use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Output};

fn empty_inventory() -> std::io::Result<Output> {
    Ok(Output {
        status: ExitStatus::from_raw(0),
        stdout: b"[]".to_vec(),
        stderr: Vec::new(),
    })
}

#[tokio::test(start_paused = true)]
async fn bind_fault_is_published_despite_successful_service_probes() {
    let mut cluster = ClusterFixture::bind(3).await;
    let addr = ip4(192, 0, 2, 99);
    let vips = [VipConfig {
        address: VipAddr::host(addr),
        interface: "lo".into(),
        vlan: None,
    }];
    let mut budget = Duration::ZERO;
    let mut locals = Vec::new();
    let mut probes = Vec::new();
    let mut stops = Vec::new();
    let mut handles = Vec::new();
    for index in 0..3 {
        let mut cfg = (*cluster.config(index, &vips)).clone();
        cfg.failover_delay_secs = 30;
        cfg.health.interval_ms = 1_000;
        // Stale expiry must not satisfy the four-second bind-fault takeover check.
        cfg.health.stale_secs = Some(10);
        budget = budget.max(startup_budget(&cfg));
        let probe = ControlledProbe::new(index == 0);
        let cfg = Arc::new(cfg);
        let local = LocalVip::new(index != 0);
        if index == 0 {
            local
                .force_startup_discovery_result(empty_inventory())
                .await;
            local
                .force_startup_marker_discovery_results(empty_inventory(), empty_inventory())
                .await;
            local
                .force_next_startup_cleanup_results(
                    ("lo", addr, 32),
                    Ok(ExitStatus::from_raw(0)),
                    None,
                )
                .await;
            local
                .force_next_bind_result(
                    ("lo", addr, 32),
                    Err(std::io::Error::other("interface missing")),
                )
                .await;
        }
        let (stop, stopped) = oneshot::channel();
        // Every daemon is stopped and joined before this test returns.
        handles.push(tokio::spawn(run_with_probe(
            cfg.clone(),
            Arc::new(cfg.sorted_vips()),
            local.clone(),
            async {
                let _ = stopped.await;
            },
            cluster.take_raft(index),
            cluster.take_submit(index),
            probe.clone(),
        )));
        probes.push(probe);
        locals.push(local);
        stops.push(stop);
    }
    let outcome = std::panic::AssertUnwindSafe(async {
        assert!(
            advance_until(budget, async || locals[0].bind_attempts(addr).await > 0).await,
            "the initially healthy node did not attempt binding after runtime admission"
        );
        for probe in &probes[1..] {
            probe.set(true);
        }
        assert!(
            advance_until(Duration::from_secs(4), async || {
                !locals[1].bound_addrs().await.is_empty()
                    || !locals[2].bound_addrs().await.is_empty()
            })
            .await,
            "a healthy peer did not acquire the VIP after the failed bind"
        );
        assert_eq!(
            locals[0].bind_attempts(addr).await,
            1,
            "healthy probes must not retry the failed bind"
        );
        assert!(locals[0].bound_addrs().await.is_empty());
        assert!(!handles[0].is_finished());
    });
    use futures::FutureExt;
    let outcome = outcome.catch_unwind().await;
    for stop in stops {
        let _ = stop.send(());
    }
    for handle in handles {
        join_daemon(handle).await.unwrap();
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    for local in locals {
        assert!(
            local.bound_addrs().await.is_empty(),
            "a node retained its VIP after shutdown"
        );
    }
}
