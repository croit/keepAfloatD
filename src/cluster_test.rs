//! In-process, end-to-end cluster tests.
//!
//! Spins up real `keepafloatd` daemons (via [`crate::run_with_listeners`]) on loopback ports with
//! dry-run VIP binding - both a single node and a three-node cluster - lets them auto-form, publish
//! health and reconcile VIPs, then asserts every VIP ends up bound on exactly one holder and is
//! released on shutdown.
//!
//! This exercises the networked stack the unit tests cannot reach - peer handshake + RPC transport
//! (`raft::network`), auto-formation (`raft::mod`), the full `RaftStorage` trait (`raft::store`),
//! follower-to-leader submit forwarding (`submit`) and the reconciliation loop (`vip`) through the
//! public composition API, so it stays valid as the transport internals evolve.
//!
//! Assertions are invariant-based (every VIP bound exactly once across the cluster; all released on
//! shutdown), never "which node holds which VIP", so the upcoming sticky/min-move placement change
//! does not contradict it.

use crate::config::{PeerConfig, VipAddr, VipConfig};
use crate::listener::ListenerSource;
use crate::raft::KafRequest;
use crate::vip::LocalVip;
use crate::{
    StopReason, finish_daemon_run, record_lifecycle_result, record_optional_failure, run,
    run_with_listeners, run_with_probe, stop_daemon_task, submit_task_failure,
    supervision_channel_failure, unexpected_task_failure,
};
use std::net::{IpAddr, Ipv4Addr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

mod bind_health;
mod fixtures;
mod health_proof;
mod restart;
mod stop_handoff;
mod submit_pressure;

use fixtures::{
    ClusterFixture, ControlledProbe, ReservedListeners, advance_until, make_cfg, startup_budget,
};

const CLUSTER_TEST_ADDR: &str = "127.255.255.254";

async fn join_daemon(
    handle: JoinHandle<anyhow::Result<Option<crate::raft::FatalReason>>>,
) -> anyhow::Result<Option<crate::raft::FatalReason>> {
    join_daemon_with_timeout(handle, Duration::from_secs(15)).await
}

async fn join_daemon_with_timeout(
    handle: JoinHandle<anyhow::Result<Option<crate::raft::FatalReason>>>,
    timeout: Duration,
) -> anyhow::Result<Option<crate::raft::FatalReason>> {
    let abort = handle.abort_handle();
    match tokio::time::timeout(timeout, handle).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(anyhow::anyhow!("daemon task failed: {error}")),
        Err(_) => {
            abort.abort();
            Err(anyhow::anyhow!(
                "daemon did not stop within {}ms",
                timeout.as_millis()
            ))
        }
    }
}

#[tokio::test]
async fn daemon_join_oracle_rejects_timeout_panic_and_run_error() {
    let timed_out = tokio::spawn(std::future::pending());
    assert!(
        join_daemon_with_timeout(timed_out, Duration::from_millis(1))
            .await
            .unwrap_err()
            .to_string()
            .contains("did not stop")
    );

    let panicked = tokio::spawn(async {
        panic!("test panic");
        #[allow(unreachable_code)]
        Ok(None)
    });
    assert!(
        join_daemon_with_timeout(panicked, Duration::from_secs(1))
            .await
            .unwrap_err()
            .to_string()
            .contains("task failed")
    );

    let failed = tokio::spawn(async { Err(anyhow::anyhow!("run failed")) });
    assert!(
        join_daemon_with_timeout(failed, Duration::from_secs(1))
            .await
            .unwrap_err()
            .to_string()
            .contains("run failed")
    );
}

#[tokio::test]
async fn essential_task_completion_is_always_a_daemon_failure() {
    let completed = tokio::spawn(async {});
    assert!(
        unexpected_task_failure("health task", completed.await)
            .to_string()
            .contains("exited unexpectedly")
    );

    let panicked = tokio::spawn(async { panic!("essential task panic") });
    assert!(
        unexpected_task_failure("VIP reconciliation task", panicked.await)
            .to_string()
            .contains("task failed")
    );
}

#[tokio::test]
async fn daemon_task_cleanup_rejects_racing_completion_and_panic() {
    let mut pending = tokio::spawn(std::future::pending::<()>());
    assert!(
        stop_daemon_task("pending task", &mut pending)
            .await
            .is_none()
    );

    let mut completed = tokio::spawn(async {});
    tokio::task::yield_now().await;
    assert!(
        stop_daemon_task("completed task", &mut completed)
            .await
            .unwrap()
            .contains("exited before cancellation")
    );

    let mut panicked = tokio::spawn(async { panic!("cleanup race panic") });
    tokio::task::yield_now().await;
    assert!(
        stop_daemon_task("panicked task", &mut panicked)
            .await
            .unwrap()
            .contains("task failed")
    );
}

#[tokio::test]
async fn task_and_channel_failure_classification_preserves_diagnostics() {
    assert!(
        supervision_channel_failure("network", None)
            .to_string()
            .contains("channel closed unexpectedly")
    );
    assert_eq!(
        supervision_channel_failure("network", Some("accept failed".into())).to_string(),
        "accept failed"
    );

    let completed = tokio::spawn(async { Ok::<(), anyhow::Error>(()) });
    assert!(
        submit_task_failure(completed.await)
            .to_string()
            .contains("exited unexpectedly")
    );
    let failed = tokio::spawn(async { Err::<(), _>(anyhow::anyhow!("accept failed")) });
    assert!(
        format!("{:#}", submit_task_failure(failed.await)).contains("submit server: accept failed")
    );
    let panicked = tokio::spawn(async {
        panic!("submit panic");
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    });
    assert!(
        submit_task_failure(panicked.await)
            .to_string()
            .contains("task failed")
    );

    let mut failures = Vec::new();
    record_optional_failure(&mut failures, None);
    record_optional_failure(&mut failures, Some("task cleanup".into()));
    record_lifecycle_result(
        &mut failures,
        "network shutdown",
        Ok::<(), anyhow::Error>(()),
    );
    record_lifecycle_result(
        &mut failures,
        "network shutdown",
        Err(anyhow::anyhow!("join failed")),
    );
    assert_eq!(failures, ["task cleanup", "network shutdown: join failed"]);
}

#[test]
fn daemon_result_preserves_primary_and_shutdown_failures() {
    assert!(finish_daemon_run(StopReason::Complete(None), Vec::new()).is_ok());

    let cleanup_only = finish_daemon_run(
        StopReason::Complete(None),
        vec!["graceful VIP cleanup: failed".into()],
    )
    .unwrap_err();
    assert!(cleanup_only.to_string().contains("daemon shutdown failed"));

    let primary_only = finish_daemon_run(
        StopReason::Failed(anyhow::anyhow!("submit server failed")),
        Vec::new(),
    )
    .unwrap_err();
    assert!(primary_only.to_string().contains("submit server failed"));

    let combined = finish_daemon_run(
        StopReason::Failed(anyhow::anyhow!("submit server failed")),
        vec!["graceful VIP cleanup: failed".into()],
    )
    .unwrap_err();
    let diagnostic = format!("{combined:#}");
    assert!(diagnostic.contains("submit server failed"));
    assert!(diagnostic.contains("additional daemon shutdown failures"));
    assert!(diagnostic.contains("graceful VIP cleanup: failed"));
}

fn ip4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(a, b, c, d))
}

#[tokio::test]
async fn reserved_cluster_listeners_reject_competing_binds() {
    let mut reserved = ReservedListeners::bind(6).await;
    let ports = reserved.ports();
    for &port in &ports {
        let error = TcpListener::bind((CLUSTER_TEST_ADDR, port))
            .expect_err("fixture reservation must prevent competing bind");
        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
    }

    let sources: Vec<_> = (0..ports.len()).map(|index| reserved.take(index)).collect();
    drop(reserved);
    for (source, port) in sources.iter().zip(ports) {
        let ListenerSource::Bound(listener) = source else {
            panic!("reserved listener must be transferred, not rebound");
        };
        assert_eq!(listener.local_addr().unwrap().port(), port);
        let error = TcpListener::bind((CLUSTER_TEST_ADDR, port))
            .expect_err("transferred listener must prevent competing bind");
        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
    }
}

async fn test_cluster_connect(port: u16) -> anyhow::Result<tokio::net::TcpStream> {
    test_cluster_connect_to(&format!("{CLUSTER_TEST_ADDR}:{port}")).await
}

async fn test_cluster_connect_to(address: &str) -> anyhow::Result<tokio::net::TcpStream> {
    tokio::time::timeout(
        Duration::from_secs(1),
        crate::connection_admission::connect_from_advertised(address, address),
    )
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn occupied_submit_listener_fails_the_composition_root() {
    let occupied_submit = TcpListener::bind((CLUSTER_TEST_ADDR, 0)).expect("occupy submit port");
    let submit_port = occupied_submit.local_addr().unwrap().port();
    let peers = vec![PeerConfig {
        id: 1,
        raft_address: format!("{CLUSTER_TEST_ADDR}:0"),
        client_submit_address: format!("{CLUSTER_TEST_ADDR}:{submit_port}"),
    }];
    let vips = vec![VipConfig {
        address: VipAddr::host(ip4(10, 0, 0, 1)),
        interface: "lo".into(),
        vlan: None,
    }];
    let cfg = make_cfg(0, &peers, &vips);
    let table = Arc::new(cfg.sorted_vips());
    let local = LocalVip::new(true);

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        run(cfg, table, local, std::future::pending()),
    )
    .await
    .expect("submit-listener failure did not stop the daemon");

    let error = result.expect_err("occupied submit listener must fail startup");
    assert!(error.to_string().contains("submit server"), "{error:#}");
}

#[tokio::test(start_paused = true)]
async fn single_node_cluster_binds_all_vips_then_releases_on_shutdown() {
    let mut cluster = ClusterFixture::bind(1).await;
    let vips = vec![
        VipConfig {
            address: VipAddr::host(ip4(10, 0, 0, 1)),
            interface: "lo".into(),
            vlan: None,
        },
        VipConfig {
            address: VipAddr::host(ip4(10, 0, 0, 2)),
            interface: "lo".into(),
            vlan: None,
        },
    ];
    let mut expected: Vec<IpAddr> = vips.iter().map(|v| v.address.addr).collect();
    expected.sort_unstable();

    let cfg = cluster.config(0, &vips);
    let raft_address = cfg.raft_listen.clone();
    let submit_address = cfg.client_submit_listen.clone();
    let budget = startup_budget(&cfg);
    let table = Arc::new(cfg.sorted_vips());
    let lv = LocalVip::new(true);
    let (tx, rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(run_with_probe(
        cfg,
        table,
        lv.clone(),
        async move {
            let _ = rx.await;
        },
        cluster.take_raft(0),
        cluster.take_submit(0),
        ControlledProbe::new(true),
    ));

    tokio::task::yield_now().await;
    assert!(lv.bound_addrs().await.is_empty());
    let converged = advance_until(budget, async || lv.bound_addrs().await == expected).await;
    assert!(converged, "single node did not bind all VIPs");

    let mut stalled_submit = test_cluster_connect_to(&submit_address).await.unwrap();
    stalled_submit
        .write_all(&64_u32.to_be_bytes())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let _ = tx.send(());
    join_daemon(handle).await.unwrap();
    assert!(
        lv.bound_addrs().await.is_empty(),
        "VIPs not released on shutdown"
    );
    match tokio::net::TcpStream::connect(&raft_address).await {
        Ok(_) => panic!("Raft listener must not accept connections after shutdown"),
        Err(error) => assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionRefused,
            "Raft listener probe failed unexpectedly"
        ),
    }
    let mut byte = [0_u8; 1];
    let read = tokio::time::timeout(Duration::from_millis(250), stalled_submit.read(&mut byte))
        .await
        .expect("accepted submit connection survived server shutdown")
        .unwrap();
    assert_eq!(read, 0, "submit connection remained open after shutdown");
}

#[tokio::test(start_paused = true)]
async fn three_node_cluster_forms_distributes_and_releases_vips() {
    let (logs, _capture) = crate::warning_limit::test_support::LogCapture::start(
        "keepafloatd::raft::admission=debug,keepafloatd::health=warn",
    );
    let mut cluster = ClusterFixture::bind(3).await;
    let vips = vec![
        VipConfig {
            address: VipAddr::host(ip4(10, 0, 0, 1)),
            interface: "lo".into(),
            vlan: None,
        },
        VipConfig {
            address: VipAddr::host(ip4(10, 0, 0, 2)),
            interface: "lo".into(),
            vlan: None,
        },
        VipConfig {
            address: VipAddr::host(ip4(10, 0, 0, 3)),
            interface: "lo".into(),
            vlan: None,
        },
    ];
    let mut expected: Vec<IpAddr> = vips.iter().map(|v| v.address.addr).collect();
    expected.sort_unstable();
    let mut budget = Duration::ZERO;

    let mut locals: Vec<Arc<LocalVip>> = Vec::new();
    let mut shutdowns: Vec<oneshot::Sender<()>> = Vec::new();
    let mut handles = Vec::new();
    for i in 0..3 {
        let mut cfg = cluster.config(i, &vips);
        // Bound protocol work during startup fencing without extending health freshness.
        let tune = Arc::make_mut(&mut cfg);
        tune.health.interval_ms = 1_000;
        tune.health.stale_secs = Some(3);
        budget = budget.max(startup_budget(&cfg));
        let table = Arc::new(cfg.sorted_vips());
        let lv = LocalVip::new(true);
        let (tx, rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(run_with_probe(
            cfg,
            table,
            lv.clone(),
            async move {
                let _ = rx.await;
            },
            cluster.take_raft(i),
            cluster.take_submit(i),
            ControlledProbe::new(true),
        ));
        locals.push(lv);
        shutdowns.push(tx);
        handles.push(handle);
    }

    // Wait for the cluster to form, elect a leader, commit health and reconcile: every VIP should
    // end up bound on exactly one node (union == all VIPs, with no duplicates across nodes).
    let converged = advance_until(budget, async || {
        if handles.iter().any(|handle| handle.is_finished()) {
            return true;
        }
        let mut bound: Vec<IpAddr> = Vec::new();
        for lv in &locals {
            bound.extend(lv.bound_addrs().await);
        }
        bound.sort_unstable();
        bound == expected
    })
    .await;
    for handle in &mut handles {
        if handle.is_finished() {
            panic!(
                "daemon exited before convergence: {:?}\n{}",
                handle.await,
                logs.text()
            );
        }
    }
    assert!(
        converged,
        "cluster did not bind each VIP exactly once across the three nodes"
    );

    // Signal shutdown and let each daemon tear down (which unbinds the VIPs it held).
    for tx in shutdowns {
        let _ = tx.send(());
    }
    for handle in handles {
        join_daemon(handle).await.unwrap();
    }
    for lv in &locals {
        assert!(
            lv.bound_addrs().await.is_empty(),
            "node still holds VIPs after shutdown"
        );
    }
}

/// End-to-end notify hook test: a single-node cluster with a real notify script.
///
/// Verifies that acquiring a VIP causes the script to be invoked with `INSTANCE <addr> MASTER`,
/// and that a health-loss release triggers `INSTANCE <addr> FAULT`.
#[tokio::test(start_paused = true)]
async fn notify_script_fires_master_on_vip_acquisition_and_fault_on_health_failure() {
    // Use a unique suffix so parallel test runs do not share the same temporary directory.
    static NOTIFY_TEST_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let uid = NOTIFY_TEST_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let tmp = std::env::temp_dir().join(format!("kaf_notify_{}_{}", std::process::id(), uid));
    tokio::fs::create_dir_all(&tmp).await.unwrap();
    let script = tmp.join("notify.sh");
    let log = tmp.join("notify.log");

    let probe = ControlledProbe::new(true);

    tokio::fs::write(
        &script,
        format!(
            "#!/bin/sh\necho \"$1 $2 $3\" >> {log}\n",
            log = log.display()
        ),
    )
    .await
    .unwrap();
    tokio::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
        .await
        .unwrap();

    let mut cluster = ClusterFixture::bind(1).await;
    let vips = vec![VipConfig {
        address: "10.0.0.99/32".parse().unwrap(),
        interface: "lo".into(),
        vlan: None,
    }];

    let mut cfg = (*cluster.config(0, &vips)).clone();
    cfg.notify = Some(script.to_str().unwrap().to_owned());
    cfg.failover_delay_secs = 1;
    // notify script needs to execute; override dry_run from the fixture.
    cfg.dry_run = false;
    let cfg = Arc::new(cfg);
    let budget = startup_budget(&cfg);
    let table = Arc::new(cfg.sorted_vips());
    let lv = LocalVip::new(true);
    let (tx, rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(run_with_probe(
        cfg,
        table,
        lv.clone(),
        async move {
            let _ = rx.await;
        },
        cluster.take_raft(0),
        cluster.take_submit(0),
        probe.clone(),
    ));

    let vip_ip = ip4(10, 0, 0, 99);

    let bound = advance_until(budget, async || lv.bound_addrs().await.contains(&vip_ip)).await;
    assert!(bound, "VIP was not acquired");
    tokio::time::resume();

    // Poll for the log entry instead of sleeping a fixed duration.
    // tokio::fs avoids blocking a worker thread during the read.
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let content = tokio::fs::read_to_string(&log).await.unwrap_or_default();
            if content.contains("INSTANCE 10.0.0.99 MASTER") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                tokio::time::Instant::now() < deadline,
                "timeout waiting for MASTER notify; got: {content:?}"
            );
        }
    }

    probe.set(false);

    // The first failed probe starts, but cannot complete, the configured one-second delay.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        lv.bound_addrs().await.contains(&vip_ip),
        "VIP was released before failover_delay_secs elapsed"
    );

    // Wait for the reconcile loop to observe !local_ok, unbind the VIP and report FAULT.
    let mut released = false;
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if lv.bound_addrs().await.is_empty() {
            released = true;
            break;
        }
    }
    assert!(released, "VIP was not released after health failure");

    // Poll for FAULT entry; tokio::fs avoids blocking a worker thread during the read.
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let content = tokio::fs::read_to_string(&log).await.unwrap_or_default();
            if content.contains("INSTANCE 10.0.0.99 FAULT") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                tokio::time::Instant::now() < deadline,
                "timeout waiting for FAULT notify; got: {content:?}"
            );
        }
    }

    let _ = tx.send(());
    join_daemon(handle).await.unwrap();
    let _ = tokio::fs::remove_dir_all(&tmp).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_mismatch_fatal_signal_runs_composition_root_cleanup() {
    let mut reserved = ReservedListeners::bind(6).await;
    let ports = reserved.ports();
    let peers: Vec<PeerConfig> = (0..3)
        .map(|index| PeerConfig {
            id: index as u64 + 1,
            raft_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[index]),
            client_submit_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[index + 3]),
        })
        .collect();
    let vips = vec![VipConfig {
        address: VipAddr::host(ip4(10, 0, 0, 99)),
        interface: "lo".into(),
        vlan: None,
    }];

    let mut locals = Vec::new();
    let mut shutdowns = Vec::new();
    let mut handles = Vec::new();
    for index in 0..3 {
        let mut cfg = (*make_cfg(index, &peers, &vips)).clone();
        if index == 0 {
            cfg.health.stale_secs = Some(11);
        }
        let cfg = Arc::new(cfg);
        let table = Arc::new(cfg.sorted_vips());
        let local = LocalVip::new(true);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        handles.push(tokio::spawn(run_with_listeners(
            cfg,
            table,
            local.clone(),
            async move {
                let _ = shutdown_rx.await;
            },
            reserved.take(index),
            reserved.take(index + 3),
        )));
        locals.push(local);
        shutdowns.push(shutdown_tx);
    }

    let mismatched = handles.remove(0);
    let result = tokio::time::timeout(Duration::from_secs(15), mismatched)
        .await
        .expect("mismatched node did not self-fence")
        .unwrap()
        .unwrap();
    assert_eq!(result, Some(crate::raft::FatalReason::ConfigMismatch));
    assert!(locals[0].bound_addrs().await.is_empty());

    for shutdown in shutdowns.into_iter().skip(1) {
        let _ = shutdown.send(());
    }
    for handle in handles {
        tokio::time::timeout(Duration::from_secs(15), handle)
            .await
            .expect("matching node did not stop")
            .unwrap()
            .unwrap();
    }
    for local in locals {
        assert!(local.bound_addrs().await.is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn single_voter_genesis_activates_v2_and_rejects_replacement() {
    let mut cluster = ClusterFixture::bind(1).await;
    let vips = vec![VipConfig {
        address: VipAddr::host(ip4(10, 0, 0, 1)),
        interface: "lo".into(),
        vlan: None,
    }];
    let cfg = cluster.config(0, &vips);
    let budget = startup_budget(&cfg);
    let table = Arc::new(cfg.sorted_vips());
    let (raft, network, state_ref, _fatal_rx, _network_failure_rx, mut control_tasks) =
        crate::raft::start_raft(cfg, table, cluster.take_raft(0))
            .await
            .unwrap();

    assert!(
        advance_until(budget, async || {
            let state = state_ref.read().await;
            state.genesis.is_some()
                && state.failover_semantics == crate::raft::FailoverSemantics::V2
        })
        .await,
        "fresh cluster did not commit its V2 genesis"
    );
    let genesis = state_ref.read().await.genesis.clone().unwrap();
    let mut changed = genesis.clone();
    changed.epoch ^= 1;
    let response = raft
        .client_write(KafRequest::AdmissionGenesis(changed))
        .await
        .unwrap();
    assert!(matches!(
        response.data,
        crate::raft::types::KafResponse::Rejected(_)
    ));
    assert_eq!(state_ref.read().await.genesis.as_ref(), Some(&genesis));
    assert_eq!(state_ref.read().await.cluster_epoch, Some(genesis.epoch));
    assert_eq!(
        state_ref.read().await.failover_semantics,
        crate::raft::FailoverSemantics::V2
    );

    control_tasks.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn blocked_health_probe_withdraws_vips_and_recovers_without_restart() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct AfterStartupProbe {
        enabled: AtomicBool,
        command: crate::health_publication::CommandProbe,
    }
    impl crate::health_publication::Probe for Arc<AfterStartupProbe> {
        async fn check(&self) -> bool {
            !self.enabled.load(Ordering::SeqCst) || self.command.check().await
        }
    }
    let mut cluster = ClusterFixture::bind(1).await;
    let address = ip4(10, 0, 0, 98);
    let vips = vec![VipConfig {
        address: VipAddr::host(address),
        interface: "lo".into(),
        vlan: None,
    }];
    let directory = std::env::temp_dir().join(format!(
        "kafd-proof-{}-{}",
        std::process::id(),
        cluster.config(0, &[]).raft_listen.replace(':', "-")
    ));
    tokio::fs::create_dir(&directory).await.unwrap();
    let block = directory.join("block");
    let entered = directory.join("entered");
    let mut cfg = (*cluster.config(0, &vips)).clone();
    cfg.health.stale_secs = Some(1);
    cfg.health.timeout_ms = 2_000;
    cfg.health.command = vec![
        "/bin/sh".into(),
        "-c".into(),
        format!(
            "if test -f '{}'; then touch '{}'; sleep 1.5; fi",
            block.display(),
            entered.display()
        ),
    ];
    let probe = Arc::new(AfterStartupProbe {
        enabled: AtomicBool::new(false),
        command: crate::health_publication::CommandProbe::new(cfg.health.clone()),
    });
    let cfg = Arc::new(cfg);
    let budget = startup_budget(&cfg);
    let table = Arc::new(cfg.sorted_vips());
    let local = LocalVip::new(true);
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(run_with_probe(
        cfg,
        table,
        local.clone(),
        async move {
            let _ = rx.await;
        },
        cluster.take_raft(0),
        cluster.take_submit(0),
        probe.clone(),
    ));
    assert!(
        advance_until(budget, async || local
            .bound_addrs()
            .await
            .contains(&address))
        .await
    );
    tokio::time::resume();
    let outcome = tokio::time::timeout(Duration::from_secs(4), async {
        tokio::fs::write(&block, b"").await.unwrap();
        probe.enabled.store(true, Ordering::SeqCst);
        while !entered.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        while !local.bound_addrs().await.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!handle.is_finished(), "fencing must leave the daemon alive");
        tokio::fs::remove_file(&block).await.unwrap();
        while !local.bound_addrs().await.contains(&address) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let _ = tx.send(());
    join_daemon(handle).await.unwrap();
    tokio::fs::remove_dir_all(&directory).await.unwrap();
    assert!(
        outcome.is_ok(),
        "blocked health probe did not fence and recover"
    );
    assert!(local.bound_addrs().await.is_empty());
}

#[tokio::test(start_paused = true)]
async fn minimum_stale_window_does_not_flap_during_regular_probe_jitter() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct JitterProbe {
        enabled: AtomicBool,
        alternate: AtomicBool,
        delayed: AtomicUsize,
    }
    impl crate::health_publication::Probe for Arc<JitterProbe> {
        async fn check(&self) -> bool {
            if self.enabled.load(Ordering::SeqCst)
                && self.alternate.fetch_xor(true, Ordering::SeqCst)
            {
                tokio::time::sleep(Duration::from_millis(150)).await;
                self.delayed.fetch_add(1, Ordering::SeqCst);
            }
            true
        }
    }
    let mut cluster = ClusterFixture::bind(1).await;
    let address = ip4(10, 0, 0, 97);
    let vips = vec![VipConfig {
        address: VipAddr::host(address),
        interface: "lo".into(),
        vlan: None,
    }];
    let mut cfg = (*cluster.config(0, &vips)).clone();
    cfg.health.interval_ms = 1_000;
    cfg.health.stale_secs = Some(1);
    cfg.health.timeout_ms = 1_000;
    let cfg = Arc::new(cfg);
    let budget = startup_budget(&cfg);
    let table = Arc::new(cfg.sorted_vips());
    let local = LocalVip::new(true);
    let (tx, rx) = oneshot::channel();
    let probe = Arc::new(JitterProbe {
        enabled: AtomicBool::new(false),
        alternate: AtomicBool::new(true),
        delayed: AtomicUsize::new(0),
    });
    let handle = tokio::spawn(run_with_probe(
        cfg,
        table,
        local.clone(),
        async move {
            let _ = rx.await;
        },
        cluster.take_raft(0),
        cluster.take_submit(0),
        probe.clone(),
    ));
    let acquired = advance_until(budget, async || {
        local.bound_addrs().await.contains(&address)
    })
    .await;
    probe.enabled.store(true, Ordering::SeqCst);
    let flapped = advance_until(Duration::from_millis(2_200), async || {
        !local.bound_addrs().await.contains(&address)
    })
    .await;
    let _ = tx.send(());
    join_daemon(handle).await.unwrap();
    assert!(acquired, "healthy node did not acquire its VIP");
    assert!(
        probe.delayed.load(Ordering::SeqCst) > 0,
        "jitter was not exercised"
    );
    assert!(
        !flapped,
        "regular successful probe renewal flapped a healthy VIP"
    );
}
