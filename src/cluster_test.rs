//! In-process, end-to-end cluster tests.
//!
//! Spins up real `keepafloatd` daemons (via [`crate::run`]) on loopback ports with dry-run VIP
//! binding - both a single node and a three-node cluster - lets them auto-form, publish health and
//! reconcile VIPs, then asserts every VIP ends up bound on exactly one holder and is released on
//! shutdown.
//!
//! This exercises the networked stack the unit tests cannot reach - peer handshake + RPC transport
//! (`raft::network`), auto-formation (`raft::mod`), the full `RaftStorage` trait (`raft::store`),
//! follower-to-leader submit forwarding (`submit`) and the reconciliation loop (`vip`) through the
//! public composition API, so it stays valid as the transport internals evolve.
//!
//! Assertions are invariant-based (every VIP bound exactly once across the cluster; all released on
//! shutdown), never "which node holds which VIP", so the upcoming sticky/min-move placement change
//! does not contradict it.

use crate::config::{Config, HealthConfig, PeerConfig, RaftTuneConfig, VipAddr, VipConfig};
use crate::raft::KafRequest;
use crate::vip::LocalVip;
use crate::{
    StopReason, finish_daemon_run, record_lifecycle_result, record_optional_failure, run,
    stop_daemon_task, submit_task_failure, supervision_channel_failure, unexpected_task_failure,
};
use std::net::{IpAddr, Ipv4Addr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

mod health_proof;

// These tests release ephemeral-port reservations immediately before starting the daemons. Keep
// the complete network-test lifetime serial so another in-process cluster cannot claim that port
// window when the Rust test runner executes this module in parallel.
static CLUSTER_TEST_LOCK: Mutex<()> = Mutex::const_new(());
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

/// Reserve `n` free loopback ports by binding then immediately releasing them. Callers hold
/// [`CLUSTER_TEST_LOCK`] across the daemon lifetime to exclude other tests using this allocator.
fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind((CLUSTER_TEST_ADDR, 0)).expect("bind ephemeral port"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
}

fn make_cfg(node_idx: usize, peers: &[PeerConfig], vips: &[VipConfig]) -> Arc<Config> {
    let p = &peers[node_idx];
    Arc::new(Config {
        node_id: p.id,
        raft_listen: p.raft_address.clone(),
        client_submit_listen: p.client_submit_address.clone(),
        peers: peers.to_vec(),
        vips: vips.to_vec(),
        health: HealthConfig {
            command: vec!["/bin/true".into()],
            interval_ms: 200,
            timeout_ms: 500,
            // Generous staleness window so scheduling jitter under coverage instrumentation does
            // not transiently fence a healthy node.
            stale_secs: Some(10),
        },
        raft: RaftTuneConfig::default(),
        cluster_secret: None,
        max_frame_bytes: crate::config::DEFAULT_MAX_FRAME_BYTES,
        submit_timeout_ms: 2_000,
        address_protocol: crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL,
        dry_run: true,
        notify: None,
        failover_delay_secs: 0,
        failback: true,
        failback_delay_secs: 0,
    })
}

async fn raw_health_submit(port: u16) -> anyhow::Result<()> {
    let mut stream = test_cluster_connect(port).await?;
    let body = serde_json::to_vec(&serde_json::json!({
        "secret": null,
        "request": KafRequest::HealthUpdate {
            node_id: 1,
            healthy: true,
        },
    }))?;
    stream.write_all(&(body.len() as u32).to_be_bytes()).await?;
    stream.write_all(&body).await?;
    let mut len = [0_u8; 4];
    tokio::time::timeout(Duration::from_secs(1), stream.read_exact(&mut len)).await??;
    let mut response = vec![0_u8; u32::from_be_bytes(len) as usize];
    tokio::time::timeout(Duration::from_secs(1), stream.read_exact(&mut response)).await??;
    let response: serde_json::Value = serde_json::from_slice(&response)?;
    anyhow::ensure!(response["ok"] == true, "submit rejected: {response}");
    // #26: the response can arrive before the server task releases its source permit.
    // That body-local permit drops before the captured socket, so EOF orders slot reuse.
    let mut trailing = [0_u8; 1];
    let bytes = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut trailing)).await??;
    anyhow::ensure!(bytes == 0, "submit server sent data after its response");
    Ok(())
}

#[tokio::test]
async fn raw_health_submit_waits_for_server_close_after_successful_response() {
    let listener = tokio::net::TcpListener::bind((CLUSTER_TEST_ADDR, 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (replied_tx, replied_rx) = oneshot::channel();
    let (close_tx, close_rx) = oneshot::channel();
    // Lifetime: the test releases close_rx and joins this server before returning.
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).await.unwrap();
        let mut request = vec![0; u32::from_be_bytes(prefix) as usize];
        stream.read_exact(&mut request).await.unwrap();
        let reply = br#"{"ok":true}"#;
        stream
            .write_all(&(reply.len() as u32).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(reply).await.unwrap();
        replied_tx.send(()).unwrap();
        close_rx.await.unwrap();
    });
    let submit = raw_health_submit(port);
    tokio::pin!(submit);
    tokio::select! {
        result = &mut submit => panic!("submit finished before server closure: {result:?}"),
        result = replied_rx => result.unwrap(),
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut submit)
            .await
            .is_err(),
        "successful response is not proof that the server released its connection slot"
    );
    close_tx.send(()).unwrap();
    submit.await.unwrap();
    server.await.unwrap();
}

async fn test_cluster_connect(port: u16) -> anyhow::Result<tokio::net::TcpStream> {
    tokio::time::timeout(
        Duration::from_secs(1),
        crate::connection_admission::connect_from_advertised(
            &format!("{CLUSTER_TEST_ADDR}:0"),
            &format!("{CLUSTER_TEST_ADDR}:{port}"),
        ),
    )
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submit_admission_sheds_saturation_and_recovers() {
    let _cluster_test_guard = CLUSTER_TEST_LOCK.lock().await;
    let ports = free_ports(2);
    let peers = vec![PeerConfig {
        id: 1,
        raft_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[0]),
        client_submit_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[1]),
    }];
    let vips = vec![VipConfig {
        address: VipAddr::host(ip4(10, 0, 0, 1)),
        interface: "lo".into(),
        vlan: None,
    }];
    let cfg = make_cfg(0, &peers, &vips);
    let table = Arc::new(cfg.sorted_vips());
    let local = LocalVip::new(true);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(run(cfg, table, local.clone(), async move {
        let _ = shutdown_rx.await;
    }));

    for _ in 0..100 {
        if local.bound_addrs().await == [ip4(10, 0, 0, 1)] {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(local.bound_addrs().await, [ip4(10, 0, 0, 1)]);

    let mut raft_stalled = Vec::new();
    for _ in 0..8 {
        raft_stalled.push(test_cluster_connect(ports[0]).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut raft_excess = test_cluster_connect(ports[0]).await.unwrap();
    let mut byte = [0_u8; 1];
    let shed = tokio::time::timeout(Duration::from_secs(1), raft_excess.read(&mut byte))
        .await
        .expect("one source exceeded the eight-connection Raft quota");
    assert!(matches!(shed, Err(_) | Ok(0)));
    drop(raft_stalled);

    let mut stalled = Vec::new();
    // #27: one source must not consume the other peers' listener capacity.
    for _ in 0..7 {
        let mut stream = test_cluster_connect(ports[1]).await.unwrap();
        stream.write_all(&1_u32.to_be_bytes()).await.unwrap();
        stalled.push(stream);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    raw_health_submit(ports[1])
        .await
        .expect("one reserved slot must carry legitimate traffic under pressure");

    let mut last = test_cluster_connect(ports[1]).await.unwrap();
    last.write_all(&1_u32.to_be_bytes()).await.unwrap();
    stalled.push(last);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut excess = test_cluster_connect(ports[1]).await.unwrap();
    excess.write_all(&1_u32.to_be_bytes()).await.unwrap();
    let mut byte = [0_u8; 1];
    let shed = tokio::time::timeout(Duration::from_secs(1), excess.read(&mut byte))
        .await
        .expect("excess submit connection was not shed promptly");
    assert!(
        matches!(shed, Err(_) | Ok(0)),
        "excess submit connection remained admitted: {shed:?}"
    );

    drop(stalled.pop());
    tokio::time::sleep(Duration::from_millis(50)).await;
    raw_health_submit(ports[1])
        .await
        .expect("submit capacity did not recover after a stalled client disconnected");

    drop(stalled);
    let _ = shutdown_tx.send(());
    join_daemon(handle).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn occupied_submit_listener_fails_the_composition_root() {
    let _cluster_test_guard = CLUSTER_TEST_LOCK.lock().await;
    let raft_port = free_ports(1)[0];
    let occupied_submit = TcpListener::bind((CLUSTER_TEST_ADDR, 0)).expect("occupy submit port");
    let submit_port = occupied_submit.local_addr().unwrap().port();
    let peers = vec![PeerConfig {
        id: 1,
        raft_address: format!("{CLUSTER_TEST_ADDR}:{raft_port}"),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_cluster_binds_all_vips_then_releases_on_shutdown() {
    let _cluster_test_guard = CLUSTER_TEST_LOCK.lock().await;
    let ports = free_ports(2);
    let peers = vec![PeerConfig {
        id: 1,
        raft_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[0]),
        client_submit_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[1]),
    }];
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

    let cfg = make_cfg(0, &peers, &vips);
    let table = Arc::new(cfg.sorted_vips());
    let lv = LocalVip::new(true);
    let (tx, rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(run(cfg, table, lv.clone(), async move {
        let _ = rx.await;
    }));

    // A single node forms immediately and, as sole eligible holder, binds every VIP.
    let mut converged = false;
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if lv.bound_addrs().await == expected {
            converged = true;
            break;
        }
    }
    assert!(converged, "single node did not bind all VIPs");

    let mut stalled_submit = test_cluster_connect(ports[1]).await.unwrap();
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
    match tokio::net::TcpStream::connect((CLUSTER_TEST_ADDR, ports[0])).await {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_forms_distributes_and_releases_vips() {
    let _cluster_test_guard = CLUSTER_TEST_LOCK.lock().await;
    let ports = free_ports(6);
    let raft_ports = &ports[0..3];
    let submit_ports = &ports[3..6];

    let peers: Vec<PeerConfig> = (0..3)
        .map(|i| PeerConfig {
            id: (i as u64) + 1,
            raft_address: format!("{}:{}", CLUSTER_TEST_ADDR, raft_ports[i]),
            client_submit_address: format!("{}:{}", CLUSTER_TEST_ADDR, submit_ports[i]),
        })
        .collect();
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

    let mut locals: Vec<Arc<LocalVip>> = Vec::new();
    let mut shutdowns: Vec<oneshot::Sender<()>> = Vec::new();
    let mut handles = Vec::new();
    for i in 0..3 {
        let cfg = make_cfg(i, &peers, &vips);
        let table = Arc::new(cfg.sorted_vips());
        let lv = LocalVip::new(true);
        let (tx, rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(run(cfg, table, lv.clone(), async move {
            let _ = rx.await;
        }));
        locals.push(lv);
        shutdowns.push(tx);
        handles.push(handle);
    }

    // Wait for the cluster to form, elect a leader, commit health and reconcile: every VIP should
    // end up bound on exactly one node (union == all VIPs, with no duplicates across nodes).
    let mut converged = false;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut bound: Vec<IpAddr> = Vec::new();
        for lv in &locals {
            bound.extend(lv.bound_addrs().await);
        }
        bound.sort_unstable();
        if bound == expected {
            converged = true;
            break;
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notify_script_fires_master_on_vip_acquisition_and_fault_on_health_failure() {
    let _cluster_test_guard = CLUSTER_TEST_LOCK.lock().await;
    // Use a unique suffix so parallel test runs do not share the same temporary directory.
    static NOTIFY_TEST_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let uid = NOTIFY_TEST_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let tmp = std::env::temp_dir().join(format!("kaf_notify_{}_{}", std::process::id(), uid));
    tokio::fs::create_dir_all(&tmp).await.unwrap();
    let script = tmp.join("notify.sh");
    let log = tmp.join("notify.log");

    // The node is healthy while the flag exists and unhealthy after it is removed.
    let health_flag = tmp.join("healthy");
    tokio::fs::write(&health_flag, "").await.unwrap();

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

    let ports = free_ports(2);
    let peers = vec![PeerConfig {
        id: 1,
        raft_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[0]),
        client_submit_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[1]),
    }];
    let vips = vec![VipConfig {
        address: "10.0.0.99/32".parse().unwrap(),
        interface: "lo".into(),
        vlan: None,
    }];

    let mut cfg = (*make_cfg(0, &peers, &vips)).clone();
    // Quote the path because the temporary directory may contain spaces.
    cfg.health.command = vec![
        "/bin/sh".into(),
        "-c".into(),
        format!("test -f '{}'", health_flag.display()),
    ];
    cfg.notify = Some(script.to_str().unwrap().to_owned());
    cfg.failover_delay_secs = 1;
    // notify script needs to execute; override dry_run from make_cfg.
    cfg.dry_run = false;
    let cfg = Arc::new(cfg);
    let table = Arc::new(cfg.sorted_vips());
    let lv = LocalVip::new(true);
    let (tx, rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(run(cfg, table, lv.clone(), async move {
        let _ = rx.await;
    }));

    let vip_ip = ip4(10, 0, 0, 99);

    // Wait for MASTER bind.
    let mut bound = false;
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if lv.bound_addrs().await.contains(&vip_ip) {
            bound = true;
            break;
        }
    }
    assert!(bound, "VIP was not acquired");

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

    // Trigger FAULT: remove the health flag so the health check starts failing.
    tokio::fs::remove_file(&health_flag).await.unwrap();

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
    let _cluster_test_guard = CLUSTER_TEST_LOCK.lock().await;
    let ports = free_ports(6);
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
        handles.push(tokio::spawn(run(cfg, table, local.clone(), async move {
            let _ = shutdown_rx.await;
        })));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_single_voter_cluster_activates_v2_after_legacy_state() {
    let _cluster_test_guard = CLUSTER_TEST_LOCK.lock().await;
    let ports = free_ports(2);
    let peers = vec![PeerConfig {
        id: 1,
        raft_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[0]),
        client_submit_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[1]),
    }];
    let vips = vec![VipConfig {
        address: VipAddr::host(ip4(10, 0, 0, 1)),
        interface: "lo".into(),
        vlan: None,
    }];
    let cfg = make_cfg(0, &peers, &vips);
    let table = Arc::new(cfg.sorted_vips());
    let (raft, network, state_ref, _fatal_rx, _network_failure_rx, mut control_tasks) =
        crate::raft::start_raft(cfg, table).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let state = state_ref.read().await;
        if state.cluster_epoch.is_some()
            && state.failover_semantics == crate::raft::FailoverSemantics::V2
        {
            break;
        }
        drop(state);
        assert!(
            tokio::time::Instant::now() < deadline,
            "fresh single-voter cluster did not form with V2 semantics"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    state_ref.write().await.failover_semantics = crate::raft::FailoverSemantics::Legacy;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if state_ref.read().await.failover_semantics == crate::raft::FailoverSemantics::V2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "all-new existing cluster did not activate V2 semantics"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    control_tasks.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_health_probe_withdraws_vips_and_recovers_without_restart() {
    let _cluster_test_guard = CLUSTER_TEST_LOCK.lock().await;
    let ports = free_ports(2);
    let peers = vec![PeerConfig {
        id: 1,
        raft_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[0]),
        client_submit_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[1]),
    }];
    let address = ip4(10, 0, 0, 98);
    let vips = vec![VipConfig {
        address: VipAddr::host(address),
        interface: "lo".into(),
        vlan: None,
    }];
    let directory = std::env::temp_dir().join(format!("kafd-proof-{}", ports[0]));
    tokio::fs::create_dir(&directory).await.unwrap();
    let block = directory.join("block");
    let entered = directory.join("entered");
    let mut cfg = (*make_cfg(0, &peers, &vips)).clone();
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
    let cfg = Arc::new(cfg);
    let table = Arc::new(cfg.sorted_vips());
    let local = LocalVip::new(true);
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(run(cfg, table, local.clone(), async move {
        let _ = rx.await;
    }));
    let outcome = tokio::time::timeout(Duration::from_secs(4), async {
        while !local.bound_addrs().await.contains(&address) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::fs::write(&block, b"").await.unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minimum_stale_window_does_not_flap_during_regular_probe_jitter() {
    let _cluster_test_guard = CLUSTER_TEST_LOCK.lock().await;
    let ports = free_ports(2);
    let peers = vec![PeerConfig {
        id: 1,
        raft_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[0]),
        client_submit_address: format!("{}:{}", CLUSTER_TEST_ADDR, ports[1]),
    }];
    let address = ip4(10, 0, 0, 97);
    let vips = vec![VipConfig {
        address: VipAddr::host(address),
        interface: "lo".into(),
        vlan: None,
    }];
    let directory = std::env::temp_dir().join(format!("kafd-renewal-{}", ports[0]));
    tokio::fs::create_dir(&directory).await.unwrap();
    let flag = directory.join("alternate");
    let mut cfg = (*make_cfg(0, &peers, &vips)).clone();
    cfg.health.interval_ms = 1_000;
    cfg.health.stale_secs = Some(1);
    cfg.health.timeout_ms = 1_000;
    cfg.health.command = vec![
        "/bin/sh".into(),
        "-c".into(),
        format!(
            "if test -f '{0}'; then rm '{0}'; sleep 0.15; else touch '{0}'; fi",
            flag.display()
        ),
    ];
    let cfg = Arc::new(cfg);
    let table = Arc::new(cfg.sorted_vips());
    let local = LocalVip::new(true);
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(run(cfg, table, local.clone(), async move {
        let _ = rx.await;
    }));
    let acquired = tokio::time::timeout(Duration::from_secs(2), async {
        while !local.bound_addrs().await.contains(&address) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    let until = tokio::time::Instant::now() + Duration::from_millis(2_200);
    let mut flapped = false;
    while acquired && tokio::time::Instant::now() < until {
        if !local.bound_addrs().await.contains(&address) {
            flapped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = tx.send(());
    join_daemon(handle).await.unwrap();
    tokio::fs::remove_dir_all(&directory).await.unwrap();
    assert!(acquired, "healthy node did not acquire its VIP");
    assert!(
        !flapped,
        "regular successful probe renewal flapped a healthy VIP"
    );
}
