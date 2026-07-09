//! In-process, end-to-end cluster tests.
//!
//! Spins up real `keepafloatd` daemons (via [`crate::run`]) on loopback ports with dry-run VIP
//! binding — both a single node and a three-node cluster — lets them auto-form, publish health and
//! reconcile VIPs, then asserts every VIP ends up bound on exactly one holder and is released on
//! shutdown.
//!
//! This exercises the networked stack the unit tests cannot reach — peer handshake + RPC transport
//! (`raft::network`), auto-formation (`raft::mod`), the full `RaftStorage` trait (`raft::store`),
//! follower→leader submit forwarding (`submit`) and the reconciliation loop (`vip`) — through the
//! public composition API only, so it stays valid as the transport internals evolve.
//!
//! Assertions are invariant-based (every VIP bound exactly once across the cluster; all released on
//! shutdown), never "which node holds which VIP", so the upcoming sticky/min-move placement change
//! does not contradict it.

use crate::config::{Config, HealthConfig, PeerConfig, RaftTuneConfig, VipAddr, VipConfig};
use crate::run;
use crate::vip::LocalVip;
use std::net::{IpAddr, Ipv4Addr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

fn ip4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(a, b, c, d))
}

/// Reserve `n` free loopback ports by binding then immediately releasing them. Good enough for a
/// local test; the small reuse window before the daemons bind is acceptable here.
fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port"))
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
        dry_run: true,
        notify: None,
        failback: true,
        failback_delay_secs: 0,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_cluster_binds_all_vips_then_releases_on_shutdown() {
    let ports = free_ports(2);
    let peers = vec![PeerConfig {
        id: 1,
        raft_address: format!("127.0.0.1:{}", ports[0]),
        client_submit_address: format!("127.0.0.1:{}", ports[1]),
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

    let _ = tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(15), handle).await;
    assert!(
        lv.bound_addrs().await.is_empty(),
        "VIPs not released on shutdown"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_forms_distributes_and_releases_vips() {
    let ports = free_ports(6);
    let raft_ports = &ports[0..3];
    let submit_ports = &ports[3..6];

    let peers: Vec<PeerConfig> = (0..3)
        .map(|i| PeerConfig {
            id: (i as u64) + 1,
            raft_address: format!("127.0.0.1:{}", raft_ports[i]),
            client_submit_address: format!("127.0.0.1:{}", submit_ports[i]),
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
        let _ = tokio::time::timeout(Duration::from_secs(15), handle).await;
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
/// and that releasing it (health loss → FAULT) triggers `INSTANCE <addr> FAULT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notify_script_fires_master_on_vip_acquisition_and_fault_on_health_failure() {
    // Fix #8: unique per-invocation suffix so parallel test runs don't share the same tmpdir.
    static NOTIFY_TEST_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let uid = NOTIFY_TEST_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let tmp = std::env::temp_dir().join(format!("kaf_notify_{}_{}", std::process::id(), uid));
    tokio::fs::create_dir_all(&tmp).await.unwrap();
    let script = tmp.join("notify.sh");
    let log = tmp.join("notify.log");

    // Health flag file: exists → healthy, absent → unhealthy.
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
        raft_address: format!("127.0.0.1:{}", ports[0]),
        client_submit_address: format!("127.0.0.1:{}", ports[1]),
    }];
    let vips = vec![VipConfig {
        address: "10.0.0.99/32".parse().unwrap(),
        interface: "lo".into(),
        vlan: None,
    }];

    let mut cfg = (*make_cfg(0, &peers, &vips)).clone();
    // Fix #6: quote the path so it is safe if TMPDIR contains spaces.
    cfg.health.command = vec![
        "/bin/sh".into(),
        "-c".into(),
        format!("test -f '{}'", health_flag.display()),
    ];
    cfg.notify = Some(script.to_str().unwrap().to_owned());
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

    // Wait for the VIP to be released (reconcile loop sees !local_ok → unbind → FAULT).
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
    let _ = tokio::time::timeout(Duration::from_secs(15), handle).await;
    let _ = tokio::fs::remove_dir_all(&tmp).await;
}
