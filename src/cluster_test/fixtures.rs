use super::CLUSTER_TEST_ADDR;
use crate::config::{Config, HealthConfig, PeerConfig, RaftTuneConfig, VipConfig};
use crate::listener::ListenerSource;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

#[derive(Clone)]
pub(super) struct ControlledProbe(pub(super) Arc<std::sync::atomic::AtomicBool>);

impl ControlledProbe {
    pub(super) fn new(healthy: bool) -> Self {
        Self(Arc::new(std::sync::atomic::AtomicBool::new(healthy)))
    }
    pub(super) fn set(&self, healthy: bool) {
        self.0.store(healthy, std::sync::atomic::Ordering::SeqCst);
    }
}

impl crate::health_publication::Probe for ControlledProbe {
    async fn check(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

pub(super) async fn advance_until(budget: Duration, mut ready: impl AsyncFnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        if ready().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::advance(Duration::from_millis(10)).await;
    }
}

pub(super) fn startup_budget(cfg: &Config) -> Duration {
    let timing = crate::runtime_permission::LeaseTiming::for_config(cfg, cfg.vips.len()).unwrap();
    timing.startup_vip_delay().unwrap() + timing.consumer_use() * 2
}

#[tokio::test(start_paused = true)]
async fn virtual_wait_checks_the_deadline_boundary_and_bounds_failure() {
    let start = tokio::time::Instant::now();
    let budget = Duration::from_millis(20);
    assert!(
        advance_until(budget, async || tokio::time::Instant::now()
            == start + budget)
        .await
    );
    assert_eq!(tokio::time::Instant::now(), start + budget);
    assert!(!advance_until(budget, async || false).await);
    assert_eq!(tokio::time::Instant::now(), start + budget * 2);
}

pub(super) struct ClusterFixture {
    peers: Vec<PeerConfig>,
    reserved: ReservedListeners,
}

impl ClusterFixture {
    pub(super) async fn bind(nodes: usize) -> Self {
        static NEXT_FIXTURE: AtomicU32 = AtomicU32::new(1);
        let count = u32::try_from(nodes).unwrap();
        assert!((1..4096).contains(&count), "invalid fixture node count");
        let first = NEXT_FIXTURE.fetch_add(count, Ordering::Relaxed);
        assert!(first + count <= 4096, "fixture loopback range exhausted");
        // Keep closed fixtures away from live ones while kernel TIME_WAIT outlives virtual time.
        let base = 0x7f00_0000 | ((std::process::id() % 4095) << 12);
        let addresses: Vec<_> = (first..first + count)
            .map(|node| Ipv4Addr::from(base | node))
            .collect();
        // Distinct source IPs prevent peers from occupying a stopped node's listener port.
        let reserved =
            ReservedListeners::bind_addresses(addresses.iter().chain(&addresses).copied()).await;
        let ports = reserved.ports();
        let peers = (0..nodes)
            .map(|index| PeerConfig {
                id: index as u64 + 1,
                raft_address: format!("{}:{}", addresses[index], ports[index]),
                client_submit_address: format!("{}:{}", addresses[index], ports[index + nodes]),
            })
            .collect();
        Self { peers, reserved }
    }

    pub(super) fn config(&self, node_idx: usize, vips: &[VipConfig]) -> Arc<Config> {
        make_cfg(node_idx, &self.peers, vips)
    }

    pub(super) fn ports(&self) -> Vec<u16> {
        self.reserved.ports()
    }

    pub(super) fn take_raft(&mut self, node_idx: usize) -> ListenerSource {
        self.reserved.take(node_idx)
    }

    pub(super) fn take_submit(&mut self, node_idx: usize) -> ListenerSource {
        self.reserved.take(node_idx + self.peers.len())
    }
}

pub(super) struct ReservedListeners {
    listeners: Vec<Option<tokio::net::TcpListener>>,
}

impl ReservedListeners {
    pub(super) async fn bind(n: usize) -> Self {
        Self::bind_on(n, CLUSTER_TEST_ADDR.parse().unwrap()).await
    }

    pub(super) async fn bind_on(n: usize, address: Ipv4Addr) -> Self {
        Self::bind_addresses(std::iter::repeat_n(address, n)).await
    }

    async fn bind_addresses(addresses: impl IntoIterator<Item = Ipv4Addr>) -> Self {
        let mut listeners = Vec::new();
        for address in addresses {
            listeners.push(Some(
                tokio::net::TcpListener::bind((address, 0))
                    .await
                    .expect("reserve ephemeral listener"),
            ));
        }
        Self { listeners }
    }

    pub(super) fn ports(&self) -> Vec<u16> {
        self.listeners
            .iter()
            .map(|listener| {
                listener
                    .as_ref()
                    .expect("read ports before transferring listeners")
                    .local_addr()
                    .unwrap()
                    .port()
            })
            .collect()
    }

    pub(super) fn take(&mut self, index: usize) -> ListenerSource {
        ListenerSource::Bound(
            self.listeners[index]
                .take()
                .expect("transfer each reserved listener only once"),
        )
    }
}

pub(super) fn make_cfg(node_idx: usize, peers: &[PeerConfig], vips: &[VipConfig]) -> Arc<Config> {
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
        cluster_secret: Some("cluster-test-secret-01234567890123".into()),
        cluster_secret_file: None,
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

#[tokio::test]
async fn independent_fixtures_never_reuse_the_outbound_source_ip() {
    let (first, second) = tokio::join!(ClusterFixture::bind(3), ClusterFixture::bind(3));
    let first_address: std::net::SocketAddr = first.config(0, &[]).raft_listen.parse().unwrap();
    let second_address: std::net::SocketAddr = second.config(0, &[]).raft_listen.parse().unwrap();
    assert_ne!(first_address.ip(), second_address.ip());
    drop(first);
    let third = ClusterFixture::bind(1).await;
    let third_address: std::net::SocketAddr = third.config(0, &[]).raft_listen.parse().unwrap();
    assert_ne!(first_address.ip(), third_address.ip());
    assert_ne!(second_address.ip(), third_address.ip());
}

#[tokio::test]
async fn stopped_node_listener_is_not_stolen_by_another_nodes_outbound_port() {
    let mut cluster = ClusterFixture::bind(2).await;
    let stopped: SocketAddr = cluster.config(0, &[]).raft_listen.parse().unwrap();
    let running: SocketAddr = cluster.config(1, &[]).raft_listen.parse().unwrap();
    let old = cluster.take_raft(0).bind(stopped).await.unwrap();
    let peer = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    drop(cluster);
    drop(old);

    let outbound = tokio::net::TcpSocket::new_v4().unwrap();
    outbound
        .bind(SocketAddr::new(running.ip(), stopped.port()))
        .unwrap();
    let client = outbound.connect(peer.local_addr().unwrap()).await.unwrap();
    let (accepted, _) = peer.accept().await.unwrap();
    let restarted = ListenerSource::Configured
        .bind(stopped)
        .await
        .expect("another node's outgoing port must not occupy the stopped listener");
    assert_eq!(restarted.local_addr().unwrap(), stopped);
    assert_eq!(client.local_addr().unwrap().port(), stopped.port());
    assert_eq!(accepted.peer_addr().unwrap(), client.local_addr().unwrap());
}

#[tokio::test]
async fn fixture_connection_uses_reserved_address_and_advertised_source() {
    let mut cluster = ClusterFixture::bind(1).await;
    let cfg = cluster.config(0, &[]);
    let address: SocketAddr = cfg.raft_listen.parse().unwrap();
    let listener = cluster.take_raft(0).bind(address).await.unwrap();
    assert_eq!(listener.local_addr().unwrap(), address);
    let client = super::test_cluster_connect_to(&cfg.raft_listen)
        .await
        .unwrap();
    let (server, observed) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.ip(), address.ip());
    assert_eq!(client.peer_addr().unwrap(), address);
    drop(client);
    drop(server);
    drop(listener);
    let restarted = ListenerSource::Configured.bind(address).await.unwrap();
    assert_eq!(restarted.local_addr().unwrap(), address);
    assert_eq!(cluster.config(0, &[]).raft_listen, cfg.raft_listen);
}

#[tokio::test]
async fn standard_configs_preserve_defaults_topology_and_vips() {
    let vips = [
        VipConfig {
            address: "192.0.2.2/24".parse().unwrap(),
            interface: "lo".into(),
            vlan: Some(100),
        },
        VipConfig {
            address: "192.0.2.1/32".parse().unwrap(),
            interface: "lo".into(),
            vlan: None,
        },
    ];
    for nodes in [1, 3] {
        let cluster = ClusterFixture::bind(nodes).await;
        let ports = cluster.ports();
        let addresses: Vec<_> = cluster.reserved.listeners[..nodes]
            .iter()
            .map(|listener| listener.as_ref().unwrap().local_addr().unwrap().ip())
            .collect();
        assert_eq!(
            addresses
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            nodes
        );
        let peers: Vec<_> = (0..nodes)
            .map(|index| PeerConfig {
                id: index as u64 + 1,
                raft_address: format!("{}:{}", addresses[index], ports[index]),
                client_submit_address: format!("{}:{}", addresses[index], ports[index + nodes]),
            })
            .collect();
        let configs: Vec<_> = (0..nodes)
            .map(|index| cluster.config(index, &vips))
            .collect();
        let fingerprint = configs[0].cluster_config_fingerprint().unwrap();
        for (index, cfg) in configs.iter().enumerate() {
            assert_eq!(
                serde_json::to_value(cfg.as_ref()).unwrap(),
                serde_json::json!({
                    "node_id": index as u64 + 1,
                    "raft_listen": peers[index].raft_address,
                    "client_submit_listen": peers[index].client_submit_address,
                    "peers": peers,
                    "vips": vips,
                    "health": {
                        "command": ["/bin/true"],
                        "interval_ms": 200,
                        "timeout_ms": 500,
                        "stale_secs": 10,
                    },
                    "raft": RaftTuneConfig::default(),
                    "cluster_secret": "cluster-test-secret-01234567890123",
                    "max_frame_bytes": crate::config::DEFAULT_MAX_FRAME_BYTES,
                    "submit_timeout_ms": 2_000,
                    "address_protocol": crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL,
                    "dry_run": true,
                    "notify": null,
                    "failover_delay_secs": 0,
                    "failback": true,
                    "failback_delay_secs": 0,
                }),
            );
            assert_eq!(cfg.cluster_config_fingerprint().unwrap(), fingerprint);
        }
    }
}

#[tokio::test]
async fn config_overrides_do_not_change_other_nodes_or_later_configs() {
    let cluster = ClusterFixture::bind(3).await;
    let baseline = cluster.config(0, &[]);
    let other_node = cluster.config(1, &[]);
    let other_before = serde_json::to_value(other_node.as_ref()).unwrap();
    let mut overridden = (*baseline).clone();
    overridden.health.stale_secs = Some(60);
    overridden.health.command = vec!["/bin/false".into()];
    overridden.failover_delay_secs = 30;
    overridden.submit_timeout_ms = 50;
    overridden.notify = Some("notify.sh".into());
    overridden.dry_run = false;
    overridden.peers[0].client_submit_address = "invalid endpoint".into();

    assert_eq!(overridden.health.stale_secs, Some(60));
    assert_eq!(overridden.health.command, ["/bin/false"]);
    assert_eq!(overridden.failover_delay_secs, 30);
    assert_eq!(overridden.submit_timeout_ms, 50);
    assert_eq!(overridden.notify.as_deref(), Some("notify.sh"));
    assert!(!overridden.dry_run);
    assert_eq!(
        overridden.peers[0].client_submit_address,
        "invalid endpoint"
    );
    assert!(overridden.vips.is_empty());
    assert_eq!(
        serde_json::to_value(other_node.as_ref()).unwrap(),
        other_before
    );
    assert_eq!(
        serde_json::to_value(cluster.config(0, &[]).as_ref()).unwrap(),
        serde_json::to_value(baseline.as_ref()).unwrap(),
    );
}

#[test]
fn explicit_config_preserves_nonstandard_peer_ids_and_invalid_endpoints() {
    let peers = [PeerConfig {
        id: 42,
        raft_address: "invalid raft endpoint".into(),
        client_submit_address: "invalid submit endpoint".into(),
    }];
    let cfg = make_cfg(0, &peers, &[]);
    assert_eq!(cfg.node_id, 42);
    assert_eq!(cfg.raft_listen, peers[0].raft_address);
    assert_eq!(cfg.client_submit_listen, peers[0].client_submit_address);
    assert!(cfg.vips.is_empty());
}

#[tokio::test]
async fn config_endpoints_stay_reserved_through_listener_transfer() {
    for nodes in [1, 3] {
        let mut cluster = ClusterFixture::bind(nodes).await;
        let mut transferred = Vec::new();
        for index in (0..nodes).rev() {
            let cfg = cluster.config(index, &[]);
            for (source, configured) in [
                (cluster.take_raft(index), &cfg.raft_listen),
                (cluster.take_submit(index), &cfg.client_submit_listen),
            ] {
                assert!(matches!(&source, ListenerSource::Bound(_)));
                let address: std::net::SocketAddr = configured.parse().unwrap();
                assert_eq!(
                    TcpListener::bind(address).unwrap_err().kind(),
                    std::io::ErrorKind::AddrInUse,
                );
                transferred.push(source.bind(address).await.unwrap());
            }
        }
        drop(cluster);
        for listener in &transferred {
            assert_eq!(
                TcpListener::bind(listener.local_addr().unwrap())
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::AddrInUse,
            );
        }
    }
}
