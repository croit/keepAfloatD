//! Pre-Vote regressions over isolated loopback peers.

use super::client::RaftConnection;
use super::*;
use openraft::network::RPCOption;
use openraft::network::v2::RaftNetworkV2;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use serde_json::json;
use tokio::sync::oneshot;

use super::request::{Operation, decode_payload, encode};
use super::wire::{read_handshake, write_framed};

fn request() -> VoteRequest<TypeConfig> {
    VoteRequest::new(
        VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(1)),
        None,
    )
}

#[test]
fn pre_vote_is_enabled_in_daemon_configuration() {
    let network = super::tests::test_network(65_536, &[1, 2, 3]);
    let config = crate::raft::build_openraft_config(&network.config).unwrap();
    assert_eq!(config.enable_pre_vote, Some(true));
}

#[tokio::test]
async fn pre_vote_unknown_target_cannot_fabricate_a_grant() {
    let mut connection = RaftConnection {
        network: Arc::new(super::tests::test_network(65_536, &[1, 2, 3])),
        target: crate::raft::types::test_replica(99),
    };
    let result = connection
        .pre_vote(request(), RPCOption::new(Duration::from_millis(50)))
        .await;
    assert!(
        result.is_err(),
        "unknown target fabricated a grant: {result:?}"
    );
}

#[tokio::test]
async fn pre_vote_unreachable_unknown_capability_cannot_fabricate_a_grant() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = (*super::tests::test_network(65_536, &[1, 2]).config).clone();
    config.peers[1].raft_address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let (_, _, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
    let mut connection = RaftConnection {
        network: Arc::new(
            RaftNetworkImpl::new(
                Arc::new(config.clone()),
                state,
                crate::raft::network::testing::controller(&config),
            )
            .unwrap(),
        ),
        target: crate::raft::types::test_replica(2),
    };
    let result = connection
        .pre_vote(request(), RPCOption::new(Duration::from_millis(50)))
        .await;
    assert!(
        result.is_err(),
        "unreachable peer fabricated a grant: {result:?}"
    );
}

#[derive(Clone, Copy, Debug)]
enum PeerReply {
    Supported(bool),
    MissingCapability,
    UnsupportedCapability,
    MalformedStatus,
    StatusTimeout,
    PolicyChanged,
    ForeignEpoch,
    ForeignConfig,
    Disconnect,
    MalformedVote,
    WrongVoteOperation,
    Timeout,
}

async fn exchange(reply: PeerReply) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = (*super::tests::test_network(65_536, &[1, 2]).config).clone();
    config.peers[1].raft_address = listener.local_addr().unwrap().to_string();
    let (_, _, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
    state.write().await.cluster_epoch = Some(1);
    let network = Arc::new(
        RaftNetworkImpl::new(
            Arc::new(config.clone()),
            state,
            crate::raft::network::testing::controller(&config),
        )
        .unwrap(),
    );
    let fingerprint = network.config_fingerprint;
    let local_state = network.state_ref.clone();
    let (observed_tx, observed_rx) = oneshot::channel();
    // Lifetime: serves one attempt; joined or cancelled before this helper returns.
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let (id, epoch, _) = read_handshake(&mut stream, 2, Some("network-test-secret"))
            .await
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(epoch, Some(1));
        let body = read_framed_bounded(&mut stream, 65_536).await.unwrap();
        let status: crate::raft::probe::ClusterStatusRequest =
            decode_payload(&body, Operation::Status).unwrap();
        assert_eq!(status.config_fingerprint, Some(fingerprint));
        observed_tx.send(()).unwrap();
        if matches!(reply, PeerReply::StatusTimeout) {
            std::future::pending::<()>().await;
        }
        if matches!(reply, PeerReply::MalformedStatus) {
            write_framed(&mut stream, br#"{"supports_pre_vote":false}"#)
                .await
                .unwrap();
            return;
        }
        let mut response = json!({
            "initialized": true, "current_leader": null, "member_count": 2,
            "cluster_epoch": 1, "config_fingerprint": fingerprint,
            "supports_config_identity_v1": true, "supports_pre_vote": true,
            "supports_failover_semantics_v2": true
        });
        if matches!(reply, PeerReply::PolicyChanged) {
            local_state.write().await.cluster_epoch = Some(2);
        }
        match reply {
            PeerReply::MissingCapability => {
                response
                    .as_object_mut()
                    .unwrap()
                    .remove("supports_pre_vote");
            }
            PeerReply::UnsupportedCapability => response["supports_pre_vote"] = json!(false),
            PeerReply::ForeignEpoch => response["cluster_epoch"] = json!(2),
            PeerReply::ForeignConfig => {
                response["config_fingerprint"]["digest"] = json!([255; 32].to_vec())
            }
            _ => {}
        }
        write_framed(&mut stream, &encode(Operation::Status, &response).unwrap())
            .await
            .unwrap();
        if matches!(
            reply,
            PeerReply::MissingCapability
                | PeerReply::UnsupportedCapability
                | PeerReply::ForeignEpoch
                | PeerReply::ForeignConfig
                | PeerReply::PolicyChanged
        ) {
            return;
        }
        let body = read_framed_bounded(&mut stream, 65_536).await.unwrap();
        assert!(serde_json::from_slice::<VoteRequest<TypeConfig>>(&body).is_err());
        let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(envelope, json!({"pre_vote": request()}));
        match reply {
            PeerReply::Supported(grant) => {
                let response = VoteResponse::<TypeConfig>::new(
                    VoteOf::<TypeConfig>::new(3, crate::raft::types::test_replica(2)),
                    None,
                    grant,
                );
                write_framed(&mut stream, &encode(Operation::PreVote, &response).unwrap())
                    .await
                    .unwrap();
            }
            PeerReply::MalformedVote => write_framed(&mut stream, b"not-json").await.unwrap(),
            PeerReply::WrongVoteOperation => {
                let response = VoteResponse::<TypeConfig>::new(
                    VoteOf::<TypeConfig>::new(3, crate::raft::types::test_replica(2)),
                    None,
                    true,
                );
                write_framed(&mut stream, &encode(Operation::Vote, &response).unwrap())
                    .await
                    .unwrap();
            }
            PeerReply::Timeout => std::future::pending::<()>().await,
            _ => {}
        }
    });
    let result = RaftConnection {
        network,
        target: crate::raft::types::test_replica(2),
    }
    .pre_vote(request(), RPCOption::new(Duration::from_millis(150)))
    .await;
    server.abort();
    let joined = server.await;
    assert!(joined.is_ok() || joined.is_err_and(|error| error.is_cancelled()));
    assert!(
        observed_rx.await.is_ok(),
        "Pre-Vote skipped authenticated capability discovery: {reply:?}"
    );
    result
}

#[tokio::test]
async fn pre_vote_supported_peer_returns_its_real_decision() {
    for grant in [false, true] {
        let response = exchange(PeerReply::Supported(grant)).await.unwrap();
        assert_eq!(response.vote_granted, grant);
        assert_eq!(
            response.vote,
            VoteOf::<TypeConfig>::new(3, crate::raft::types::test_replica(2))
        );
    }
}

#[tokio::test]
async fn pre_vote_missing_required_capability_never_fabricates_a_grant() {
    for reply in [
        PeerReply::MissingCapability,
        PeerReply::UnsupportedCapability,
    ] {
        assert!(
            exchange(reply)
                .await
                .unwrap_err()
                .to_string()
                .contains("required Pre-Vote")
        );
    }
}

#[tokio::test]
async fn pre_vote_protocol_transport_and_safety_errors_never_grant() {
    for reply in [
        PeerReply::MalformedStatus,
        PeerReply::StatusTimeout,
        PeerReply::PolicyChanged,
        PeerReply::ForeignEpoch,
        PeerReply::ForeignConfig,
        PeerReply::Disconnect,
        PeerReply::MalformedVote,
        PeerReply::WrongVoteOperation,
        PeerReply::Timeout,
    ] {
        assert!(
            exchange(reply).await.is_err(),
            "fabricated a grant for {reply:?}"
        );
    }
}

struct Cluster {
    rafts: Vec<KafRaft>,
    networks: Vec<Arc<RaftNetworkImpl>>,
    blocked: tokio::sync::watch::Sender<Option<usize>>,
    proxies: tokio::task::JoinSet<()>,
}

impl Cluster {
    async fn new() -> Self {
        let mut reserved = Vec::new();
        let mut listeners = Vec::new();
        for _ in 0..3 {
            reserved.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
            listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let mut config = (*super::tests::test_network(65_536, &[1, 2, 3]).config).clone();
        for (peer, listener) in config.peers.iter_mut().zip(&listeners) {
            peer.raft_address = listener.local_addr().unwrap().to_string();
        }
        let (blocked, _) = tokio::sync::watch::channel(None);
        let mut proxies = tokio::task::JoinSet::new();
        for (target, listener) in listeners.into_iter().enumerate() {
            let address = reserved[target].local_addr().unwrap();
            let blocked = blocked.clone();
            // Lifetime: the cluster owns each proxy and its accepted streams until teardown.
            proxies.spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut clients = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let (mut incoming, _) = accepted.unwrap();
                            let mut changes = blocked.subscribe();
                            clients.spawn(async move {
                                // Route by the authenticated hello's claimed ID without terminating HMAC.
                                let mut prefix = [0; 20];
                                match incoming.read_exact(&mut prefix).await {
                                    Ok(_) => {},
                                    Err(error) => {
                                        assert!(matches!(error.kind(), std::io::ErrorKind::UnexpectedEof
                                            | std::io::ErrorKind::ConnectionReset));
                                        return;
                                    }
                                }
                                assert_eq!(&prefix[..8], b"KAFDAUTH");
                                let source = u64::from_be_bytes(prefix[12..20].try_into().unwrap());
                                assert!((1..=3).contains(&source));
                                let source = source as usize - 1;
                                let is_blocked = |blocked: &Option<usize>| {
                                    *blocked == Some(source) || *blocked == Some(target)
                                };
                                if is_blocked(&changes.borrow_and_update()) { return; }
                                let mut outgoing = TcpStream::connect(address).await.unwrap();
                                outgoing.write_all(&prefix).await.unwrap();
                                tokio::select! {
                                    result = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing) => {
                                        if let Err(error) = result {
                                            assert!(matches!(error.kind(), std::io::ErrorKind::ConnectionReset
                                                | std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::UnexpectedEof));
                                        }
                                    }
                                    _ = changes.wait_for(is_blocked) => {}
                                }
                            });
                        }
                        result = clients.join_next(), if !clients.is_empty() => { result.unwrap().unwrap(); }
                    }
                }
            });
        }
        let mut rafts = Vec::new();
        let mut networks = Vec::new();
        for (index, reservation) in reserved.into_iter().enumerate() {
            let mut cfg = config.clone();
            cfg.node_id = (index + 1) as u64;
            cfg.raft_listen = reservation.local_addr().unwrap().to_string();
            let (log, machine, state) =
                crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
            let network = Arc::new(
                RaftNetworkImpl::new(
                    Arc::new(cfg.clone()),
                    state,
                    crate::raft::network::testing::controller(&cfg),
                )
                .unwrap(),
            );
            let mut raft_config = crate::raft::build_openraft_config(&cfg).unwrap();
            raft_config.enable_tick = false;
            let raft = KafRaft::new(
                crate::raft::types::test_replica(cfg.node_id),
                Arc::new(raft_config),
                network.as_ref().clone(),
                log,
                machine,
            )
            .await
            .unwrap();
            let error = std::net::TcpListener::bind(&cfg.raft_listen)
                .expect_err("reserved Raft port must remain owned until server startup");
            assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
            network
                .start_with_listener(
                    raft.clone(),
                    crate::listener::ListenerSource::Bound(reservation),
                )
                .await
                .unwrap();
            networks.push(network);
            rafts.push(raft);
        }
        let members: std::collections::BTreeMap<_, _> = config
            .peers
            .iter()
            .map(|peer| {
                (
                    crate::raft::types::test_replica(peer.id),
                    openraft::BasicNode::new(&peer.raft_address),
                )
            })
            .collect();
        rafts[0].initialize(members).await.unwrap();
        for raft in &rafts {
            raft.wait(Some(Duration::from_secs(3)))
                .current_leader(crate::raft::types::test_replica(1), "initial leader")
                .await
                .unwrap();
        }
        Self {
            rafts,
            networks,
            blocked,
            proxies,
        }
    }

    async fn settle(&self) {
        // Advance injected time while letting the real loopback I/O and Raft actors drain.
        for _ in 0..100 {
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_millis(10)).await;
        }
    }

    async fn stop(mut self) {
        for network in &self.networks {
            network.shutdown().await.unwrap();
        }
        for raft in &self.rafts {
            raft.shutdown().await.unwrap();
        }
        self.proxies.abort_all();
        while let Some(result) = self.proxies.join_next().await {
            assert!(result.is_ok() || result.is_err_and(|error| error.is_cancelled()));
        }
    }
}

#[tokio::test]
async fn pre_vote_cluster_keeps_reserved_ports_until_server_start() {
    Cluster::new().await.stop().await;
}

#[tokio::test]
async fn pre_vote_isolated_follower_rejoins_without_term_inflation() {
    let cluster = Cluster::new().await;
    tokio::time::pause();
    let before = cluster.networks[2].state_ref.read().await.vote;
    cluster.blocked.send_replace(Some(2));
    for _ in 0..4 {
        cluster.rafts[2].trigger().elect(true).await.unwrap();
        cluster.settle().await;
        assert_eq!(
            cluster.networks[2].state_ref.read().await.vote,
            before,
            "isolated follower inflated its saved vote"
        );
    }
    cluster.blocked.send_replace(None);
    cluster.rafts[0].trigger().heartbeat().await.unwrap();
    cluster.settle().await;
    use openraft::async_runtime::WatchReceiver;
    for raft in &cluster.rafts {
        assert_eq!(
            raft.metrics().borrow_watched().current_leader,
            Some(crate::raft::types::test_replica(1))
        );
    }
    let after = cluster.networks[0].state_ref.read().await.vote;
    assert_eq!(after, before, "follower rejoin disrupted the leader's vote");
    cluster.stop().await;
}

#[tokio::test]
async fn pre_vote_majority_can_replace_a_lost_leader() {
    let cluster = Cluster::new().await;
    tokio::time::pause();
    cluster.blocked.send_replace(Some(0));
    tokio::time::advance(Duration::from_secs(3)).await;
    cluster.settle().await;
    cluster.rafts[1].trigger().elect(true).await.unwrap();
    cluster.settle().await;
    tokio::time::resume();
    cluster.rafts[1].trigger().elect(true).await.unwrap();
    for raft in &cluster.rafts[1..] {
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(
                crate::raft::types::test_replica(2),
                "replacement majority leader",
            )
            .await
            .unwrap();
    }
    use openraft::async_runtime::WatchReceiver;
    assert_eq!(
        cluster.rafts[1].metrics().borrow_watched().current_leader,
        Some(crate::raft::types::test_replica(2))
    );
    assert_eq!(
        cluster.rafts[2].metrics().borrow_watched().current_leader,
        Some(crate::raft::types::test_replica(2))
    );
    cluster.stop().await;
}

#[tokio::test]
async fn pre_vote_authentication_and_config_fences_precede_dispatch() {
    let cluster = Cluster::new().await;
    let vote_before = cluster.networks[1].state_ref.read().await.vote;
    for wrong_secret in [true, false] {
        let mut config = (*cluster.networks[0].config).clone();
        if wrong_secret {
            config.cluster_secret = Some("incorrect-test-peer-secret".into());
        } else {
            config.failback = !config.failback;
        }
        let (_, _, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
        let network = Arc::new(
            RaftNetworkImpl::new(
                Arc::new(config.clone()),
                state,
                crate::raft::network::testing::controller(&config),
            )
            .unwrap(),
        );
        let result = RaftConnection {
            network,
            target: crate::raft::types::test_replica(2),
        }
        .pre_vote(request(), RPCOption::new(Duration::from_millis(200)))
        .await;
        assert!(result.is_err(), "incompatible peer fabricated a grant");
        assert_eq!(cluster.networks[1].state_ref.read().await.vote, vote_before);
    }
    cluster.stop().await;
}
