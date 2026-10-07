//! Regression coverage for the inbound deadlines lost during the #26 rebase.

use super::inbound::{InboundPeer, InboundStreamPolicy, serve_raft_stream};
use super::request::{Operation, decode_payload, encode};
use super::wire::{
    read_framed_bounded, read_framed_bounded_with_timeout_and_budget, write_framed, write_handshake,
};
use super::{KafRaft, RaftNetworkImpl};
use crate::connection_admission::FrameByteBudget;
use crate::raft::probe::{ClusterStatusRequest, ClusterStatusResponse};
use crate::raft::store::new_store;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test(start_paused = true)]
async fn outbound_rpc_deadline_includes_waiting_for_state() {
    let network = super::tests::test_network(65_536, &[1, 2]);
    let _state = network.state_ref.write().await;
    let request = super::testing::vote_request();
    let rpc = network.send_rpc::<_, serde_json::Value>(
        crate::raft::types::test_replica(2),
        &request,
        super::RPCTypes::Vote,
        Duration::from_secs(1),
    );
    tokio::pin!(rpc);
    assert!(futures::poll!(&mut rpc).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(
        futures::poll!(&mut rpc),
        std::task::Poll::Ready(Err(super::RPCError::Timeout(_)))
    ));
    assert!(network.peers[&2].stream.try_lock().is_ok());
}

#[tokio::test(start_paused = true)]
async fn pre_vote_deadline_includes_waiting_for_state() {
    use openraft::alias::VoteOf;
    use openraft::network::RPCOption;
    use openraft::network::v2::RaftNetworkV2;
    use openraft::raft::VoteRequest;

    let network = Arc::new(super::tests::test_network(65_536, &[1, 2]));
    let _state = network.state_ref.write().await;
    let mut connection = super::client::RaftConnection {
        network: network.clone(),
        target: crate::raft::types::test_replica(2),
    };
    let rpc = connection.pre_vote(
        VoteRequest::new(
            VoteOf::<crate::raft::types::TypeConfig>::new(1, crate::raft::types::test_replica(1)),
            None,
        ),
        RPCOption::new(Duration::from_secs(1)),
    );
    tokio::pin!(rpc);
    assert!(futures::poll!(&mut rpc).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(
        futures::poll!(&mut rpc),
        std::task::Poll::Ready(Err(super::RPCError::Timeout(_)))
    ));
}

#[tokio::test(start_paused = true)]
async fn idle_authenticated_frame_read_expires() {
    let (_client, mut server) = tokio::io::duplex(16);
    let budget = FrameByteBudget::new(64);
    let read = read_framed_bounded_with_timeout_and_budget(
        &mut server,
        64,
        Duration::from_secs(5),
        &budget,
    );
    tokio::pin!(read);
    assert!(futures::poll!(&mut read).is_pending());
    tokio::time::advance(Duration::from_secs(6)).await;
    let error = tokio::time::timeout(Duration::from_millis(100), &mut read)
        .await
        .expect("idle frame prefix retained its connection forever")
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
}

async fn assert_shared_frame_deadline(finish_prefix: bool) {
    let (mut client, mut server) = tokio::io::duplex(16);
    client.write_all(&[0]).await.unwrap();
    let budget = FrameByteBudget::new(64);
    let read = read_framed_bounded_with_timeout_and_budget(
        &mut server,
        64,
        Duration::from_secs(5),
        &budget,
    );
    tokio::pin!(read);
    assert!(futures::poll!(&mut read).is_pending());
    tokio::time::advance(Duration::from_secs(3)).await;
    client
        .write_all(if finish_prefix {
            &[0, 0, 8, 1][..]
        } else {
            &[0][..]
        })
        .await
        .unwrap();
    assert!(futures::poll!(&mut read).is_pending());
    tokio::time::advance(Duration::from_secs(3)).await;
    let error = tokio::time::timeout(Duration::from_millis(100), &mut read)
        .await
        .expect("trickled prefix/body restarted or bypassed the deadline")
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(
        budget.available_bytes(),
        64,
        "timeout leaked frame-byte permits"
    );
}

#[tokio::test(start_paused = true)]
async fn partial_prefix_cannot_extend_the_frame_deadline() {
    assert_shared_frame_deadline(false).await;
}

#[tokio::test(start_paused = true)]
async fn prefix_and_body_share_one_frame_deadline() {
    assert_shared_frame_deadline(true).await;
}

struct Fixture {
    network: RaftNetworkImpl,
    raft: KafRaft,
    address: std::net::SocketAddr,
}

impl Fixture {
    async fn new(heartbeat_ms: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        Self::from_listener(heartbeat_ms, listener).await
    }

    async fn from_listener(heartbeat_ms: u64, listener: TcpListener) -> Self {
        let address = listener.local_addr().unwrap();
        Self::from_connection_listener(heartbeat_ms, listener, address).await
    }

    async fn from_connection_listener(
        heartbeat_ms: u64,
        listener: impl crate::listener::ConnectionListener,
        address: std::net::SocketAddr,
    ) -> Self {
        let mut config = (*super::tests::test_network(65_536, &[1]).config).clone();
        config.raft_listen = address.to_string();
        config.peers[0].raft_address = address.to_string();
        config.raft.heartbeat_interval_ms = heartbeat_ms;
        let (log, machine, state) = new_store(Arc::new(Vec::new()), 3, true, 0);
        let network = RaftNetworkImpl::new(
            Arc::new(config.clone()),
            state,
            crate::raft::network::testing::controller(&config),
        )
        .unwrap();
        let raft = KafRaft::new(
            crate::raft::types::test_replica(1),
            Arc::new(openraft::Config::default()),
            network.clone(),
            log,
            machine,
        )
        .await
        .unwrap();
        // #26: retain this socket through construction; a close/rebind races parallel fixtures.
        // This self-only fixture needs the real accept task but no outbound reconnect loops.
        assert!(network.peers.is_empty());
        super::claim_start(&network.started).unwrap();
        let (failure_tx, _failure_rx) = tokio::sync::mpsc::unbounded_channel();
        let accept_task = super::server::spawn_raft_accept_task(
            listener,
            super::server::RaftAcceptContext {
                admission: network.admission.clone(),
                config: network.config.clone(),
                raft: raft.clone(),
                state_ref: network.state_ref.clone(),
                config_fingerprint: network.config_fingerprint,
                shutdown: network.shutdown.clone(),
            },
            failure_tx,
        );
        network.tasks.lock().await.push(accept_task);
        Self {
            network,
            raft,
            address,
        }
    }

    async fn connect(&self) -> TcpStream {
        let mut stream = TcpStream::connect(self.address).await.unwrap();
        super::wire::write_replica_handshake(
            &mut stream,
            crate::raft::types::test_replica(1),
            1,
            Some("network-test-secret"),
            None,
            false,
        )
        .await
        .unwrap();
        status(&mut stream).await;
        stream
    }

    async fn stop(self) {
        self.network.shutdown().await.unwrap();
        self.raft.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn accept_error_does_not_stop_raft_listener() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let faults = Arc::new(std::sync::atomic::AtomicUsize::new(1));
    let fixture = Fixture::from_connection_listener(
        250,
        crate::listener::test_support::FaultyListener {
            inner: listener,
            faults: faults.clone(),
        },
        address,
    )
    .await;
    tokio::time::timeout(Duration::from_millis(100), async {
        while faults.load(std::sync::atomic::Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Raft listener did not attempt accept");
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut tasks = fixture.network.tasks.lock().await;
        tokio::select! {
            result = &mut tasks[0].handle => panic!("accept error stopped Raft: {result:?}"),
            _client = fixture.connect() => {},
        }
    })
    .await
    .expect("Raft listener did not recover");
    fixture.stop().await;
}

#[tokio::test]
async fn accepted_raft_stream_remains_usable_during_accept_backoff() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let faults = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fixture = Fixture::from_connection_listener(
        250,
        crate::listener::test_support::FaultyListener {
            inner: listener,
            faults: faults.clone(),
        },
        address,
    )
    .await;
    let mut established = fixture.connect().await;
    faults.store(1, std::sync::atomic::Ordering::SeqCst);
    let wake_accept = TcpStream::connect(address).await.unwrap();
    tokio::time::timeout(Duration::from_millis(200), async {
        while faults.load(std::sync::atomic::Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
        drop(wake_accept);
        status(&mut established).await;
    })
    .await
    .expect("accept backoff blocked an established Raft stream");
    fixture.stop().await;
    assert_closed(&mut established).await;
}

#[tokio::test]
async fn raft_shutdown_cancels_accept_backoff() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let faults = Arc::new(std::sync::atomic::AtomicUsize::new(2));
    let fixture = Fixture::from_connection_listener(
        250,
        crate::listener::test_support::FaultyListener {
            inner: listener,
            faults: faults.clone(),
        },
        address,
    )
    .await;
    tokio::time::timeout(Duration::from_millis(100), async {
        while faults.load(std::sync::atomic::Ordering::SeqCst) == 2 {
            tokio::task::yield_now().await;
        }
        fixture.stop().await;
    })
    .await
    .expect("retry delay prevented Raft shutdown");
    assert_eq!(faults.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn idle_listener_shutdown_does_not_wait_for_a_connection() {
    let fixture = Fixture::new(250).await;
    tokio::time::timeout(Duration::from_secs(1), fixture.stop())
        .await
        .expect("idle accept prevented shutdown");
}

#[tokio::test]
async fn rejected_handshake_does_not_stop_later_status_requests() {
    let fixture = Fixture::new(250).await;
    let mut rejected = TcpStream::connect(fixture.address).await.unwrap();
    write_handshake(&mut rejected, 1, 1, Some("wrong-secret"), None, false)
        .await
        .unwrap_err();
    rejected.shutdown().await.unwrap();
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), rejected.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let _client = fixture.connect().await;
    fixture.stop().await;
}

#[tokio::test]
async fn fixture_adopts_bound_listener_and_closes_accepted_streams_on_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    // Establish before construction: closing and rebinding cannot preserve this queued stream.
    let mut stream = TcpStream::connect(address).await.unwrap();
    let fixture = Fixture::from_listener(250, listener).await;
    assert_eq!(fixture.address, address);
    write_handshake(&mut stream, 1, 1, Some("network-test-secret"), None, false)
        .await
        .unwrap();
    status(&mut stream).await;
    fixture.stop().await;
    assert_closed(&mut stream).await;
}

async fn status(stream: &mut TcpStream) {
    let body = encode(
        Operation::Status,
        &ClusterStatusRequest {
            probe_from: 1,
            config_fingerprint: None,
            supports_cancellation_safe_rpc_v1: true,
        },
    )
    .unwrap();
    write_framed(stream, &body).await.unwrap();
    let response =
        tokio::time::timeout(Duration::from_secs(1), read_framed_bounded(stream, 65_536))
            .await
            .unwrap()
            .unwrap();
    let _: ClusterStatusResponse = decode_payload(&response, Operation::Status).unwrap();
}

async fn assert_closed(stream: &mut TcpStream) {
    let mut byte = [0];
    let result = tokio::time::timeout(Duration::from_millis(100), stream.read(&mut byte))
        .await
        .expect("expired connection is still open");
    assert!(
        matches!(result, Ok(0) | Err(_)),
        "unexpected response: {result:?}"
    );
}

#[tokio::test]
async fn unknown_peer_handshake_is_rejected_without_poisoning_admission() {
    let fixture = Fixture::new(250).await;
    let mut stream = TcpStream::connect(fixture.address).await.unwrap();
    write_handshake(
        &mut stream,
        999,
        1,
        Some("network-test-secret"),
        None,
        false,
    )
    .await
    .unwrap();
    assert_closed(&mut stream).await;
    let mut recovered = fixture.connect().await;
    status(&mut recovered).await;
    fixture.stop().await;
}

#[tokio::test]
async fn incorrect_secret_is_rejected_without_poisoning_admission() {
    let fixture = Fixture::new(250).await;
    let mut stream = TcpStream::connect(fixture.address).await.unwrap();
    write_handshake(&mut stream, 1, 1, Some("incorrect-secret"), None, false)
        .await
        .unwrap_err();
    stream.shutdown().await.unwrap();
    assert_closed(&mut stream).await;
    let mut recovered = fixture.connect().await;
    status(&mut recovered).await;
    fixture.stop().await;
}

#[tokio::test]
async fn truncated_handshake_is_rejected_without_poisoning_admission() {
    let fixture = Fixture::new(250).await;
    let mut stream = TcpStream::connect(fixture.address).await.unwrap();
    stream.write_all(&[0]).await.unwrap();
    stream.shutdown().await.unwrap();
    assert_closed(&mut stream).await;
    let mut recovered = fixture.connect().await;
    status(&mut recovered).await;
    fixture.stop().await;
}

#[tokio::test]
async fn authenticated_stream_does_not_consume_source_handshake_capacity() {
    let fixture = Fixture::new(250).await;
    let mut authenticated = fixture.connect().await;
    let mut handshakes = Vec::new();
    for _ in 0..7 {
        handshakes.push(TcpStream::connect(fixture.address).await.unwrap());
    }
    let mut next = fixture.connect().await;
    status(&mut authenticated).await;
    status(&mut next).await;
    fixture.stop().await;
}

#[tokio::test]
async fn expired_authenticated_connections_release_admission_slots() {
    let fixture = Fixture::new(250).await;
    let mut clients = Vec::new();
    for _ in 0..8 {
        clients.push(fixture.connect().await);
    }
    let mut excess = TcpStream::connect(fixture.address).await.unwrap();
    write_handshake(&mut excess, 1, 1, Some("network-test-secret"), None, false)
        .await
        .unwrap();
    assert_closed(&mut excess).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    // TCP EOF is delivered by the OS reactor, not virtual time. Resume before observing it.
    tokio::time::resume();
    for client in &mut clients {
        assert_closed(client).await;
    }
    let mut recovered = fixture.connect().await;
    status(&mut recovered).await;
    fixture.stop().await;
}

#[tokio::test]
async fn idle_deadline_tracks_slow_heartbeats_without_disabling_expiry() {
    let fixture = Fixture::new(6_000).await;
    let mut stream = fixture.connect().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    tokio::time::resume();
    status(&mut stream).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(13)).await;
    tokio::time::resume();
    assert_closed(&mut stream).await;
    fixture.stop().await;
}

#[tokio::test]
async fn blocked_state_acquisition_expires_and_releases_frame_bytes() {
    let fixture = Fixture::new(250).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let state = fixture.network.state_ref.clone();
    let guard = state.write().await;
    let budget = FrameByteBudget::new(65_536);
    let request = encode(
        Operation::Status,
        &ClusterStatusRequest {
            probe_from: 1,
            config_fingerprint: None,
            supports_cancellation_safe_rpc_v1: true,
        },
    )
    .unwrap();
    write_framed(&mut client, &request).await.unwrap();
    let serving = serve_raft_stream(
        fixture.raft.clone(),
        state.clone(),
        server,
        InboundPeer {
            channel_binding: [0; 32],
            id: 1,
            replica: Some(crate::raft::types::test_replica(1)),
            epoch: None,
            supports_v2: false,
        },
        InboundStreamPolicy {
            admission: fixture.network.admission.clone(),
            secret: None,
            local_config_fingerprint: fixture.network.config_fingerprint,
            max_frame_bytes: 65_536,
            legacy_response_budget: Duration::from_millis(200),
            frame_byte_budget: budget.clone(),
            idle_timeout: Duration::from_secs(5),
            warnings: crate::warning_limit::WarningLimiter::default(),
        },
    );
    tokio::pin!(serving);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            assert!(futures::poll!(&mut serving).is_pending());
            if budget.available_bytes() < 65_536 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request must reach the blocked state read");
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    let error = tokio::time::timeout(Duration::from_millis(100), &mut serving)
        .await
        .expect("blocked state read escaped the processing deadline")
        .unwrap_err();
    assert!(
        error.to_string().contains("processing timed out"),
        "{error}"
    );
    assert_eq!(budget.available_bytes(), 65_536);
    tokio::time::resume();
    drop(guard);
    fixture.stop().await;
}

#[tokio::test]
async fn legacy_response_budget_includes_waiting_for_state() {
    let fixture = Fixture::new(250).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let state = fixture.network.state_ref.clone();
    let guard = state.write().await;
    let budget = FrameByteBudget::new(65_536);
    let request = openraft::raft::VoteRequest::<crate::raft::TypeConfig> {
        vote: openraft::alias::VoteOf::<crate::raft::TypeConfig>::new(
            1,
            crate::raft::types::test_replica(1),
        ),
        last_log_id: None,
        leadership_transfer: false,
    };
    write_framed(&mut client, &encode(Operation::Vote, &request).unwrap())
        .await
        .unwrap();
    let serving = serve_raft_stream(
        fixture.raft.clone(),
        state.clone(),
        server,
        InboundPeer {
            channel_binding: [0; 32],
            id: 1,
            replica: Some(crate::raft::types::test_replica(1)),
            epoch: None,
            supports_v2: false,
        },
        InboundStreamPolicy {
            admission: fixture.network.admission.clone(),
            secret: None,
            local_config_fingerprint: fixture.network.config_fingerprint,
            max_frame_bytes: 65_536,
            legacy_response_budget: Duration::from_millis(200),
            frame_byte_budget: budget.clone(),
            idle_timeout: Duration::from_secs(5),
            warnings: crate::warning_limit::WarningLimiter::default(),
        },
    );
    tokio::pin!(serving);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            assert!(futures::poll!(&mut serving).is_pending());
            if budget.available_bytes() < 65_536 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(250)).await;
    drop(guard);
    tokio::time::timeout(Duration::from_millis(100), &mut serving)
        .await
        .expect("late legacy response kept the stream open")
        .unwrap();
    tokio::time::resume();
    assert_closed(&mut client).await;
    fixture.stop().await;
}

#[tokio::test]
async fn session_expiry_while_waiting_for_state_prevents_inbound_raft() {
    let fixture = Fixture::new(250).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let local = crate::raft::types::test_replica(1);
    let context = fixture
        .network
        .admission
        .authorize_raft(local)
        .unwrap()
        .session
        .context()
        .clone();
    let authority = super::testing::with_deadline(
        context,
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    let state = fixture.network.state_ref.clone();
    let guard = state.write().await;
    let before = guard.vote;
    let budget = FrameByteBudget::new(65_536);
    write_framed(
        &mut client,
        &encode(Operation::Vote, &super::testing::vote_request()).unwrap(),
    )
    .await
    .unwrap();
    let serving = serve_raft_stream(
        fixture.raft.clone(),
        state.clone(),
        server,
        InboundPeer {
            id: 1,
            replica: Some(local),
            epoch: None,
            supports_v2: true,
            channel_binding: [0; 32],
        },
        InboundStreamPolicy {
            admission: authority,
            secret: None,
            local_config_fingerprint: fixture.network.config_fingerprint,
            max_frame_bytes: 65_536,
            legacy_response_budget: Duration::from_secs(5),
            frame_byte_budget: budget.clone(),
            idle_timeout: Duration::from_secs(5),
            warnings: crate::warning_limit::WarningLimiter::default(),
        },
    );
    tokio::pin!(serving);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            assert!(futures::poll!(&mut serving).is_pending());
            if budget.available_bytes() < 65_536 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    drop(guard);
    tokio::time::timeout(Duration::from_millis(100), &mut serving)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.read().await.vote, before);
    assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
    tokio::time::resume();
    fixture.stop().await;
}
