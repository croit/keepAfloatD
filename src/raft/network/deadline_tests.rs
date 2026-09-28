//! Regression coverage for the inbound deadlines lost during the #26 rebase.

use super::inbound::{InboundPeer, InboundStreamPolicy, serve_raft_stream};
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
        let mut config = (*super::tests::test_network(65_536, &[1]).config).clone();
        config.raft_listen = address.to_string();
        config.peers[0].raft_address = address.to_string();
        config.raft.heartbeat_interval_ms = heartbeat_ms;
        let (log, machine, state) = new_store(Arc::new(Vec::new()), 3, true, 0);
        let network = RaftNetworkImpl::new(Arc::new(config), state).unwrap();
        let raft = KafRaft::new(
            1,
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
        write_handshake(&mut stream, 1, Some("network-test-secret"), None, false)
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
async fn fixture_adopts_bound_listener_and_closes_accepted_streams_on_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    // Establish before construction: closing and rebinding cannot preserve this queued stream.
    let mut stream = TcpStream::connect(address).await.unwrap();
    let fixture = Fixture::from_listener(250, listener).await;
    assert_eq!(fixture.address, address);
    write_handshake(&mut stream, 1, Some("network-test-secret"), None, false)
        .await
        .unwrap();
    status(&mut stream).await;
    fixture.stop().await;
    assert_closed(&mut stream).await;
}

async fn status(stream: &mut TcpStream) {
    let body = serde_json::to_vec(&ClusterStatusRequest {
        probe_from: 1,
        config_fingerprint: None,
        supports_cancellation_safe_rpc_v1: true,
    })
    .unwrap();
    write_framed(stream, &body).await.unwrap();
    let response =
        tokio::time::timeout(Duration::from_secs(1), read_framed_bounded(stream, 65_536))
            .await
            .unwrap()
            .unwrap();
    let _: ClusterStatusResponse = serde_json::from_slice(&response).unwrap();
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
    write_handshake(&mut stream, 999, Some("network-test-secret"), None, false)
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
    write_handshake(&mut stream, 1, Some("incorrect-secret"), None, false)
        .await
        .unwrap();
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
async fn expired_authenticated_connections_release_admission_slots() {
    let fixture = Fixture::new(250).await;
    let mut clients = Vec::new();
    for _ in 0..8 {
        clients.push(fixture.connect().await);
    }
    let mut excess = TcpStream::connect(fixture.address).await.unwrap();
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
    let request = serde_json::to_vec(&ClusterStatusRequest {
        probe_from: 1,
        config_fingerprint: None,
        supports_cancellation_safe_rpc_v1: true,
    })
    .unwrap();
    write_framed(&mut client, &request).await.unwrap();
    let serving = serve_raft_stream(
        fixture.raft.clone(),
        state.clone(),
        server,
        InboundPeer {
            id: 1,
            epoch: None,
            supports_v2: false,
        },
        InboundStreamPolicy {
            local_config_fingerprint: fixture.network.config_fingerprint,
            max_frame_bytes: 65_536,
            legacy_response_budget: Duration::from_millis(200),
            frame_byte_budget: budget.clone(),
            idle_timeout: Duration::from_secs(5),
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
        vote: openraft::alias::VoteOf::<crate::raft::TypeConfig>::new(1, 1),
        last_log_id: None,
        leadership_transfer: false,
    };
    write_framed(&mut client, &serde_json::to_vec(&request).unwrap())
        .await
        .unwrap();
    let serving = serve_raft_stream(
        fixture.raft.clone(),
        state.clone(),
        server,
        InboundPeer {
            id: 1,
            epoch: None,
            supports_v2: false,
        },
        InboundStreamPolicy {
            local_config_fingerprint: fixture.network.config_fingerprint,
            max_frame_bytes: 65_536,
            legacy_response_budget: Duration::from_millis(200),
            frame_byte_budget: budget.clone(),
            idle_timeout: Duration::from_secs(5),
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
