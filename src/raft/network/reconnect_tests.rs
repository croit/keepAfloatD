//! Peer-close recovery over real loopback sockets, without changing host interfaces.

use super::wire::{read_handshake, write_framed};
use super::*;
use crate::raft::probe::{ClusterStatusRequest, ClusterStatusResponse};
use crate::raft::store::new_store;
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

async fn fixture() -> (RaftNetworkImpl, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = (*super::tests::test_network(65_536, &[1, 2]).config).clone();
    config.raft_listen = "127.0.0.1:0".into();
    config.peers[0].raft_address = config.raft_listen.clone();
    config.peers[1].raft_address = listener.local_addr().unwrap().to_string();
    let (_, _, state) = new_store(Arc::new(Vec::new()), 3, true, 0);
    (
        RaftNetworkImpl::new(
            Arc::new(config.clone()),
            state,
            crate::raft::network::testing::controller(&config),
        )
        .unwrap(),
        listener,
    )
}

async fn attach_cached(network: &RaftNetworkImpl, listener: &TcpListener) -> TcpStream {
    let stream = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (remote, _) = listener.accept().await.unwrap();
    *network.peers[&2].stream.lock().await = Some(stream);
    *network.peers[&2].remote_replica.write().unwrap() = Some(crate::raft::types::test_replica(2));
    remote
}

async fn accept_preflight(
    listener: &TcpListener,
    fingerprint: ClusterConfigFingerprint,
) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    let (id, _, _) = read_handshake(&mut stream, 2, Some("network-test-secret"))
        .await
        .unwrap();
    assert_eq!(id, 1);
    let body = read_framed_bounded(&mut stream, 65_536).await.unwrap();
    let request: ClusterStatusRequest = decode_payload(&body, Operation::Status).unwrap();
    assert_eq!(request.config_fingerprint, Some(fingerprint));
    let response = ClusterStatusResponse {
        config_fingerprint: Some(fingerprint),
        supports_config_identity_v1: true,
        ..ClusterStatusResponse::default()
    };
    write_framed(&mut stream, &encode(Operation::Status, &response).unwrap())
        .await
        .unwrap();
    stream
}

#[tokio::test]
async fn first_vote_rpc_recovers_from_a_closed_cached_stream() {
    let (network, listener) = fixture().await;
    let mut old = attach_cached(&network, &listener).await;
    old.shutdown().await.unwrap();
    drop(old);
    let fingerprint = network.config_fingerprint;
    // Lifetime: one replacement connection; aborted if the regression returns early.
    let server = tokio::spawn(async move {
        let mut stream = accept_preflight(&listener, fingerprint).await;
        let body = read_framed_bounded(&mut stream, 65_536).await.unwrap();
        assert_eq!(
            decode_payload::<Value>(&body, Operation::Vote).unwrap(),
            serde_json::to_value(super::testing::vote_request()).unwrap()
        );
        write_framed(
            &mut stream,
            &encode(Operation::Vote, &super::testing::vote_response(true)).unwrap(),
        )
        .await
        .unwrap();
    });
    let result = network
        .send_rpc::<_, Value>(
            crate::raft::types::test_replica(2),
            &super::testing::vote_request(),
            RPCTypes::Vote,
            Duration::from_secs(1),
        )
        .await;
    if result.is_err() {
        server.abort();
        let _ = server.await;
    } else {
        server.await.unwrap();
    }
    assert_eq!(
        result.expect("the first vote must reconnect, not fail with EOF"),
        serde_json::to_value(super::testing::vote_response(true)).unwrap()
    );
}

#[tokio::test]
async fn reconnect_worker_replaces_a_peer_closed_idle_stream_without_rpc() {
    let (network, listener) = fixture().await;
    let (log, machine, _) = new_store(Arc::new(Vec::new()), 3, true, 0);
    let raft = KafRaft::new(
        crate::raft::types::test_replica(1),
        Arc::new(openraft::Config::default()),
        network.clone(),
        log,
        machine,
    )
    .await
    .unwrap();
    let _failures = network.start(raft.clone()).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let mut original = accept_preflight(&listener, network.config_fingerprint).await;
        original.shutdown().await.unwrap();
        drop(original);
        let _replacement = accept_preflight(&listener, network.config_fingerprint).await;
    })
    .await;
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    result.expect("idle peer close must trigger reconnection without an RPC");
}

fn vote(network: &RaftNetworkImpl) -> tokio::task::JoinHandle<Result<Value, RPCError<TypeConfig>>> {
    let network = network.clone();
    // Lifetime: one bounded RPC; callers join or abort it before returning.
    tokio::spawn(async move {
        network
            .send_rpc(
                crate::raft::types::test_replica(2),
                &super::testing::vote_request(),
                RPCTypes::Vote,
                Duration::from_secs(1),
            )
            .await
    })
}

async fn without_clock_advance<F: std::future::Future>(future: F) -> F::Output {
    let watchdog = std::time::Instant::now();
    tokio::pin!(future);
    // Keep the runtime runnable so paused time cannot advance before loopback I/O is ready.
    loop {
        assert!(
            watchdog.elapsed() < Duration::from_secs(5),
            "paused-clock fixture exceeded its wall-clock watchdog"
        );
        tokio::select! {
            biased;
            result = &mut future => return result,
            _ = tokio::task::yield_now() => {}
        }
    }
}

async fn start_background(network: &RaftNetworkImpl) -> (KafRaft, mpsc::UnboundedReceiver<String>) {
    let (log, machine, _) = new_store(Arc::new(Vec::new()), 3, true, 0);
    let raft = KafRaft::new(
        crate::raft::types::test_replica(1),
        Arc::new(openraft::Config {
            enable_tick: false,
            ..openraft::Config::default()
        }),
        network.clone(),
        log,
        machine,
    )
    .await
    .unwrap();
    let failures = network.start(raft.clone()).await.unwrap();
    (raft, failures)
}

async fn closed_preflight<'a>(
    network: &'a RaftNetworkImpl,
    listener: &TcpListener,
) -> tokio::sync::MutexGuard<'a, Option<TcpStream>> {
    let (mut remote, _) = listener.accept().await.unwrap();
    read_handshake(&mut remote, 2, Some("network-test-secret"))
        .await
        .unwrap();
    let body = read_framed_bounded(&mut remote, 65_536).await.unwrap();
    let request: ClusterStatusRequest = decode_payload(&body, Operation::Status).unwrap();
    assert_eq!(request.config_fingerprint, Some(network.config_fingerprint));
    let published = network.peers[&2].stream.lock();
    tokio::pin!(published);
    assert!(futures::poll!(&mut published).is_pending());
    let response = ClusterStatusResponse {
        config_fingerprint: Some(network.config_fingerprint),
        supports_config_identity_v1: true,
        ..ClusterStatusResponse::default()
    };
    write_framed(&mut remote, &encode(Operation::Status, &response).unwrap())
        .await
        .unwrap();
    remote.shutdown().await.unwrap();
    drop(remote);
    let cached = published.await;
    let stream = cached
        .as_ref()
        .expect("successful preflight must publish a stream");
    assert_eq!(stream.peek(&mut [0; 1]).await.unwrap(), 0);
    cached
}

#[tokio::test(start_paused = true)]
async fn background_reconnects_are_paced_after_successful_peer_close() {
    without_clock_advance(async {
        let (network, listener) = fixture().await;
        let (raft, mut failures) = start_background(&network).await;
        let started = tokio::time::Instant::now();
        for attempt in 0..3 {
            let cached = closed_preflight(&network, &listener).await;
            assert_eq!(started.elapsed(), RECONNECT_RETRY_INTERVAL * attempt);
            let available = network.peers[&2].stream.lock();
            tokio::pin!(available);
            assert!(futures::poll!(&mut available).is_pending());
            drop(cached);
            tokio::select! {
                biased;
                guard = &mut available => {
                    assert!(guard.is_some(), "pacing must leave the published stream available");
                }
                accepted = listener.accept() => {
                    accepted.unwrap();
                    panic!("background reconnected before the retry interval after successful preflight");
                }
            }
            if attempt < 2 {
                let next = listener.accept();
                tokio::pin!(next);
                assert!(futures::poll!(&mut next).is_pending());
                tokio::time::advance(RECONNECT_RETRY_INTERVAL - Duration::from_millis(1)).await;
                assert!(futures::poll!(&mut next).is_pending());
                tokio::time::advance(Duration::from_millis(1)).await;
            }
        }
        let stopped = tokio::time::Instant::now();
        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
        assert_eq!(stopped.elapsed(), Duration::ZERO, "shutdown must cancel pacing");
        assert!(failures.try_recv().is_err());
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn foreground_retry_does_not_wait_for_background_pacing() {
    without_clock_advance(async {
        let (network, listener) = fixture().await;
        let (raft, mut failures) = start_background(&network).await;
        let started = tokio::time::Instant::now();
        drop(closed_preflight(&network, &listener).await);
        let rpc = vote(&network);
        let mut replacement = accept_preflight(&listener, network.config_fingerprint).await;
        let request = read_framed_bounded(&mut replacement, 65_536).await.unwrap();
        assert_eq!(
            decode_payload::<Value>(&request, Operation::Vote).unwrap(),
            serde_json::to_value(super::testing::vote_request()).unwrap()
        );
        write_framed(
            &mut replacement,
            &encode(Operation::Vote, &super::testing::vote_response(true)).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            rpc.await.unwrap().unwrap(),
            serde_json::to_value(super::testing::vote_response(true)).unwrap()
        );
        assert_eq!(started.elapsed(), Duration::ZERO);

        tokio::time::advance(RECONNECT_RETRY_INTERVAL).await;
        let next = vote(&network);
        assert_eq!(
            read_framed_bounded(&mut replacement, 65_536).await.unwrap(),
            request
        );
        write_framed(
            &mut replacement,
            &encode(Operation::Vote, &super::testing::vote_response(false)).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            next.await.unwrap().unwrap(),
            serde_json::to_value(super::testing::vote_response(false)).unwrap()
        );
        assert_eq!(started.elapsed(), RECONNECT_RETRY_INTERVAL);

        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
        assert!(failures.try_recv().is_err());
    })
    .await;
}

#[tokio::test]
async fn disconnect_after_request_retries_once_and_reuses_the_replacement() {
    let (network, listener) = fixture().await;
    let mut old = attach_cached(&network, &listener).await;
    let rpc = vote(&network);
    let original = read_framed_bounded(&mut old, 65_536).await.unwrap();
    drop(old);
    let mut fresh = accept_preflight(&listener, network.config_fingerprint).await;
    assert_eq!(
        read_framed_bounded(&mut fresh, 65_536).await.unwrap(),
        original
    );
    write_framed(
        &mut fresh,
        &encode(Operation::Vote, &super::testing::vote_response(true)).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        rpc.await.unwrap().unwrap(),
        serde_json::to_value(super::testing::vote_response(true)).unwrap()
    );
    let next = vote(&network);
    assert_eq!(
        read_framed_bounded(&mut fresh, 65_536).await.unwrap(),
        original
    );
    write_framed(
        &mut fresh,
        &encode(Operation::Vote, &super::testing::vote_response(false)).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        next.await.unwrap().unwrap(),
        serde_json::to_value(super::testing::vote_response(false)).unwrap()
    );
}

#[tokio::test]
async fn fresh_stream_disconnect_is_not_retried_again() {
    let (network, listener) = fixture().await;
    drop(attach_cached(&network, &listener).await);
    let rpc = vote(&network);
    let mut fresh = accept_preflight(&listener, network.config_fingerprint).await;
    read_framed_bounded(&mut fresh, 65_536).await.unwrap();
    drop(fresh);
    assert!(matches!(rpc.await.unwrap(), Err(RPCError::Network(_))));
    assert!(network.peers[&2].stream.lock().await.is_none());
}

#[tokio::test]
async fn retry_preflight_preserves_configuration_and_epoch_fences() {
    for foreign_epoch in [false, true] {
        let (network, listener) = fixture().await;
        network.state_ref.write().await.cluster_epoch = Some(1);
        drop(attach_cached(&network, &listener).await);
        let rpc = vote(&network);
        let (mut fresh, _) = listener.accept().await.unwrap();
        let (_, epoch, _) = read_handshake(&mut fresh, 2, Some("network-test-secret"))
            .await
            .unwrap();
        assert_eq!(epoch, Some(1));
        read_framed_bounded(&mut fresh, 65_536).await.unwrap();
        let mut response = ClusterStatusResponse {
            config_fingerprint: Some(network.config_fingerprint),
            supports_config_identity_v1: true,
            cluster_epoch: Some(1),
            ..ClusterStatusResponse::default()
        };
        if foreign_epoch {
            response.cluster_epoch = Some(2);
        } else {
            let mut other_config = (*network.config).clone();
            other_config.failover_delay_secs += 1;
            response.config_fingerprint = Some(other_config.cluster_config_fingerprint().unwrap());
        }
        write_framed(&mut fresh, &encode(Operation::Status, &response).unwrap())
            .await
            .unwrap();
        let err = rpc.await.unwrap().unwrap_err().to_string();
        assert!(
            err.contains(if foreign_epoch {
                "cluster_epoch mismatch"
            } else {
                "configuration identity mismatch"
            }),
            "{err}"
        );
        assert_eq!(
            fresh.read(&mut [0; 1]).await.unwrap(),
            0,
            "no Raft request after a failed preflight"
        );
        assert!(network.peers[&2].stream.lock().await.is_none());
    }
}

#[tokio::test]
async fn every_retry_requires_an_exact_boot_and_status_preflight() {
    let (network, listener) = fixture().await;
    drop(attach_cached(&network, &listener).await);
    let rpc = vote(&network);
    let mut fresh = accept_preflight(&listener, network.config_fingerprint()).await;
    let body = read_framed_bounded(&mut fresh, 65_536).await.unwrap();
    assert_eq!(
        decode_payload::<Value>(&body, Operation::Vote).unwrap(),
        serde_json::to_value(super::testing::vote_request()).unwrap()
    );
    write_framed(
        &mut fresh,
        &encode(Operation::Vote, &super::testing::vote_response(true)).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        rpc.await.unwrap().unwrap(),
        serde_json::to_value(super::testing::vote_response(true)).unwrap()
    );
}

#[tokio::test]
async fn activation_during_preflight_discards_the_connection() {
    for activate_identity in [false, true] {
        let (network, listener) = fixture().await;
        drop(attach_cached(&network, &listener).await);
        let rpc = vote(&network);
        let (mut fresh, _) = listener.accept().await.unwrap();
        read_handshake(&mut fresh, 2, Some("network-test-secret"))
            .await
            .unwrap();
        read_framed_bounded(&mut fresh, 65_536).await.unwrap();
        if activate_identity {
            network.state_ref.write().await.config_identity_enforced = true;
        } else {
            network.state_ref.write().await.failover_semantics = FailoverSemantics::V2;
        }
        write_framed(
            &mut fresh,
            &encode(Operation::Status, &ClusterStatusResponse::default()).unwrap(),
        )
        .await
        .unwrap();
        assert!(
            rpc.await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("policy changed")
        );
        assert_eq!(fresh.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(network.peers[&2].stream.lock().await.is_none());
    }
}

#[tokio::test]
async fn reconnect_timeout_and_cancellation_clear_the_stream_slot() {
    for cancel in [false, true] {
        let (network, listener) = fixture().await;
        drop(attach_cached(&network, &listener).await);
        let rpc = vote(&network);
        let (mut fresh, _) = listener.accept().await.unwrap();
        read_handshake(&mut fresh, 2, Some("network-test-secret"))
            .await
            .unwrap();
        read_framed_bounded(&mut fresh, 65_536).await.unwrap();
        if cancel {
            rpc.abort();
            assert!(rpc.await.unwrap_err().is_cancelled());
        } else {
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(matches!(rpc.await.unwrap(), Err(RPCError::Timeout(_))));
            tokio::time::resume();
        }
        assert!(network.peers[&2].stream.lock().await.is_none());
        assert_eq!(fresh.read(&mut [0; 1]).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn retry_does_not_reset_the_original_rpc_deadline() {
    let (network, listener) = fixture().await;
    let mut old = attach_cached(&network, &listener).await;
    let rpc = vote(&network);
    read_framed_bounded(&mut old, 65_536).await.unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(600)).await;
    tokio::time::resume();
    drop(old);
    let (mut fresh, _) = listener.accept().await.unwrap();
    read_handshake(&mut fresh, 2, Some("network-test-secret"))
        .await
        .unwrap();
    read_framed_bounded(&mut fresh, 65_536).await.unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(401)).await;
    let result = tokio::time::timeout(Duration::ZERO, rpc)
        .await
        .expect("the original one-second deadline must not restart at reconnect");
    assert!(matches!(result.unwrap(), Err(RPCError::Timeout(_))));
    assert!(network.peers[&2].stream.lock().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn rpc_deadline_includes_waiting_for_the_peer_lock() {
    let (network, _listener) = fixture().await;
    let guard = network.peers[&2].stream.lock().await;
    let (started, ready) = oneshot::channel();
    let clone = network.clone();
    // Lifetime: deadline-bound request queued behind another owner of this peer.
    let rpc = tokio::spawn(async move {
        started.send(()).unwrap();
        clone
            .send_rpc::<_, Value>(
                crate::raft::types::test_replica(2),
                &super::testing::vote_request(),
                RPCTypes::Vote,
                Duration::from_secs(1),
            )
            .await
    });
    ready.await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(rpc.await.unwrap(), Err(RPCError::Timeout(_))));
    drop(guard);
}

#[tokio::test]
async fn idle_probe_retains_live_channels_and_rejects_unsolicited_bytes() {
    let (network, listener) = fixture().await;
    let mut remote = attach_cached(&network, &listener).await;
    let slot = network.peers[&2].stream.lock().await;
    let stream = slot.as_ref().unwrap();
    assert!(idle_stream_is_usable(stream));
    remote.write_all(b"unexpected").await.unwrap();
    stream.readable().await.unwrap();
    assert!(!idle_stream_is_usable(stream));
}

#[tokio::test(start_paused = true)]
async fn background_reconnect_replaces_outdated_streams_without_peer_close() {
    without_clock_advance(async {
        for activate_identity in [false, true] {
            let (network, listener) = fixture().await;
            let (raft, mut failures) = start_background(&network).await;
            let mut original = accept_preflight(&listener, network.config_fingerprint).await;
            let cached = network.peers[&2].stream.lock().await;
            assert!(cached.is_some());
            if activate_identity {
                network.peers[&2]
                    .advertises_config_identity
                    .store(false, Ordering::SeqCst);
                network.state_ref.write().await.config_identity_enforced = true;
            } else {
                network.state_ref.write().await.failover_semantics = FailoverSemantics::V2;
            }
            drop(cached);
            tokio::time::advance(RECONNECT_RETRY_INTERVAL).await;
            let _replacement = accept_preflight(&listener, network.config_fingerprint).await;
            assert_eq!(original.read(&mut [0; 1]).await.unwrap(), 0);
            let cached = network.peers[&2].stream.lock().await;
            assert!(cached.is_some());
            assert!(
                network.peers[&2]
                    .advertises_config_identity
                    .load(Ordering::SeqCst)
            );
            if !activate_identity {
                assert!(network.peers[&2].advertises_v2.load(Ordering::SeqCst));
            }
            drop(cached);
            network.shutdown().await.unwrap();
            raft.shutdown().await.unwrap();
            assert!(failures.try_recv().is_err());
        }
    })
    .await;
}

#[test]
fn only_disconnect_errors_are_retryable() {
    use std::io::ErrorKind::*;
    for kind in [
        UnexpectedEof,
        ConnectionReset,
        ConnectionAborted,
        BrokenPipe,
        NotConnected,
        WriteZero,
    ] {
        assert!(is_disconnect(&kind.into()));
    }
    for kind in [InvalidData, TimedOut, PermissionDenied, WouldBlock, Other] {
        assert!(!is_disconnect(&kind.into()));
    }
}
