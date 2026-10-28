use super::*;
use crate::auth::{Listener, Peer};
use crate::config::PeerConfig;
use crate::raft::probe::{ClusterStatusRequest, ClusterStatusResponse};
use crate::raft::store::new_store;
use crate::raft::types::test_replica;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Reply {
    Matching,
    ForeignFingerprint,
    MissingFingerprint,
    MissingBoot,
    ForeignFingerprintMissingBoot,
    ForeignGenesis,
    UnconfiguredGenesis,
    MismatchedPhysical,
    WrongSecret,
    InvalidStatusTag,
}

async fn read_payload<T: DeserializeOwned>(stream: &mut TcpStream, tag: &str) -> T {
    let (bytes, ()) = crate::frame::read(stream, 64 * 1024, |_| Ok(()))
        .await
        .unwrap();
    let mut envelope: BTreeMap<String, T> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(envelope.len(), 1);
    envelope.remove(tag).expect("expected protocol operation")
}

async fn write_payload<T: Serialize>(stream: &mut TcpStream, tag: &str, payload: T) {
    let bytes = serde_json::to_vec(&BTreeMap::from([(tag, payload)])).unwrap();
    let mut frame = u32::try_from(bytes.len()).unwrap().to_be_bytes().to_vec();
    frame.extend(bytes);
    stream.write_all(&frame).await.unwrap();
}

async fn discover_once(reply: Reply) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = (*super::tests::config()).clone();
    cfg.peers.push(PeerConfig {
        id: 2,
        raft_address: listener.local_addr().unwrap().to_string(),
        client_submit_address: "127.0.0.1:2001".into(),
    });
    let cfg = Arc::new(cfg);
    let fingerprint = cfg.cluster_config_fingerprint().unwrap();
    let mut foreign = fingerprint;
    foreign.digest[0] ^= 1;
    let (_, _, state) = new_store(Arc::new(vec![]), 3, true, 0);
    let runtime = RuntimeDriver::new(
        cfg.clone(),
        state.clone(),
        LeaseTiming::new(Duration::from_secs(5), Duration::from_secs(1)).unwrap(),
        Instant::now(),
    )
    .unwrap();
    let network = RaftNetworkImpl::new(cfg.clone(), state.clone(), runtime.clone()).unwrap();
    let local = runtime.local_replica();
    let peer = test_replica(2);
    let mut genesis = Genesis {
        config: fingerprint,
        epoch: 17,
        voters: [local, peer].into(),
    };
    if reply == Reply::ForeignGenesis {
        genesis.config = foreign;
    }
    if reply == Reply::UnconfiguredGenesis {
        genesis.voters = [local, test_replica(3)].into();
    }
    let expected_genesis = genesis.clone();
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let server = async {
        let mut operations = Vec::new();
        loop {
            let (mut stream, _) = tokio::select! {
                _ = &mut stop_rx => break,
                accepted = listener.accept() => accepted.unwrap(),
            };
            let advertised = if reply == Reply::MismatchedPhysical {
                test_replica(3)
            } else {
                peer
            };
            let authenticated = crate::auth::server_bound(
                &mut stream,
                if matches!(
                    reply,
                    Reply::MissingBoot | Reply::ForeignFingerprintMissingBoot
                ) {
                    Peer::new(advertised.physical_id, None, true)
                } else {
                    Peer::for_replica(advertised, None, true)
                },
                if reply == Reply::WrongSecret {
                    Some("different-discovery-test-secret-0123456789")
                } else {
                    cfg.cluster_secret.as_deref()
                },
                Listener::Raft,
            )
            .await;
            if reply == Reply::WrongSecret {
                assert!(authenticated.is_err());
                operations.push("authentication_rejected");
                continue;
            }
            if reply == Reply::MismatchedPhysical {
                let error = authenticated.unwrap_err();
                assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
                assert_eq!(error.to_string(), "authentication destination mismatch");
                operations.push("identity_rejected");
                continue;
            }
            let authenticated = authenticated.unwrap();
            assert_eq!(authenticated.peer.id, local.physical_id);
            match operations.len() {
                0 => {
                    let request: ClusterStatusRequest = read_payload(&mut stream, "status").await;
                    assert_eq!(request.probe_from, local.physical_id);
                    assert_eq!(request.config_fingerprint, Some(fingerprint));
                    write_payload(
                        &mut stream,
                        if reply == Reply::InvalidStatusTag {
                            "vote"
                        } else {
                            "status"
                        },
                        ClusterStatusResponse {
                            config_fingerprint: (reply != Reply::MissingFingerprint).then_some(
                                if matches!(
                                    reply,
                                    Reply::ForeignFingerprint
                                        | Reply::ForeignFingerprintMissingBoot
                                ) {
                                    foreign
                                } else {
                                    fingerprint
                                },
                            ),
                            supports_config_identity_v1: true,
                            ..Default::default()
                        },
                    )
                    .await;
                    operations.push("status");
                }
                1 => {
                    assert_eq!(authenticated.peer.replica(), Some(local));
                    let request: SignedAdmission<ManagementRequest> =
                        read_payload(&mut stream, "admission_control").await;
                    request
                        .verify(
                            cfg.cluster_secret.as_deref(),
                            REQUEST_ROLE,
                            local,
                            peer,
                            authenticated.binding,
                        )
                        .unwrap();
                    assert_eq!(request.payload.action, ManagementAction::Discover);
                    let response = SignedAdmission::sign(
                        cfg.cluster_secret.as_deref(),
                        RESPONSE_ROLE,
                        peer,
                        local,
                        authenticated.binding,
                        ManagementResponse {
                            nonce: request.payload.nonce,
                            result: ManagementResult::Discovery {
                                genesis: Some(genesis.clone()),
                                voters: genesis.voters.clone(),
                            },
                        },
                    )
                    .unwrap();
                    write_payload(&mut stream, "admission_control", response).await;
                    operations.push("discovery");
                }
                _ => panic!("single-pass discovery sent an extra request"),
            }
        }
        operations
    };
    let client = async {
        let result = runtime
            .discover_for_health(&network, Duration::from_millis(500))
            .await;
        stop_tx.send(()).unwrap();
        result
    };
    let (operations, discovery) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(server, client)
    })
    .await
    .expect("single discovery pass must finish");
    let expected_operations = match reply {
        Reply::MismatchedPhysical => vec!["identity_rejected"],
        Reply::WrongSecret => vec!["authentication_rejected"],
        Reply::ForeignFingerprint
        | Reply::MissingFingerprint
        | Reply::MissingBoot
        | Reply::ForeignFingerprintMissingBoot
        | Reply::InvalidStatusTag => vec!["status"],
        _ => vec!["status", "discovery"],
    };
    assert_eq!(operations, expected_operations);
    if reply == Reply::Matching {
        let discovery = discovery.unwrap();
        assert_eq!(discovery.boots, [local, peer].into());
        assert_eq!(discovery.histories, [(peer, expected_genesis)].into());
    } else if matches!(
        reply,
        Reply::MismatchedPhysical | Reply::WrongSecret | Reply::ForeignFingerprint
    ) {
        let discovery = discovery.unwrap();
        assert_eq!(discovery.boots, [local].into());
        assert!(discovery.histories.is_empty());
    } else {
        let error = discovery.err().expect(
            "observed configuration or genesis rejection must survive discovery aggregation",
        );
        assert_eq!(
            crate::consensus_freshness::failure_kind(&error),
            crate::consensus_freshness::FailureKind::Rejected
        );
    }
    {
        let state = state.read().await;
        assert!(state.admission.is_none());
        assert!(state.genesis.is_none());
        assert!(state.last_applied_log.is_none());
    }
    assert!(runtime.current().is_none());
    assert!(runtime.authorize_raft(peer).is_err());
    assert!(runtime.first_verified_admission.lock().unwrap().is_none());
    network.shutdown().await.unwrap();
    runtime.shutdown();
}

#[tokio::test]
async fn single_pass_accepts_matching_signed_discovery_without_admission() {
    discover_once(Reply::Matching).await;
}

#[tokio::test]
async fn single_pass_excludes_foreign_status_fingerprint_before_management() {
    discover_once(Reply::ForeignFingerprint).await;
}

#[tokio::test]
async fn single_pass_rejects_missing_status_fingerprint_before_management() {
    discover_once(Reply::MissingFingerprint).await;
}

#[tokio::test]
async fn single_pass_rejects_missing_authenticated_boot_before_management() {
    discover_once(Reply::MissingBoot).await;
}

#[tokio::test]
async fn foreign_fingerprint_does_not_mask_a_missing_authenticated_boot() {
    discover_once(Reply::ForeignFingerprintMissingBoot).await;
}

#[tokio::test]
async fn single_pass_rejects_signed_foreign_genesis_configuration() {
    discover_once(Reply::ForeignGenesis).await;
}

#[tokio::test]
async fn single_pass_rejects_signed_genesis_with_unconfigured_physical_member() {
    discover_once(Reply::UnconfiguredGenesis).await;
}

#[tokio::test]
async fn single_pass_rejects_peer_with_mismatched_physical_identity() {
    discover_once(Reply::MismatchedPhysical).await;
}

#[tokio::test]
async fn wrong_key_discovery_excludes_peer_without_rejecting_healthy_peers() {
    discover_once(Reply::WrongSecret).await;
}

#[tokio::test]
async fn authenticated_invalid_status_is_still_a_rejection() {
    discover_once(Reply::InvalidStatusTag).await;
}

struct OrderedCandidate {
    listener: TcpListener,
    replica: ReplicaId,
    foreign: bool,
    gate: Option<oneshot::Receiver<()>>,
    completed: Option<oneshot::Sender<()>>,
}

impl OrderedCandidate {
    async fn serve(
        mut self,
        cfg: Arc<Config>,
        local: ReplicaId,
        genesis: Genesis,
        stop: oneshot::Receiver<()>,
    ) -> Vec<&'static str> {
        let mut operations = Vec::new();
        let work = async {
            if let Some(gate) = self.gate.take() {
                gate.await.unwrap();
            }
            loop {
                let (mut stream, _) = self.listener.accept().await.unwrap();
                let authenticated = crate::auth::server_bound(
                    &mut stream,
                    Peer::for_replica(self.replica, None, true),
                    cfg.cluster_secret.as_deref(),
                    Listener::Raft,
                )
                .await
                .unwrap();
                if operations.is_empty() {
                    let request: ClusterStatusRequest = read_payload(&mut stream, "status").await;
                    assert_eq!(request.config_fingerprint, Some(genesis.config));
                    let mut fingerprint = genesis.config;
                    if self.foreign {
                        fingerprint.digest[0] ^= 1;
                    }
                    write_payload(
                        &mut stream,
                        "status",
                        ClusterStatusResponse {
                            config_fingerprint: Some(fingerprint),
                            supports_config_identity_v1: true,
                            ..Default::default()
                        },
                    )
                    .await;
                    operations.push("status");
                } else {
                    assert!(!self.foreign, "foreign candidate received management RPC");
                    assert_eq!(operations, ["status"]);
                    let request: SignedAdmission<ManagementRequest> =
                        read_payload(&mut stream, "admission_control").await;
                    request
                        .verify(
                            cfg.cluster_secret.as_deref(),
                            REQUEST_ROLE,
                            local,
                            self.replica,
                            authenticated.binding,
                        )
                        .unwrap();
                    assert_eq!(request.payload.action, ManagementAction::Discover);
                    let response = SignedAdmission::sign(
                        cfg.cluster_secret.as_deref(),
                        RESPONSE_ROLE,
                        self.replica,
                        local,
                        authenticated.binding,
                        ManagementResponse {
                            nonce: request.payload.nonce,
                            result: ManagementResult::Discovery {
                                genesis: Some(genesis.clone()),
                                voters: genesis.voters.clone(),
                            },
                        },
                    )
                    .unwrap();
                    write_payload(&mut stream, "admission_control", response).await;
                    operations.push("discovery");
                }
                assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
                if (self.foreign || operations.len() == 2)
                    && let Some(completed) = self.completed.take()
                {
                    // Rejection may already have cancelled the remaining discovery candidates.
                    let _ = completed.send(());
                }
            }
        };
        tokio::select! {
            _ = work => unreachable!(),
            _ = stop => {}
        }
        operations
    }
}

async fn matching_discovery_in_order(foreign_first: bool) {
    let matching = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let foreign = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = (*super::tests::config()).clone();
    for (id, listener) in [(2, &matching), (3, &foreign)] {
        cfg.peers.push(PeerConfig {
            id,
            raft_address: listener.local_addr().unwrap().to_string(),
            client_submit_address: format!("127.0.0.1:{}", 2000 + id),
        });
    }
    let cfg = Arc::new(cfg);
    let (_, _, state) = new_store(Arc::new(vec![]), 3, true, 0);
    let runtime = RuntimeDriver::new(
        cfg.clone(),
        state.clone(),
        LeaseTiming::new(Duration::from_secs(5), Duration::from_secs(1)).unwrap(),
        Instant::now(),
    )
    .unwrap();
    let network = RaftNetworkImpl::new(cfg.clone(), state.clone(), runtime.clone()).unwrap();
    let local = runtime.local_replica();
    let matching_boot = test_replica(2);
    let genesis = Genesis {
        config: cfg.cluster_config_fingerprint().unwrap(),
        epoch: 17,
        voters: [local, matching_boot].into(),
    };
    let (done, gate) = oneshot::channel();
    let mut matching = OrderedCandidate {
        listener: matching,
        replica: matching_boot,
        foreign: false,
        gate: None,
        completed: None,
    };
    let mut foreign = OrderedCandidate {
        listener: foreign,
        replica: test_replica(3),
        foreign: true,
        gate: None,
        completed: None,
    };
    if foreign_first {
        foreign.completed = Some(done);
        matching.gate = Some(gate);
    } else {
        matching.completed = Some(done);
        foreign.gate = Some(gate);
    }
    let (stop_matching, matching_stopped) = oneshot::channel();
    let (stop_foreign, foreign_stopped) = oneshot::channel();
    let client = async {
        let result = runtime
            .discover_for_health(&network, Duration::from_millis(500))
            .await;
        stop_matching.send(()).unwrap();
        stop_foreign.send(()).unwrap();
        result
    };
    let (matching_ops, foreign_ops, result) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            matching.serve(cfg.clone(), local, genesis.clone(), matching_stopped),
            foreign.serve(cfg.clone(), local, genesis.clone(), foreign_stopped),
            client,
        )
    })
    .await
    .expect("ordered discovery must finish");
    let discovery = result.expect("a concrete foreign candidate cannot veto matching discovery");
    assert_eq!(matching_ops, ["status", "discovery"]);
    assert_eq!(foreign_ops, ["status"]);
    assert_eq!(discovery.boots, [local, matching_boot].into());
    assert_eq!(discovery.histories, [(matching_boot, genesis)].into());
    assert!(runtime.current().is_none());
    assert!(state.read().await.admission.is_none());
    network.shutdown().await.unwrap();
    runtime.shutdown();
}

#[tokio::test]
async fn foreign_status_before_matching_history_does_not_veto_discovery() {
    matching_discovery_in_order(true).await;
}

#[tokio::test]
async fn foreign_status_after_matching_history_does_not_veto_discovery() {
    matching_discovery_in_order(false).await;
}
