use super::*;
use crate::auth::{Listener, Peer};
use crate::config::PeerConfig;
use crate::raft::probe::{ClusterStatusRequest, ClusterStatusResponse};
use crate::raft::store::new_store;
use crate::raft::types::test_replica;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Reply {
    Matching,
    ForeignFingerprint,
    ForeignGenesis,
    UnconfiguredGenesis,
    MismatchedPhysical,
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
                Peer::for_replica(advertised, None, true),
                cfg.cluster_secret.as_deref(),
                Listener::Raft,
            )
            .await;
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
                        "status",
                        ClusterStatusResponse {
                            config_fingerprint: Some(if reply == Reply::ForeignFingerprint {
                                foreign
                            } else {
                                fingerprint
                            }),
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
        let result = runtime.discover(&network, Duration::from_millis(500)).await;
        stop_tx.send(()).unwrap();
        result.unwrap()
    };
    let (operations, discovery) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(server, client)
    })
    .await
    .expect("single discovery pass must finish");
    let expected_operations = match reply {
        Reply::MismatchedPhysical => vec!["identity_rejected"],
        Reply::ForeignFingerprint => vec!["status"],
        _ => vec!["status", "discovery"],
    };
    assert_eq!(operations, expected_operations);
    if reply == Reply::Matching {
        assert_eq!(discovery.boots, [local, peer].into());
        assert_eq!(discovery.histories, [(peer, expected_genesis)].into());
    } else {
        assert_eq!(discovery.boots, [local].into());
        assert!(discovery.histories.is_empty());
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
async fn single_pass_rejects_foreign_status_fingerprint_before_management() {
    discover_once(Reply::ForeignFingerprint).await;
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
