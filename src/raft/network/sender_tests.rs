//! Sender admission through the real authenticated Raft listener, without OS effects.

use super::request::{Operation, decode_payload, encode};
use super::wire::{read_framed_bounded, write_framed, write_replica_handshake};
use super::{KafRaft, RaftNetworkImpl, SnapshotTransfer};
use crate::raft::probe::{ClusterStatusRequest, ClusterStatusResponse};
use crate::raft::store::new_store;
use crate::raft::types::{KafRequest, TypeConfig};
use openraft::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf, VoteOf};
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::storage::RaftSnapshotBuilder;
use openraft::vote::RaftLeaderId;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

async fn request(kind: &str, sender: u64, config: &crate::config::Config) -> Vec<u8> {
    let vote = VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(sender));
    let log_id = LogIdOf::<TypeConfig>::new(
        CommittedLeaderIdOf::<TypeConfig>::new(1, crate::raft::types::test_replica(3)),
        0,
    );
    match kind {
        "status" => encode(
            Operation::Status,
            &ClusterStatusRequest {
                probe_from: sender,
                config_fingerprint: None,
                supports_cancellation_safe_rpc_v1: true,
            },
        ),
        "vote" => encode(Operation::Vote, &VoteRequest::<TypeConfig>::new(vote, None)),
        "pre_vote" => encode(
            Operation::PreVote,
            &VoteRequest::<TypeConfig>::new(vote, None),
        ),
        "append" => encode(
            Operation::AppendEntries,
            &AppendEntriesRequest::<TypeConfig> {
                vote: VoteOf::<TypeConfig>::new_committed(
                    7,
                    crate::raft::types::test_replica(sender),
                ),
                prev_log_id: None,
                entries: vec![EntryOf::<TypeConfig> {
                    log_id,
                    payload: openraft::EntryPayload::Normal(KafRequest::HealthUpdate {
                        node_id: 3,
                        healthy: true,
                    }),
                }],
                leader_commit: Some(log_id),
            },
        ),
        "snapshot" => {
            let (_, mut machine, state) = new_store(Arc::new(Vec::new()), 3, true, 0);
            {
                let mut state = state.write().await;
                let context = super::testing::controller(config)
                    .authorize_raft(crate::raft::types::test_replica(2))
                    .unwrap();
                state.genesis = Some(context.session.context().genesis.clone());
                state.cluster_epoch = Some(context.session.context().genesis.epoch);
                state.last_membership = openraft::alias::StoredMembershipOf::<TypeConfig>::new(
                    Some(log_id),
                    context.memberships[0].clone(),
                );
                state.last_applied_log = Some(log_id);
                state.node_health.insert(3, true);
            }
            let snapshot = machine.build_snapshot().await.unwrap();
            encode(
                Operation::InstallSnapshot,
                &SnapshotTransfer {
                    vote: VoteOf::<TypeConfig>::new_committed(
                        7,
                        crate::raft::types::test_replica(sender),
                    ),
                    meta: snapshot.meta,
                    data: snapshot.snapshot.into_inner(),
                },
            )
        }
        _ => panic!("unknown RPC fixture: {kind}"),
    }
    .unwrap()
}

async fn assert_sender_admission(kind: &str, sender: u64) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = (*super::tests::test_network(65_536, &[1, 2, 3]).config).clone();
    config.raft_listen = address.to_string();
    config.peers[0].raft_address = address.to_string();
    let (log, machine, state) = new_store(Arc::new(Vec::new()), 3, true, 0);
    let network = RaftNetworkImpl::new(
        Arc::new(config.clone()),
        state.clone(),
        crate::raft::network::testing::controller(&config),
    )
    .unwrap();
    let raft = KafRaft::new(
        crate::raft::types::test_replica(1),
        Arc::new(openraft::Config {
            enable_tick: false,
            ..Default::default()
        }),
        network.clone(),
        log,
        machine,
    )
    .await
    .unwrap();
    let (failures, _failure_rx) = tokio::sync::mpsc::unbounded_channel();
    let task = super::server::spawn_raft_accept_task(
        listener,
        super::server::RaftAcceptContext {
            admission: network.admission.clone(),
            config: network.config.clone(),
            raft: raft.clone(),
            state_ref: state.clone(),
            config_fingerprint: network.config_fingerprint,
            shutdown: network.shutdown.clone(),
        },
        failures,
    );
    network.tasks.lock().await.push(task);

    let result = tokio::time::timeout(Duration::from_secs(1), async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        write_replica_handshake(
            &mut stream,
            crate::raft::types::test_replica(2),
            1,
            Some("network-test-secret"),
            None,
            false,
        )
        .await
        .unwrap();
        let preflight = encode(
            Operation::Status,
            &ClusterStatusRequest {
                probe_from: 2,
                config_fingerprint: Some(network.config_fingerprint),
                supports_cancellation_safe_rpc_v1: true,
            },
        )
        .unwrap();
        write_framed(&mut stream, &preflight).await.unwrap();
        let response = read_framed_bounded(&mut stream, 65_536).await.unwrap();
        decode_payload::<ClusterStatusResponse>(&response, Operation::Status).unwrap();
        write_framed(&mut stream, &request(kind, sender, &config).await)
            .await
            .unwrap();
        read_framed_bounded(&mut stream, 65_536).await
    })
    .await;
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
    let response = result.expect("sender admission did not complete");
    let state = state.read().await;
    if sender != 2 {
        assert!(response.is_err(), "{kind} accepted another peer's identity");
        assert!(state.vote.is_none(), "{kind} changed the stored vote");
        assert!(state.log.is_empty(), "{kind} changed the stored log");
        assert!(state.last_applied_log.is_none(), "{kind} applied state");
        assert!(
            state.current_snapshot.is_none(),
            "{kind} installed a snapshot"
        );
    } else {
        let response = response
            .unwrap_or_else(|error| panic!("{kind} rejected its authenticated sender: {error}"));
        match kind {
            "status" => {
                decode_payload::<ClusterStatusResponse>(&response, Operation::Status).unwrap();
            }
            "vote" => {
                decode_payload::<openraft::raft::VoteResponse<TypeConfig>>(
                    &response,
                    Operation::Vote,
                )
                .unwrap();
            }
            "pre_vote" => {
                decode_payload::<openraft::raft::VoteResponse<TypeConfig>>(
                    &response,
                    Operation::PreVote,
                )
                .unwrap();
            }
            "append" => {
                decode_payload::<openraft::raft::AppendEntriesResponse<TypeConfig>>(
                    &response,
                    Operation::AppendEntries,
                )
                .unwrap();
            }
            "snapshot" => {
                decode_payload::<openraft::raft::SnapshotResponse<TypeConfig>>(
                    &response,
                    Operation::InstallSnapshot,
                )
                .unwrap();
            }
            _ => panic!("unknown response fixture: {kind}"),
        }
        if matches!(kind, "append" | "snapshot") {
            assert_eq!(state.node_health.get(&3), Some(&true));
        }
    }
}

#[tokio::test]
async fn vote_rejects_another_authenticated_peers_identity() {
    assert_sender_admission("vote", 3).await;
}

#[tokio::test]
async fn pre_vote_rejects_another_authenticated_peers_identity() {
    assert_sender_admission("pre_vote", 3).await;
}

#[tokio::test]
async fn append_rejects_another_authenticated_peers_identity() {
    assert_sender_admission("append", 3).await;
}

#[tokio::test]
async fn snapshot_rejects_another_authenticated_peers_identity() {
    assert_sender_admission("snapshot", 3).await;
}

#[tokio::test]
async fn status_rejects_another_authenticated_peers_identity() {
    assert_sender_admission("status", 3).await;
}

#[tokio::test]
async fn authenticated_senders_can_replicate_other_nodes_history() {
    for kind in ["status", "vote", "pre_vote", "append", "snapshot"] {
        assert_sender_admission(kind, 2).await;
    }
}
