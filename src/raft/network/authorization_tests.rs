use super::authorization::*;
use super::request::RaftRequest;
use crate::raft::TypeConfig;
use crate::raft::admission::{AdmissionContext, AdmissionFence, Genesis};
use openraft::alias::{CommittedLeaderIdOf, LogIdOf, VoteOf};
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::vote::RaftLeaderId;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::time::{Duration, Instant};

fn replica(id: u64, byte: u8) -> ReplicaId {
    ReplicaId {
        physical_id: id,
        boot_nonce: [byte; 32],
    }
}

struct Fence(AdmissionContext, Instant);

struct WaitingController {
    inner: Arc<dyn AdmissionController>,
    gate: tokio::sync::RwLock<()>,
    calls: std::sync::atomic::AtomicUsize,
}

impl AdmissionController for WaitingController {
    fn local_replica(&self) -> ReplicaId {
        self.inner.local_replica()
    }

    fn authorize_raft(&self, _: ReplicaId) -> Result<RaftAuthorization, AdmissionDenied> {
        Err(AdmissionDenied(
            "synchronous authorization used under contention",
        ))
    }

    fn authorize_raft_async(
        &self,
        peer: ReplicaId,
    ) -> futures::future::BoxFuture<'_, Result<RaftAuthorization, AdmissionDenied>> {
        Box::pin(async move {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _read = self.gate.read().await;
            self.inner.authorize_raft(peer)
        })
    }

    fn dispatch(
        &self,
        _: ReplicaId,
        _: [u8; 32],
        _: AdmissionRpc,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<AdmissionRpc>> {
        Box::pin(async { anyhow::bail!("fixture has no grant RPC") })
    }
}

#[tokio::test]
async fn transport_contention_waits_and_retains_the_healthy_stream() {
    let mut network = super::tests::test_network(65_536, &[1, 2]);
    let mut peer = super::tests::attach_loopback_stream(&network, 2).await;
    let controller = Arc::new(WaitingController {
        inner: network.admission.clone(),
        gate: tokio::sync::RwLock::new(()),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    network.admission = controller.clone();
    let writer = controller.gate.write().await;
    let request = super::testing::vote_request();
    let rpc = network.send_rpc::<_, openraft::raft::VoteResponse<TypeConfig>>(
        crate::raft::types::test_replica(2),
        &request,
        super::RPCTypes::Vote,
        Duration::from_secs(1),
    );
    tokio::pin!(rpc);
    assert!(
        futures::poll!(&mut rpc).is_pending(),
        "state contention rejected transport"
    );
    drop(writer);
    let server = async {
        super::wire::read_framed_bounded(&mut peer, 65_536)
            .await
            .unwrap();
        let response = super::request::encode(
            super::request::Operation::Vote,
            &super::testing::vote_response(true),
        )
        .unwrap();
        super::wire::write_framed(&mut peer, &response)
            .await
            .unwrap();
    };
    let (response, ()) =
        tokio::time::timeout(Duration::from_secs(1), async { tokio::join!(rpc, server) })
            .await
            .unwrap();
    assert!(response.unwrap().vote_granted);
    assert_eq!(
        controller.calls.load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert!(network.peers[&2].stream.lock().await.is_some());
}

#[tokio::test]
async fn transport_contention_is_bounded_by_existing_rpc_deadline() {
    let mut network = super::tests::test_network(65_536, &[1, 2]);
    let _peer = super::tests::attach_loopback_stream(&network, 2).await;
    let controller = Arc::new(WaitingController {
        inner: network.admission.clone(),
        gate: tokio::sync::RwLock::new(()),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    network.admission = controller.clone();
    let _writer = controller.gate.write().await;
    let request = super::testing::vote_request();
    let rpc = network.send_rpc::<_, serde_json::Value>(
        crate::raft::types::test_replica(2),
        &request,
        super::RPCTypes::Vote,
        Duration::from_millis(50),
    );
    tokio::pin!(rpc);
    assert!(futures::poll!(&mut rpc).is_pending());
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(50)).await;
    assert!(matches!(rpc.await, Err(super::RPCError::Timeout(_))));
    assert!(network.peers[&2].stream.lock().await.is_none());
}

impl AdmissionFence for Fence {
    fn local_replica(&self) -> ReplicaId {
        self.0.local_replica
    }
    fn check(&self, context: &AdmissionContext) -> Result<Instant, AdmissionDenied> {
        if context != &self.0 {
            return Err(AdmissionDenied("context mismatch"));
        }
        Ok(self.1)
    }
}

fn grant() -> RaftAuthorization {
    let local = replica(1, 1);
    let remote = replica(2, 2);
    let context = AdmissionContext {
        local_replica: local,
        genesis: Genesis {
            config: crate::config::ClusterConfigFingerprint {
                version: 1,
                digest: [8; 32],
            },
            epoch: 9,
            voters: BTreeSet::from([local, remote]),
        },
    };
    RaftAuthorization {
        session: AdmissionSession::new(
            context.clone(),
            Arc::new(Fence(context, Instant::now() + Duration::from_secs(10))),
        )
        .unwrap(),
        peer: remote,
        configured_physical_ids: BTreeSet::from([1, 2, 3]),
        voters: BTreeSet::from([local, remote]),
        log_replicas: BTreeSet::from([local, remote]),
        memberships: vec![],
        committed_membership: openraft::alias::StoredMembershipOf::<TypeConfig>::new(
            None,
            openraft::Membership::new_with_defaults(vec![BTreeSet::from([local, remote])], []),
        ),
        last_applied: None,
        prepared_join: None,
        verified_bootstrap: None,
        replication: None,
    }
}

#[tokio::test(start_paused = true)]
async fn vote_requires_exact_authenticated_boot_and_authorized_candidate() {
    let grant = grant();
    for operation in [false, true] {
        for candidate in [replica(2, 2), replica(2, 3), replica(99, 2)] {
            let req = VoteRequest::<TypeConfig>::new(VoteOf::<TypeConfig>::new(1, candidate), None);
            let req = if operation {
                RaftRequest::PreVote(req)
            } else {
                RaftRequest::Vote(req)
            };
            assert_eq!(
                grant.validate_request(grant.peer, &req).is_ok(),
                candidate == grant.peer
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn append_checks_historical_log_identity_before_dispatch() {
    let grant = grant();
    let req = RaftRequest::AppendEntries(AppendEntriesRequest::<TypeConfig> {
        vote: VoteOf::<TypeConfig>::new(1, grant.peer),
        prev_log_id: Some(LogIdOf::<TypeConfig>::new(
            CommittedLeaderIdOf::<TypeConfig>::new(1, replica(99, 3)),
            1,
        )),
        entries: vec![],
        leader_commit: None,
    });
    assert!(grant.validate_request(grant.peer, &req).is_err());
}

#[tokio::test(start_paused = true)]
async fn expired_session_cannot_authorize_even_a_matching_vote() {
    let grant = grant();
    let req = RaftRequest::Vote(VoteRequest::<TypeConfig>::new(
        VoteOf::<TypeConfig>::new(1, grant.peer),
        None,
    ));
    assert!(grant.validate_request(grant.peer, &req).is_ok());
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(grant.validate_request(grant.peer, &req).is_err());
}

#[tokio::test]
async fn cached_stream_for_another_boot_cannot_send_raft() {
    use tokio::io::AsyncReadExt;
    let network = super::tests::test_network(65_536, &[1, 2]);
    let mut peer = super::tests::attach_loopback_stream(&network, 2).await;
    *network.peers[&2].remote_replica.write().unwrap() = Some(replica(2, 9));
    let error = network
        .send_rpc::<_, serde_json::Value>(
            crate::raft::types::test_replica(2),
            &super::testing::vote_request(),
            super::RPCTypes::Vote,
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("destination boot changed"));
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    assert!(network.peers[&2].stream.lock().await.is_none());
}

#[tokio::test]
async fn session_expiry_while_waiting_for_state_prevents_outbound_raft() {
    use tokio::io::AsyncReadExt;
    let mut network = super::tests::test_network(65_536, &[1, 2]);
    let mut peer = super::tests::attach_loopback_stream(&network, 2).await;
    let context = network
        .admission
        .authorize_raft(crate::raft::types::test_replica(2))
        .unwrap()
        .session
        .context()
        .clone();
    network.admission =
        super::testing::with_deadline(context, Instant::now() + Duration::from_secs(1));
    let guard = network.state_ref.write().await;
    let request = super::testing::vote_request();
    let rpc = network.send_rpc::<_, serde_json::Value>(
        crate::raft::types::test_replica(2),
        &request,
        super::RPCTypes::Vote,
        Duration::from_secs(10),
    );
    tokio::pin!(rpc);
    assert!(futures::poll!(&mut rpc).is_pending());
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    drop(guard);
    assert!(rpc.await.is_err());
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    assert!(network.peers[&2].stream.lock().await.is_none());
}

#[tokio::test]
async fn expired_session_never_returns_a_received_raft_response() {
    let mut network = super::tests::test_network(65_536, &[1, 2]);
    let mut peer = super::tests::attach_loopback_stream(&network, 2).await;
    let context = network
        .admission
        .authorize_raft(crate::raft::types::test_replica(2))
        .unwrap()
        .session
        .context()
        .clone();
    network.admission =
        super::testing::with_deadline(context, Instant::now() + Duration::from_secs(10));
    let request = super::testing::vote_request();
    let rpc = network.send_rpc::<_, serde_json::Value>(
        crate::raft::types::test_replica(2),
        &request,
        super::RPCTypes::Vote,
        Duration::from_secs(60),
    );
    tokio::pin!(rpc);
    {
        let read = super::wire::read_framed_bounded(&mut peer, 65_536);
        tokio::pin!(read);
        tokio::select! { _ = &mut rpc => panic!("RPC returned before response"), result = &mut read => { result.unwrap(); } }
    }
    let response = super::request::encode(
        super::request::Operation::Vote,
        &super::testing::vote_response(true),
    )
    .unwrap();
    super::wire::write_framed(&mut peer, &response)
        .await
        .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11)).await;
    assert!(rpc.await.is_err());
    assert!(network.peers[&2].stream.lock().await.is_none());
}

#[tokio::test]
async fn first_prepare_join_is_validated_against_stable_previous_voters() {
    use crate::raft::KafRequest;
    use crate::raft::admission::{AdmissionCommand, JoinPlan};
    let mut grant = grant();
    let previous = grant.voters.clone();
    let consumer = replica(2, 3);
    grant
        .memberships
        .push(openraft::Membership::new_with_defaults(
            vec![previous.clone()],
            [],
        ));
    let plan = JoinPlan {
        genesis: grant.session.context().genesis.clone(),
        consumer,
        request_nonce: [4; 32],
        previous_voters: previous.clone(),
        next_voters: [replica(1, 1), consumer].into(),
    };
    let request = RaftRequest::AppendEntries(AppendEntriesRequest::<TypeConfig> {
        vote: VoteOf::<TypeConfig>::new_committed(1, grant.peer),
        prev_log_id: None,
        leader_commit: None,
        entries: vec![openraft::alias::EntryOf::<TypeConfig> {
            log_id: openraft::testing::log_id::<TypeConfig>(1, grant.peer, 1),
            payload: openraft::EntryPayload::Normal(KafRequest::AdmissionMembership(
                AdmissionCommand::PrepareJoin(plan),
            )),
        }],
    });
    assert!(!grant.log_replicas.contains(&consumer));
    let result = grant.validate_request(grant.peer, &request);
    assert!(
        result.is_ok(),
        "first PrepareJoin must reach the state machine: {result:?}"
    );
    assert_eq!(grant.voters, previous);
    let speculative_vote = RaftRequest::Vote(VoteRequest::<TypeConfig>::new(
        VoteOf::<TypeConfig>::new(2, consumer),
        None,
    ));
    assert!(grant.validate_request(consumer, &speculative_vote).is_err());
}

#[tokio::test]
async fn prepare_join_cannot_use_one_side_of_a_joint_membership() {
    use crate::raft::admission::{AdmissionCommand, JoinPlan};
    let mut grant = grant();
    let consumer = replica(2, 3);
    let plan = JoinPlan {
        genesis: grant.session.context().genesis.clone(),
        consumer,
        request_nonce: [4; 32],
        previous_voters: grant.voters.clone(),
        next_voters: [replica(1, 1), consumer].into(),
    };
    grant.log_replicas.insert(consumer);
    grant
        .memberships
        .push(openraft::Membership::new_with_defaults(
            vec![plan.previous_voters.clone(), plan.next_voters.clone()],
            [],
        ));
    grant.committed_membership =
        openraft::alias::StoredMembershipOf::<TypeConfig>::new(None, grant.memberships[0].clone());
    let request = admission_command(&grant, AdmissionCommand::PrepareJoin(plan));
    assert!(grant.validate_request(grant.peer, &request).is_err());
}

#[tokio::test]
async fn prepare_join_rejects_unconfigured_physical_members_even_if_allowlisted() {
    use crate::raft::admission::AdmissionCommand;
    for unknown_previous in [false, true] {
        let mut grant = grant();
        let mut plan = replacement_plan(&grant);
        if unknown_previous {
            plan.previous_voters.insert(replica(99, 1));
            plan.next_voters.insert(replica(99, 1));
        } else {
            plan.consumer = replica(99, 1);
            plan.next_voters = plan
                .previous_voters
                .iter()
                .copied()
                .chain([plan.consumer])
                .collect();
        }
        grant
            .log_replicas
            .extend(plan.previous_voters.iter().copied());
        grant.log_replicas.extend(plan.next_voters.iter().copied());
        grant.memberships = [&plan.previous_voters, &plan.next_voters]
            .into_iter()
            .map(|voters| openraft::Membership::new_with_defaults(vec![voters.clone()], []))
            .collect();
        let request = admission_command(&grant, AdmissionCommand::PrepareJoin(plan));
        assert!(grant.validate_request(grant.peer, &request).is_err());
    }
}

#[tokio::test]
async fn prepare_join_does_not_authorize_batched_promotion_or_a_payload_sender() {
    use crate::raft::admission::AdmissionCommand;
    let mut grant = grant();
    let plan = replacement_plan(&grant);
    grant
        .memberships
        .push(openraft::Membership::new_with_defaults(
            vec![plan.previous_voters.clone()],
            [],
        ));
    let mut request = admission_command(&grant, AdmissionCommand::PrepareJoin(plan.clone()));
    assert!(grant.validate_request(grant.peer, &request).is_ok());
    assert!(grant.validate_request(plan.consumer, &request).is_err());
    if let RaftRequest::AppendEntries(ref mut append) = request {
        append.entries.push(openraft::alias::EntryOf::<TypeConfig> {
            log_id: openraft::testing::log_id::<TypeConfig>(1, grant.peer, 2),
            payload: openraft::EntryPayload::Membership(openraft::Membership::new_with_defaults(
                vec![plan.next_voters],
                [],
            )),
        });
    }
    assert!(grant.validate_request(grant.peer, &request).is_err());
}

fn admission_command(
    grant: &RaftAuthorization,
    command: crate::raft::admission::AdmissionCommand,
) -> RaftRequest {
    RaftRequest::AppendEntries(AppendEntriesRequest::<TypeConfig> {
        vote: VoteOf::<TypeConfig>::new_committed(1, grant.peer),
        prev_log_id: None,
        leader_commit: None,
        entries: vec![openraft::alias::EntryOf::<TypeConfig> {
            log_id: openraft::testing::log_id::<TypeConfig>(1, grant.peer, 10),
            payload: openraft::EntryPayload::Normal(crate::raft::KafRequest::AdmissionMembership(
                command,
            )),
        }],
    })
}

#[tokio::test]
async fn prepare_join_uses_committed_previous_membership_not_speculative_allowlist() {
    use crate::raft::admission::AdmissionCommand;
    let mut grant = grant();
    let mut plan = replacement_plan(&grant);
    plan.previous_voters = [replica(1, 7), replica(2, 2)].into();
    plan.next_voters = [replica(1, 7), plan.consumer].into();
    grant
        .memberships
        .push(openraft::Membership::new_with_defaults(
            vec![plan.previous_voters.clone()],
            [],
        ));
    let request = admission_command(&grant, AdmissionCommand::PrepareJoin(plan));
    assert!(grant.validate_request(grant.peer, &request).is_err());
}

#[tokio::test]
async fn configured_member_outside_genesis_can_be_prepared_but_cannot_vote() {
    use crate::raft::admission::AdmissionCommand;
    let mut grant = grant();
    let mut plan = replacement_plan(&grant);
    plan.consumer = replica(3, 3);
    plan.next_voters = plan
        .previous_voters
        .iter()
        .copied()
        .chain([plan.consumer])
        .collect();
    grant
        .memberships
        .push(grant.committed_membership.membership().clone());
    let request = admission_command(&grant, AdmissionCommand::PrepareJoin(plan.clone()));
    assert!(grant.validate_request(grant.peer, &request).is_ok());
    assert!(!grant.voters.contains(&plan.consumer));
    assert!(!grant.log_replicas.contains(&plan.consumer));
}

#[tokio::test]
async fn cancel_join_requires_exact_committed_pending_plan_and_removed_learner() {
    use crate::raft::admission::{AdmissionCommand, PreparedJoin};
    let mut grant = grant();
    let plan = replacement_plan(&grant);
    let prepared = openraft::testing::log_id::<TypeConfig>(1, grant.peer, 2);
    grant
        .memberships
        .push(grant.committed_membership.membership().clone());
    let cancel = AdmissionCommand::CancelJoin {
        plan: plan.clone(),
        prepared,
    };
    assert!(
        grant
            .validate_request(grant.peer, &admission_command(&grant, cancel.clone()))
            .is_err()
    );
    grant.prepared_join = Some(PreparedJoin {
        plan: plan.clone(),
        prepared,
        learner_applied: None,
    });
    grant.last_applied = Some(prepared);
    assert!(
        grant
            .validate_request(grant.peer, &admission_command(&grant, cancel.clone()))
            .is_ok()
    );
    let mut wrong_plan = plan.clone();
    wrong_plan.request_nonce[0] ^= 1;
    for command in [
        AdmissionCommand::CancelJoin {
            plan: wrong_plan,
            prepared,
        },
        AdmissionCommand::CancelJoin {
            plan: plan.clone(),
            prepared: openraft::testing::log_id::<TypeConfig>(1, grant.peer, 3),
        },
    ] {
        assert!(
            grant
                .validate_request(grant.peer, &admission_command(&grant, command))
                .is_err()
        );
    }
    let learner =
        openraft::Membership::new_with_defaults(vec![plan.previous_voters], [plan.consumer]);
    grant.committed_membership = openraft::alias::StoredMembershipOf::<TypeConfig>::new(
        Some(openraft::testing::log_id::<TypeConfig>(1, grant.peer, 3)),
        learner.clone(),
    );
    grant.memberships.push(learner);
    assert!(
        grant
            .validate_request(grant.peer, &admission_command(&grant, cancel))
            .is_err()
    );
}

#[tokio::test]
async fn learner_acknowledgement_requires_exact_committed_operation() {
    use crate::raft::admission::{AdmissionCommand, PreparedJoin};
    let mut grant = grant();
    let plan = replacement_plan(&grant);
    let prepared = openraft::testing::log_id::<TypeConfig>(1, grant.peer, 2);
    let learner = openraft::Membership::new_with_defaults(
        vec![plan.previous_voters.clone()],
        [plan.consumer],
    );
    grant.log_replicas.insert(plan.consumer);
    grant.memberships.push(learner.clone());
    grant.committed_membership = openraft::alias::StoredMembershipOf::<TypeConfig>::new(
        Some(openraft::testing::log_id::<TypeConfig>(1, grant.peer, 3)),
        learner,
    );
    grant.last_applied = Some(openraft::testing::log_id::<TypeConfig>(1, grant.peer, 3));
    let command = AdmissionCommand::LearnerApplied {
        consumer: plan.consumer,
        request_nonce: plan.request_nonce,
        prepared,
    };
    assert!(
        grant
            .validate_request(grant.peer, &admission_command(&grant, command.clone()))
            .is_err()
    );
    grant.prepared_join = Some(PreparedJoin {
        plan: plan.clone(),
        prepared,
        learner_applied: None,
    });
    assert!(
        grant
            .validate_request(grant.peer, &admission_command(&grant, command))
            .is_ok()
    );
    for (consumer, nonce, log) in [
        (replica(2, 9), plan.request_nonce, prepared),
        (plan.consumer, [9; 32], prepared),
        (
            plan.consumer,
            plan.request_nonce,
            openraft::testing::log_id::<TypeConfig>(1, grant.peer, 4),
        ),
    ] {
        let command = AdmissionCommand::LearnerApplied {
            consumer,
            request_nonce: nonce,
            prepared: log,
        };
        assert!(
            grant
                .validate_request(grant.peer, &admission_command(&grant, command))
                .is_err()
        );
    }
}

fn replacement_plan(grant: &RaftAuthorization) -> crate::raft::admission::JoinPlan {
    let consumer = replica(2, 3);
    crate::raft::admission::JoinPlan {
        genesis: grant.session.context().genesis.clone(),
        consumer,
        request_nonce: [4; 32],
        previous_voters: grant.voters.clone(),
        next_voters: [replica(1, 1), consumer].into(),
    }
}

fn prepared_snapshot(
    grant: &RaftAuthorization,
    prepared: crate::raft::admission::PreparedJoin,
    membership: openraft::Membership<ReplicaId, openraft::BasicNode>,
    membership_index: u64,
    applied_index: u64,
) -> RaftRequest {
    let log = |index| openraft::testing::log_id::<TypeConfig>(1, grant.peer, index);
    let stored = openraft::alias::StoredMembershipOf::<TypeConfig>::new(
        Some(log(membership_index)),
        membership,
    );
    RaftRequest::Snapshot(super::SnapshotTransfer {
        vote: VoteOf::<TypeConfig>::new_committed(1, grant.peer),
        meta: openraft::SnapshotMeta {
            last_log_id: Some(log(applied_index)),
            last_membership: stored.clone(),
            snapshot_id: "prepared".into(),
        },
        data: serde_json::to_vec(&serde_json::json!({
            "genesis": grant.session.context().genesis,
            "cluster_epoch": grant.session.context().genesis.epoch,
            "last_applied": log(applied_index),
            "last_membership": stored,
            "applied_progress": {},
            "prepared_join": prepared,
        }))
        .unwrap(),
    })
}

#[tokio::test]
async fn prepared_snapshot_accepts_first_plan_without_granting_consumer_votes() {
    let mut grant = grant();
    let plan = replacement_plan(&grant);
    let stable = openraft::Membership::new_with_defaults(vec![plan.previous_voters.clone()], []);
    grant.memberships.push(stable.clone());
    let prepared = crate::raft::admission::PreparedJoin {
        plan: plan.clone(),
        prepared: openraft::testing::log_id::<TypeConfig>(1, grant.peer, 2),
        learner_applied: None,
    };
    let request = prepared_snapshot(&grant, prepared, stable, 1, 2);
    assert!(grant.validate_request(grant.peer, &request).is_ok());
    assert!(!grant.log_replicas.contains(&plan.consumer));
    assert!(!grant.voters.contains(&plan.consumer));
}

#[tokio::test]
async fn prepared_snapshot_requires_ordered_committed_learner_barriers() {
    let mut grant = grant();
    let plan = replacement_plan(&grant);
    grant.log_replicas.insert(plan.consumer);
    let stable = openraft::Membership::new_with_defaults(vec![plan.previous_voters.clone()], []);
    let learner = openraft::Membership::new_with_defaults(
        vec![plan.previous_voters.clone()],
        [plan.consumer],
    );
    let joint = openraft::Membership::new_with_defaults(
        vec![plan.previous_voters.clone(), plan.next_voters.clone()],
        [],
    );
    let promoted = openraft::Membership::new_with_defaults(vec![plan.next_voters.clone()], []);
    grant.memberships = vec![
        stable.clone(),
        learner.clone(),
        joint.clone(),
        promoted.clone(),
    ];
    for (membership, membership_index, applied_index, learner_index, accepted) in [
        (stable.clone(), 1, 2, None, true),
        (learner.clone(), 3, 4, Some(4), true),
        (joint.clone(), 5, 5, Some(4), true),
        (stable.clone(), 1, 1, None, false),
        (stable.clone(), 1, 4, Some(4), false),
        (stable, 5, 5, Some(4), true),
        (learner.clone(), 3, 4, Some(2), false),
        (learner.clone(), 3, 4, Some(3), false),
        (learner.clone(), 3, 4, Some(5), false),
        (learner, 1, 4, Some(4), false),
        (joint.clone(), 5, 5, None, false),
        (joint.clone(), 5, 5, Some(5), false),
        (joint, 6, 5, Some(4), false),
        (promoted, 5, 5, Some(4), false),
    ] {
        let prepared = crate::raft::admission::PreparedJoin {
            plan: plan.clone(),
            prepared: openraft::testing::log_id::<TypeConfig>(1, grant.peer, 2),
            learner_applied: learner_index
                .map(|i| openraft::testing::log_id::<TypeConfig>(1, grant.peer, i)),
        };
        let request = prepared_snapshot(
            &grant,
            prepared,
            membership,
            membership_index,
            applied_index,
        );
        assert_eq!(
            grant.validate_request(grant.peer, &request).is_ok(),
            accepted,
            "membership={membership_index}, applied={applied_index}, learner={learner_index:?}"
        );
    }
}

#[tokio::test]
async fn historical_log_boots_never_authorize_current_votes() {
    use super::request::{Operation, encode};
    use openraft::raft::{AppendEntriesResponse, SnapshotResponse, VoteResponse};
    let grant = grant();
    let historical = replica(2, 7);
    assert!(!grant.log_replicas.contains(&historical));
    let log = openraft::testing::log_id::<TypeConfig>(1, historical, 1);
    let current_vote = VoteOf::<TypeConfig>::new(2, grant.peer);
    let old_vote = VoteOf::<TypeConfig>::new(3, historical);
    for pre_vote in [false, true] {
        let request = |vote| {
            let req = VoteRequest::<TypeConfig>::new(vote, Some(log));
            if pre_vote {
                RaftRequest::PreVote(req)
            } else {
                RaftRequest::Vote(req)
            }
        };
        assert!(
            grant
                .validate_request(grant.peer, &request(current_vote))
                .is_ok()
        );
        assert!(
            grant
                .validate_request(historical, &request(old_vote))
                .is_err()
        );
    }
    let append = RaftRequest::AppendEntries(AppendEntriesRequest::<TypeConfig> {
        vote: current_vote,
        prev_log_id: Some(log),
        leader_commit: Some(log),
        entries: vec![openraft::alias::EntryOf::<TypeConfig> {
            log_id: log,
            payload: openraft::EntryPayload::Blank,
        }],
    });
    assert!(grant.validate_request(grant.peer, &append).is_ok());
    for response in [
        encode(
            Operation::Vote,
            &VoteResponse::<TypeConfig>::new(current_vote, Some(log), true),
        )
        .unwrap(),
        encode(
            Operation::AppendEntries,
            &AppendEntriesResponse::<TypeConfig>::PartialSuccess(Some(log)),
        )
        .unwrap(),
    ] {
        assert!(grant.validate_response(&response).is_ok());
    }
    for response in [
        encode(
            Operation::Vote,
            &VoteResponse::<TypeConfig>::new(old_vote, Some(log), true),
        )
        .unwrap(),
        encode(
            Operation::InstallSnapshot,
            &SnapshotResponse::<TypeConfig>::new(old_vote),
        )
        .unwrap(),
        encode(
            Operation::AppendEntries,
            &AppendEntriesResponse::<TypeConfig>::HigherVote(old_vote),
        )
        .unwrap(),
    ] {
        assert!(grant.validate_response(&response).is_err());
    }
}

#[tokio::test]
async fn historical_join_commands_replay_without_current_pending_or_voting_rights() {
    use crate::raft::admission::AdmissionCommand;
    let mut grant = grant();
    let plan = replacement_plan(&grant);
    let old_leader = grant.peer;
    let historical =
        openraft::Membership::new_with_defaults(vec![plan.previous_voters.clone()], []);
    let current = openraft::Membership::new_with_defaults(vec![plan.next_voters.clone()], []);
    grant.memberships = vec![historical, current.clone()];
    grant.committed_membership = openraft::alias::StoredMembershipOf::<TypeConfig>::new(
        Some(openraft::testing::log_id::<TypeConfig>(1, old_leader, 20)),
        current,
    );
    grant.last_applied = Some(openraft::testing::log_id::<TypeConfig>(1, old_leader, 20));
    grant.peer = replica(1, 1);
    grant.voters = plan.next_voters.clone();
    grant.log_replicas.insert(plan.consumer);
    for command in [
        AdmissionCommand::PrepareJoin(plan.clone()),
        AdmissionCommand::LearnerApplied {
            consumer: plan.consumer,
            request_nonce: plan.request_nonce,
            prepared: openraft::testing::log_id::<TypeConfig>(1, old_leader, 2),
        },
        AdmissionCommand::CancelJoin {
            plan,
            prepared: openraft::testing::log_id::<TypeConfig>(1, old_leader, 2),
        },
    ] {
        let request = admission_command(&grant, command);
        assert!(grant.validate_request(grant.peer, &request).is_ok());
    }
    let request = RaftRequest::Vote(VoteRequest::<TypeConfig>::new(
        VoteOf::<TypeConfig>::new(2, old_leader),
        None,
    ));
    assert!(grant.validate_request(old_leader, &request).is_err());
    assert!(grant.prepared_join.is_none());
}

#[tokio::test]
async fn admission_membership_checks_each_boot_and_prepared_log() {
    use crate::raft::KafRequest;
    use crate::raft::admission::{AdmissionCommand, JoinPlan};
    let mut grant = grant();
    let consumer = replica(3, 3);
    let plan = JoinPlan {
        genesis: grant.session.context().genesis.clone(),
        consumer,
        request_nonce: [4; 32],
        previous_voters: grant.voters.clone(),
        next_voters: grant.voters.iter().copied().chain([consumer]).collect(),
    };
    let request = |command| {
        RaftRequest::AppendEntries(AppendEntriesRequest::<TypeConfig> {
            vote: VoteOf::<TypeConfig>::new(1, grant.peer),
            prev_log_id: None,
            leader_commit: None,
            entries: vec![openraft::alias::EntryOf::<TypeConfig> {
                log_id: openraft::testing::log_id::<TypeConfig>(1, grant.peer, 10),
                payload: openraft::EntryPayload::Normal(KafRequest::AdmissionMembership(command)),
            }],
        })
    };
    let command = AdmissionCommand::PrepareJoin(plan.clone());
    assert!(
        grant
            .validate_request(grant.peer, &request(command.clone()))
            .is_err()
    );
    grant.log_replicas.insert(consumer);
    for voters in [&plan.previous_voters, &plan.next_voters] {
        grant
            .memberships
            .push(openraft::Membership::new_with_defaults(
                vec![voters.clone()],
                [],
            ));
    }
    assert!(
        grant
            .validate_request(grant.peer, &request(command))
            .is_ok()
    );
    let mut wrong = plan.clone();
    wrong.genesis.epoch += 1;
    assert!(
        grant
            .validate_request(grant.peer, &request(AdmissionCommand::PrepareJoin(wrong)))
            .is_err()
    );
    grant.prepared_join = Some(crate::raft::admission::PreparedJoin {
        plan: plan.clone(),
        prepared: openraft::testing::log_id::<TypeConfig>(1, grant.peer, 1),
        learner_applied: None,
    });
    let learner = openraft::Membership::new_with_defaults(vec![plan.previous_voters], [consumer]);
    grant.committed_membership = openraft::alias::StoredMembershipOf::<TypeConfig>::new(
        Some(openraft::testing::log_id::<TypeConfig>(1, grant.peer, 2)),
        learner,
    );
    grant.last_applied = Some(openraft::testing::log_id::<TypeConfig>(1, grant.peer, 2));
    for leader in [grant.peer, replica(2, 9)] {
        let command = AdmissionCommand::LearnerApplied {
            consumer,
            request_nonce: [4; 32],
            prepared: openraft::testing::log_id::<TypeConfig>(1, leader, 1),
        };
        assert_eq!(
            grant
                .validate_request(grant.peer, &request(command))
                .is_ok(),
            leader == grant.peer
        );
    }
}

#[tokio::test(start_paused = true)]
async fn membership_requires_the_exact_authorized_transition() {
    use openraft::alias::EntryOf;
    use openraft::{BasicNode, EntryPayload, Membership};
    let mut grant = grant();
    let membership =
        Membership::<ReplicaId, BasicNode>::new_with_defaults(vec![grant.voters.clone()], []);
    let mut request = AppendEntriesRequest::<TypeConfig> {
        vote: VoteOf::<TypeConfig>::new(1, grant.peer),
        prev_log_id: None,
        leader_commit: None,
        entries: vec![EntryOf::<TypeConfig> {
            log_id: LogIdOf::<TypeConfig>::new(
                CommittedLeaderIdOf::<TypeConfig>::new(1, grant.peer),
                1,
            ),
            payload: EntryPayload::Membership(membership.clone()),
        }],
    };
    assert!(
        grant
            .validate_request(grant.peer, &RaftRequest::AppendEntries(request.clone()))
            .is_err()
    );
    grant.memberships.push(membership);
    assert!(
        grant
            .validate_request(grant.peer, &RaftRequest::AppendEntries(request.clone()))
            .is_ok()
    );
    request.entries[0].payload = EntryPayload::Membership(Membership::new_with_defaults(
        vec![BTreeSet::from([replica(1, 1), replica(2, 3)])],
        [],
    ));
    assert!(
        grant
            .validate_request(grant.peer, &RaftRequest::AppendEntries(request))
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn every_log_reference_and_response_vote_requires_authorized_boots() {
    use super::request::{Operation, encode};
    use openraft::raft::{AppendEntriesResponse, SnapshotResponse, VoteResponse};
    let grant = grant();
    let foreign_log =
        LogIdOf::<TypeConfig>::new(CommittedLeaderIdOf::<TypeConfig>::new(1, replica(99, 3)), 1);
    let vote = VoteOf::<TypeConfig>::new(1, grant.peer);
    let request = RaftRequest::Vote(VoteRequest::<TypeConfig>::new(vote, Some(foreign_log)));
    assert!(grant.validate_request(grant.peer, &request).is_err());
    let request = RaftRequest::AppendEntries(AppendEntriesRequest::<TypeConfig> {
        vote,
        prev_log_id: None,
        entries: vec![],
        leader_commit: Some(foreign_log),
    });
    assert!(grant.validate_request(grant.peer, &request).is_err());
    let foreign_vote = VoteOf::<TypeConfig>::new(1, replica(2, 3));
    let responses = [
        encode(
            Operation::Vote,
            &VoteResponse::<TypeConfig>::new(foreign_vote, None, true),
        )
        .unwrap(),
        encode(
            Operation::PreVote,
            &VoteResponse::<TypeConfig>::new(vote, Some(foreign_log), true),
        )
        .unwrap(),
        encode(
            Operation::InstallSnapshot,
            &SnapshotResponse::<TypeConfig>::new(foreign_vote),
        )
        .unwrap(),
        encode(
            Operation::AppendEntries,
            &AppendEntriesResponse::<TypeConfig>::HigherVote(foreign_vote),
        )
        .unwrap(),
        encode(
            Operation::AppendEntries,
            &AppendEntriesResponse::<TypeConfig>::PartialSuccess(Some(foreign_log)),
        )
        .unwrap(),
    ];
    for response in responses {
        assert!(grant.validate_response(&response).is_err());
    }
    let response = encode(
        Operation::Vote,
        &VoteResponse::<TypeConfig>::new(vote, None, false),
    )
    .unwrap();
    assert!(grant.validate_response(&response).is_ok());
}

#[tokio::test(start_paused = true)]
async fn snapshot_body_cannot_substitute_a_different_membership_or_genesis() {
    use openraft::alias::StoredMembershipOf;
    use openraft::{BasicNode, Membership};
    let mut grant = grant();
    let membership =
        Membership::<ReplicaId, BasicNode>::new_with_defaults(vec![grant.voters.clone()], []);
    grant.memberships.push(membership.clone());
    let stored = StoredMembershipOf::<TypeConfig>::new(None, membership);
    let snapshot = serde_json::json!({
        "genesis": grant.session.context().genesis,
        "cluster_epoch": 9,
        "last_applied": null,
        "last_membership": stored,
        "applied_progress": {},
    });
    let request = |body| {
        RaftRequest::Snapshot(super::SnapshotTransfer {
            vote: VoteOf::<TypeConfig>::new(1, grant.peer),
            meta: openraft::SnapshotMeta {
                last_log_id: None,
                last_membership: stored.clone(),
                snapshot_id: "exact".into(),
            },
            data: serde_json::to_vec(&body).unwrap(),
        })
    };
    assert!(
        grant
            .validate_request(grant.peer, &request(snapshot.clone()))
            .is_ok()
    );
    let mut wrong_genesis = snapshot.clone();
    wrong_genesis["genesis"]["epoch"] = serde_json::json!(10);
    assert!(
        grant
            .validate_request(grant.peer, &request(wrong_genesis))
            .is_err()
    );
    let mut wrong_membership = snapshot;
    wrong_membership["last_membership"] =
        serde_json::to_value(StoredMembershipOf::<TypeConfig>::default()).unwrap();
    assert!(
        grant
            .validate_request(grant.peer, &request(wrong_membership))
            .is_err()
    );
    assert!(grant.check(replica(1, 3), grant.peer).is_err());
    assert!(grant.check(replica(1, 1), replica(2, 3)).is_err());
}

#[tokio::test]
async fn snapshot_progress_requires_matching_member_key_and_applied_log() {
    let mut grant = grant();
    let plan = replacement_plan(&grant);
    let membership = grant.committed_membership.membership().clone();
    grant.memberships.push(membership.clone());
    let old_leader = replica(2, 7);
    grant.log_replicas.insert(old_leader);
    let prepared = crate::raft::admission::PreparedJoin {
        plan,
        prepared: openraft::testing::log_id::<TypeConfig>(1, old_leader, 2),
        learner_applied: None,
    };
    let base = prepared_snapshot(&grant, prepared, membership, 1, 10);
    let RaftRequest::Snapshot(base) = base else {
        unreachable!()
    };
    for (key, replica, index, accepted) in [
        (1, replica(1, 1), 9, true),
        (2, replica(1, 1), 9, false),
        (1, replica(1, 1), 11, false),
        (2, old_leader, 9, false),
    ] {
        let mut snapshot = super::SnapshotTransfer {
            vote: base.vote,
            meta: base.meta.clone(),
            data: base.data.clone(),
        };
        let mut body: serde_json::Value = serde_json::from_slice(&snapshot.data).unwrap();
        body["applied_progress"] = serde_json::json!({key.to_string(): {
            "request": {
                "node_id": replica.physical_id, "healthy": true, "replica": replica,
                "epoch": grant.session.context().genesis.epoch, "request_nonce": ([4u8; 32]),
                "genesis": grant.session.context().genesis,
            },
            "log_id": openraft::testing::log_id::<TypeConfig>(1, old_leader, index),
        }});
        snapshot.data = serde_json::to_vec(&body).unwrap();
        assert_eq!(
            grant
                .validate_request(grant.peer, &RaftRequest::Snapshot(snapshot))
                .is_ok(),
            accepted
        );
    }
}

#[tokio::test(start_paused = true)]
async fn state_authorization_requires_active_bound_session_and_configured_exact_peer() {
    let network = super::tests::test_network(65_536, &[1, 2, 3]);
    let peer = crate::raft::types::test_replica(2);
    let mut state = network.state_ref.write().await;
    assert!(RaftAuthorization::from_state(&network.config, &state, peer, None).is_err());
    let session = network.admission.authorize_raft(peer).unwrap().session;
    state.bind_admission(session).unwrap();
    let authority = RaftAuthorization::from_state(&network.config, &state, peer, None).unwrap();
    assert_eq!(authority.voters, authority.session.context().genesis.voters);
    assert!(RaftAuthorization::from_state(&network.config, &state, replica(2, 9), None).is_err());
    tokio::time::advance(Duration::from_secs(61)).await;
    assert!(RaftAuthorization::from_state(&network.config, &state, peer, None).is_err());
}

#[tokio::test]
async fn state_authorization_does_not_trust_an_unbound_bootstrap_plan() {
    let network = super::tests::test_network(65_536, &[1, 2, 3]);
    let peer = crate::raft::types::test_replica(2);
    let session = network.admission.authorize_raft(peer).unwrap().session;
    let mut state = network.state_ref.write().await;
    state.bind_admission(session.clone()).unwrap();
    let previous_voters = session.context().genesis.voters.clone();
    let consumer = replica(1, 99);
    let mut next_voters = previous_voters.clone();
    next_voters.retain(|id| id.physical_id != consumer.physical_id);
    next_voters.insert(consumer);
    let bootstrap = crate::raft::admission::PreparedJoin {
        plan: crate::raft::admission::JoinPlan {
            genesis: session.context().genesis.clone(),
            consumer,
            request_nonce: [9; 32],
            previous_voters,
            next_voters,
        },
        prepared: openraft::testing::log_id::<TypeConfig>(1, peer, 2),
        learner_applied: None,
    };
    state.prepared_join = Some(bootstrap);
    assert!(RaftAuthorization::from_state(&network.config, &state, peer, None).is_err());
}

#[tokio::test]
async fn state_authorization_keeps_historical_leader_out_of_current_voters() {
    let network = super::tests::test_network(65_536, &[1, 2, 3]);
    let peer = crate::raft::types::test_replica(2);
    let session = network.admission.authorize_raft(peer).unwrap().session;
    let mut state = network.state_ref.write().await;
    state.bind_admission(session.clone()).unwrap();
    state.genesis = Some(session.context().genesis.clone());
    state.cluster_epoch = Some(session.context().genesis.epoch);
    let current = replica(2, 9);
    let voters: BTreeSet<_> = [
        crate::raft::types::test_replica(1),
        current,
        crate::raft::types::test_replica(3),
    ]
    .into();
    let nodes: std::collections::BTreeMap<_, _> = voters
        .iter()
        .map(|id| {
            (
                *id,
                openraft::BasicNode::new(
                    network
                        .config
                        .get_peer(id.physical_id)
                        .unwrap()
                        .raft_address
                        .clone(),
                ),
            )
        })
        .collect();
    let membership = openraft::Membership::new(vec![voters.clone()], nodes).unwrap();
    let applied = openraft::testing::log_id::<TypeConfig>(1, peer, 5);
    state.last_membership =
        openraft::alias::StoredMembershipOf::<TypeConfig>::new(Some(applied), membership);
    state.last_applied_log = Some(applied);
    let authority = RaftAuthorization::from_state(&network.config, &state, current, None).unwrap();
    assert_eq!(authority.voters, voters);
    assert!(authority.log_replicas.contains(&peer));
    assert!(RaftAuthorization::from_state(&network.config, &state, peer, None).is_err());
}

#[tokio::test]
async fn source_replay_validates_committed_barriers_without_granting_votes() {
    use crate::raft::admission::{AdmissionCommand, JoinPlan};
    let network = super::tests::test_network(65_536, &[1, 2, 3]);
    let peer = crate::raft::types::test_replica(2);
    let session = network.admission.authorize_raft(peer).unwrap().session;
    let mut state = network.state_ref.write().await;
    state.bind_admission(session.clone()).unwrap();
    let authority = RaftAuthorization::from_state(&network.config, &state, peer, None).unwrap();
    let previous = session.context().genesis.voters.clone();
    let consumer = replica(3, 99);
    let next: BTreeSet<_> = previous
        .iter()
        .filter(|id| id.physical_id != 3)
        .copied()
        .chain([consumer])
        .collect();
    let plan = JoinPlan {
        genesis: session.context().genesis.clone(),
        consumer,
        request_nonce: [7; 32],
        previous_voters: previous.clone(),
        next_voters: next.clone(),
    };
    let membership = |configs: Vec<BTreeSet<ReplicaId>>, ids: BTreeSet<ReplicaId>| {
        let nodes: std::collections::BTreeMap<_, _> = ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    openraft::BasicNode::new(
                        network
                            .config
                            .get_peer(id.physical_id)
                            .unwrap()
                            .raft_address
                            .clone(),
                    ),
                )
            })
            .collect();
        openraft::EntryPayload::Membership(openraft::Membership::new(configs, nodes).unwrap())
    };
    let log = |index| openraft::testing::log_id::<TypeConfig>(1, peer, index);
    let command = |command| {
        openraft::EntryPayload::Normal(crate::raft::KafRequest::AdmissionMembership(command))
    };
    let learner: BTreeSet<_> = previous.iter().copied().chain([consumer]).collect();
    let payloads = vec![
        membership(vec![previous.clone()], previous.clone()),
        command(AdmissionCommand::PrepareJoin(plan.clone())),
        membership(vec![previous.clone()], learner.clone()),
        command(AdmissionCommand::LearnerApplied {
            consumer,
            request_nonce: plan.request_nonce,
            prepared: log(1),
        }),
        membership(vec![previous, next.clone()], learner),
        membership(vec![next.clone()], next),
    ];
    let entries: Vec<_> = payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| openraft::alias::EntryOf::<TypeConfig> {
            log_id: log(index as u64),
            payload,
        })
        .collect();
    let request = |entries, committed| {
        RaftRequest::AppendEntries(AppendEntriesRequest::<TypeConfig> {
            vote: VoteOf::<TypeConfig>::new_committed(2, peer),
            prev_log_id: None,
            entries,
            leader_commit: Some(log(committed)),
        })
    };
    assert!(
        authority
            .validate_request(peer, &request(entries.clone(), 5))
            .is_ok()
    );
    assert!(
        authority
            .validate_request(peer, &request(entries.clone(), 2))
            .is_err()
    );
    let mut skipped = entries.clone();
    skipped[2].payload = entries[5].payload.clone();
    assert!(
        authority
            .validate_request(peer, &request(skipped, 5))
            .is_err()
    );
    assert!(!authority.voters.contains(&consumer));
    assert!(authority.prepared_join.is_none());
}

#[tokio::test]
async fn missing_replication_prefix_is_an_explicit_conflict_not_permission() {
    let network = super::tests::test_network(65_536, &[1, 2, 3]);
    let peer = crate::raft::types::test_replica(2);
    let mut state = network.state_ref.write().await;
    state
        .bind_admission(network.admission.authorize_raft(peer).unwrap().session)
        .unwrap();
    let authority = RaftAuthorization::from_state(&network.config, &state, peer, None).unwrap();
    let request = RaftRequest::AppendEntries(AppendEntriesRequest::<TypeConfig> {
        vote: VoteOf::<TypeConfig>::new_committed(2, peer),
        prev_log_id: Some(openraft::testing::log_id::<TypeConfig>(1, peer, 8)),
        entries: vec![],
        leader_commit: None,
    });
    let error = authority.validate_request(peer, &request).unwrap_err();
    assert!(error.is::<ReplicationConflict>());
}

#[tokio::test]
async fn snapshot_completed_join_evidence_requires_exact_ordered_committed_records() {
    let mut grant = grant();
    let plan = replacement_plan(&grant);
    let final_membership =
        openraft::Membership::new_with_defaults(vec![plan.next_voters.clone()], []);
    grant.memberships.push(final_membership.clone());
    let prepared = crate::raft::admission::PreparedJoin {
        plan: plan.clone(),
        prepared: openraft::testing::log_id::<TypeConfig>(1, grant.peer, 2),
        learner_applied: Some(openraft::testing::log_id::<TypeConfig>(1, grant.peer, 4)),
    };
    let base = prepared_snapshot(&grant, prepared.clone(), final_membership, 6, 7);
    let RaftRequest::Snapshot(base) = base else {
        unreachable!()
    };
    let completed = crate::raft::admission::CompletedJoin {
        prepared,
        promoted: openraft::testing::log_id::<TypeConfig>(1, grant.peer, 6),
    };
    for mutation in [
        "valid",
        "historical_leaders",
        "genesis",
        "key",
        "consumer_boot",
        "unconfigured_history",
        "ack_before_prepare",
        "missing_ack",
        "promoted_before_ack",
        "future_promotion",
        "promotion_after_membership",
        "promotion_log_mismatch",
        "applied_log_disagrees_with_promotion",
    ] {
        let mut body: serde_json::Value = serde_json::from_slice(&base.data).unwrap();
        let mut meta = base.meta.clone();
        body["prepared_join"] = serde_json::Value::Null;
        let mut evidence = serde_json::to_value(&completed).unwrap();
        match mutation {
            "historical_leaders" => {
                for (field, index) in [("prepared", 2), ("learner_applied", 4)] {
                    evidence["prepared"][field] = serde_json::to_value(
                        openraft::testing::log_id::<TypeConfig>(1, replica(2, 99), index),
                    )
                    .unwrap();
                }
            }
            "genesis" => evidence["prepared"]["plan"]["genesis"]["epoch"] = serde_json::json!(99),
            "consumer_boot" => {
                evidence["prepared"]["plan"]["consumer"] =
                    serde_json::to_value(replica(2, 99)).unwrap();
            }
            "unconfigured_history" => {
                evidence["prepared"]["prepared"] = serde_json::to_value(
                    openraft::testing::log_id::<TypeConfig>(1, replica(99, 99), 2),
                )
                .unwrap();
            }
            "ack_before_prepare" => {
                evidence["prepared"]["learner_applied"] =
                    serde_json::to_value(openraft::testing::log_id::<TypeConfig>(1, grant.peer, 1))
                        .unwrap()
            }
            "missing_ack" => evidence["prepared"]["learner_applied"] = serde_json::Value::Null,
            "promoted_before_ack" => {
                evidence["promoted"] =
                    serde_json::to_value(openraft::testing::log_id::<TypeConfig>(1, grant.peer, 3))
                        .unwrap()
            }
            "future_promotion" => {
                evidence["promoted"] =
                    serde_json::to_value(openraft::testing::log_id::<TypeConfig>(1, grant.peer, 8))
                        .unwrap()
            }
            "promotion_after_membership" => {
                evidence["promoted"] =
                    serde_json::to_value(openraft::testing::log_id::<TypeConfig>(1, grant.peer, 7))
                        .unwrap();
            }
            "promotion_log_mismatch" => {
                evidence["promoted"] =
                    serde_json::to_value(openraft::testing::log_id::<TypeConfig>(2, grant.peer, 6))
                        .unwrap();
            }
            "applied_log_disagrees_with_promotion" => {
                meta.last_log_id = Some(openraft::testing::log_id::<TypeConfig>(2, grant.peer, 6));
                body["last_applied"] = serde_json::to_value(meta.last_log_id).unwrap();
            }
            _ => {}
        }
        let key = if mutation == "key" { "1" } else { "2" };
        body["completed_joins"] = serde_json::json!({key: evidence});
        let request = RaftRequest::Snapshot(super::SnapshotTransfer {
            vote: base.vote,
            meta,
            data: serde_json::to_vec(&body).unwrap(),
        });
        assert_eq!(
            grant.validate_request(grant.peer, &request).is_ok(),
            mutation == "valid" || mutation == "historical_leaders",
            "{mutation}"
        );
    }
    assert!(!grant.voters.contains(&plan.consumer));
    let vote = RaftRequest::Vote(VoteRequest::<TypeConfig>::new(
        VoteOf::<TypeConfig>::new(2, plan.consumer),
        None,
    ));
    assert!(grant.validate_request(plan.consumer, &vote).is_err());
}
