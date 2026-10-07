use super::*;
use crate::config::Config;
use crate::raft::admission::{
    AdmissionContext, AdmissionFence, AdmissionMode, AdmissionTiming, CompletedJoin, ReceivedGrant,
};
use crate::raft::network::SnapshotTransfer;
use crate::raft::types::test_replica;
use openraft::testing::log_id;
use std::sync::Arc;
use tokio::time::{Duration, Instant};

struct Timing;

impl AdmissionTiming for Timing {
    fn consumer_deadline(&self, start: Instant) -> Result<Instant, AdmissionDenied> {
        Ok(start + Duration::from_secs(10))
    }
    fn reservation_deadline(&self, start: Instant) -> Result<Instant, AdmissionDenied> {
        Ok(start + Duration::from_secs(30))
    }
    fn quarantine_deadline(&self, boot: Instant) -> Result<Instant, AdmissionDenied> {
        Ok(boot)
    }
}

struct Fence(AdmissionContext, Instant);

impl AdmissionFence for Fence {
    fn local_replica(&self) -> ReplicaId {
        self.0.local_replica
    }
    fn check(&self, context: &AdmissionContext) -> Result<Instant, AdmissionDenied> {
        assert_eq!(context, &self.0);
        Ok(self.1)
    }
}

async fn fixture() -> (RaftAuthorization, PreparedJoin, Arc<Config>) {
    let cfg: Arc<Config> = Arc::new(
        serde_yaml::from_str(
            r#"
node_id: 1
raft_listen: '127.0.0.1:1000'
client_submit_listen: '127.0.0.1:2000'
peers:
  - {id: 1, raft_address: '127.0.0.1:1000', client_submit_address: '127.0.0.1:2000'}
  - {id: 2, raft_address: '127.0.0.2:1000', client_submit_address: '127.0.0.2:2000'}
  - {id: 3, raft_address: '127.0.0.3:1000', client_submit_address: '127.0.0.3:2000'}
vips: []
health: {command: [unused], interval_ms: 1000, timeout_ms: 500}
cluster_secret: snapshot-unit-test-secret-0123456789
dry_run: true
"#,
        )
        .unwrap(),
    );
    let consumer = ReplicaId {
        physical_id: 1,
        boot_nonce: [99; 32],
    };
    let peer = test_replica(2);
    let genesis = Genesis {
        config: cfg.cluster_config_fingerprint().unwrap(),
        epoch: 9,
        voters: [1, 2, 3].map(test_replica).into(),
    };
    let prepared = PreparedJoin {
        plan: JoinPlan {
            genesis: genesis.clone(),
            consumer,
            request_nonce: [7; 32],
            previous_voters: genesis.voters.clone(),
            next_voters: [consumer, peer, test_replica(3)].into(),
        },
        prepared: log_id::<TypeConfig>(1, peer, 4),
        learner_applied: None,
    };
    let core = crate::raft::admission::AdmissionController::new(
        cfg.clone(),
        consumer,
        Instant::now(),
        Arc::new(Timing),
    )
    .unwrap();
    let mut round = core
        .begin(genesis.clone(), AdmissionMode::Join { prepared: None })
        .unwrap();
    round.bind_prepared(&prepared).unwrap();
    let grants: Vec<_> = [peer, test_replica(3)]
        .into_iter()
        .map(|issuer| {
            let binding = [issuer.physical_id as u8; 32];
            let signed = SignedAdmission::sign(
                cfg.cluster_secret.as_deref(),
                11,
                issuer,
                consumer,
                binding,
                round.request().clone(),
            )
            .unwrap();
            ReceivedGrant::authenticate(
                cfg.cluster_secret.as_deref(),
                issuer,
                consumer,
                binding,
                round.request(),
                signed,
            )
            .unwrap()
        })
        .collect();
    let verified = core.complete(&mut round, &grants).unwrap();
    let (_, _, state) = crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
    let mut state = state.write().await;
    let context = AdmissionContext {
        local_replica: consumer,
        genesis,
    };
    state
        .bind_admission(
            AdmissionSession::new(
                context.clone(),
                Arc::new(Fence(context, Instant::now() + Duration::from_secs(10))),
            )
            .unwrap(),
        )
        .unwrap();
    let auth =
        RaftAuthorization::from_state(&cfg, &state, peer, verified.join_bootstrap()).unwrap();
    (auth, prepared, cfg)
}

fn snapshot(cfg: &Config, prepared: &PreparedJoin, stage: &str) -> SnapshotTransfer {
    let plan = &prepared.plan;
    let mut pending = prepared.clone();
    pending.learner_applied = Some(log_id::<TypeConfig>(1, test_replica(2), 7));
    let (configs, index) = match stage {
        "learner" => (vec![plan.previous_voters.clone()], 5),
        "joint" => (
            vec![plan.previous_voters.clone(), plan.next_voters.clone()],
            8,
        ),
        "final" => (vec![plan.next_voters.clone()], 9),
        _ => unreachable!(),
    };
    let mut ids: BTreeSet<_> = configs.iter().flatten().copied().collect();
    ids.insert(plan.consumer);
    let nodes: BTreeMap<_, _> = ids
        .into_iter()
        .map(|id| {
            (
                id,
                BasicNode::new(cfg.get_peer(id.physical_id).unwrap().raft_address.clone()),
            )
        })
        .collect();
    let membership = StoredMembershipOf::<TypeConfig>::new(
        Some(log_id::<TypeConfig>(1, test_replica(2), index)),
        Membership::new(configs, nodes).unwrap(),
    );
    let completed = CompletedJoin {
        prepared: pending.clone(),
        promoted: *membership.log_id().as_ref().unwrap(),
    };
    let completed: BTreeMap<_, _> = if stage == "final" {
        [(plan.consumer.physical_id, completed)].into()
    } else {
        BTreeMap::new()
    };
    let pending = if stage == "final" {
        None
    } else {
        Some(pending)
    };
    let last = Some(log_id::<TypeConfig>(1, test_replica(2), 10));
    SnapshotTransfer {
        vote: VoteOf::<TypeConfig>::new_committed(2, test_replica(2)),
        meta: openraft::SnapshotMeta {
            last_log_id: last,
            last_membership: membership.clone(),
            snapshot_id: "bootstrap-unit".into(),
        },
        data: serde_json::to_vec(&serde_json::json!({
            "genesis": plan.genesis, "cluster_epoch": plan.genesis.epoch,
            "last_applied": last, "last_membership": membership,
            "applied_progress": {}, "prepared_join": pending, "completed_joins": completed,
        }))
        .unwrap(),
    }
}

#[tokio::test(start_paused = true)]
async fn bootstrap_snapshot_rejects_a_different_operation_before_install() {
    let (auth, prepared, cfg) = fixture().await;
    let mut foreign = prepared.clone();
    foreign.plan.request_nonce[0] ^= 1;
    let req = RaftRequest::Snapshot(snapshot(&cfg, &foreign, "learner"));
    assert!(auth.validate_request(auth.peer, &req).is_err());
}

#[tokio::test(start_paused = true)]
async fn bootstrap_snapshot_fast_forwards_exact_joint_and_final_without_voting_permission() {
    let (auth, prepared, cfg) = fixture().await;
    for stage in ["joint", "final"] {
        let req = RaftRequest::Snapshot(snapshot(&cfg, &prepared, stage));
        let result = auth.validate_request(auth.peer, &req);
        assert!(result.is_ok(), "{stage}: {result:?}");
        assert_eq!(auth.voters, prepared.plan.previous_voters);
        assert!(!auth.voters.contains(&prepared.plan.consumer));
        assert!(auth.prepared_join.is_none());
        assert!(auth.last_applied.is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn bootstrap_snapshot_learner_keeps_session_and_current_voter_guards() {
    let (auth, prepared, cfg) = fixture().await;
    let request = RaftRequest::Snapshot(snapshot(&cfg, &prepared, "learner"));
    assert!(auth.validate_request(auth.peer, &request).is_ok());
    assert!(
        auth.sender_vote(
            prepared.plan.consumer,
            &VoteOf::<TypeConfig>::new(2, prepared.plan.consumer)
        )
        .is_err()
    );
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(auth.validate_request(auth.peer, &request).is_err());
}

#[tokio::test(start_paused = true)]
async fn bootstrap_snapshot_fast_forward_requires_verified_proof_not_just_receipts() {
    let (mut auth, prepared, cfg) = fixture().await;
    auth.verified_bootstrap = None;
    for stage in ["joint", "final"] {
        let req = RaftRequest::Snapshot(snapshot(&cfg, &prepared, stage));
        assert!(auth.validate_request(auth.peer, &req).is_err(), "{stage}");
    }
    assert!(
        auth.validate_request(
            auth.peer,
            &RaftRequest::Snapshot(snapshot(&cfg, &prepared, "learner"))
        )
        .is_ok()
    );
}

#[tokio::test(start_paused = true)]
async fn bootstrap_snapshot_rejects_unbound_or_incomplete_stage_evidence() {
    let (auth, prepared, cfg) = fixture().await;
    for stage in ["learner", "joint", "final"] {
        for mutation in [
            "nonce",
            "prepared_log",
            "consumer",
            "genesis",
            "absent",
            "missing_ack",
            "ack_before_prepare",
        ] {
            let mut req = snapshot(&cfg, &prepared, stage);
            let mut body: serde_json::Value = serde_json::from_slice(&req.data).unwrap();
            let evidence = if stage == "final" {
                &mut body["completed_joins"]["1"]["prepared"]
            } else {
                &mut body["prepared_join"]
            };
            match mutation {
                "nonce" => evidence["plan"]["request_nonce"] = serde_json::json!([8; 32].to_vec()),
                "prepared_log" => {
                    evidence["prepared"] =
                        serde_json::to_value(log_id::<TypeConfig>(2, auth.peer, 4)).unwrap()
                }
                "consumer" => {
                    evidence["plan"]["consumer"] = serde_json::to_value(test_replica(1)).unwrap()
                }
                "genesis" => evidence["plan"]["genesis"]["epoch"] = serde_json::json!(10),
                "absent" => *evidence = serde_json::Value::Null,
                "missing_ack" => evidence["learner_applied"] = serde_json::Value::Null,
                "ack_before_prepare" => {
                    evidence["learner_applied"] =
                        serde_json::to_value(log_id::<TypeConfig>(1, auth.peer, 3)).unwrap()
                }
                _ => unreachable!(),
            }
            req.data = serde_json::to_vec(&body).unwrap();
            // A preparation-only learner snapshot is valid without an acknowledgement.
            let valid = stage == "learner" && mutation == "missing_ack";
            assert_eq!(
                auth.validate_request(auth.peer, &RaftRequest::Snapshot(req))
                    .is_ok(),
                valid,
                "{stage}/{mutation}"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn bootstrap_snapshot_rejects_address_substitution_and_unrelated_membership() {
    let (auth, prepared, cfg) = fixture().await;
    for stage in ["joint", "final"] {
        for change_address in [true, false] {
            let mut req = snapshot(&cfg, &prepared, stage);
            let old = req.meta.last_membership.membership();
            let mut configs = old.get_joint_config().clone();
            let mut nodes: BTreeMap<_, _> =
                old.nodes().map(|(id, node)| (*id, node.clone())).collect();
            if change_address {
                nodes.insert(auth.peer, BasicNode::new("127.0.0.99:9999"));
            } else {
                configs[0].remove(&test_replica(3));
            }
            req.meta.last_membership = StoredMembershipOf::<TypeConfig>::new(
                *req.meta.last_membership.log_id(),
                Membership::new(configs, nodes).unwrap(),
            );
            let mut body: serde_json::Value = serde_json::from_slice(&req.data).unwrap();
            body["last_membership"] = serde_json::to_value(&req.meta.last_membership).unwrap();
            req.data = serde_json::to_vec(&body).unwrap();
            assert!(
                auth.validate_request(auth.peer, &RaftRequest::Snapshot(req))
                    .is_err()
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn bootstrap_snapshot_completion_requires_ordered_full_log_ids() {
    let (mut auth, prepared, cfg) = fixture().await;
    let base = snapshot(&cfg, &prepared, "final");
    auth.memberships
        .push(base.meta.last_membership.membership().clone());
    let mut body: serde_json::Value = serde_json::from_slice(&base.data).unwrap();
    body["completed_joins"]["1"]["prepared"]["learner_applied"] =
        serde_json::to_value(log_id::<TypeConfig>(2, auth.peer, 7)).unwrap();
    let req = SnapshotTransfer {
        data: serde_json::to_vec(&body).unwrap(),
        ..base
    };
    assert!(
        auth.validate_request(auth.peer, &RaftRequest::Snapshot(req))
            .is_err()
    );
}

async fn established_voter(
    cfg: &Config,
    prepared: &PreparedJoin,
    applied_join_stage: Option<&str>,
) -> RaftAuthorization {
    let mut cfg = cfg.clone();
    cfg.node_id = 3;
    let local = test_replica(3);
    let context = AdmissionContext {
        local_replica: local,
        genesis: prepared.plan.genesis.clone(),
    };
    let (_, _, state) = crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
    let mut state = state.write().await;
    state
        .bind_admission(
            AdmissionSession::new(
                context.clone(),
                Arc::new(Fence(context, Instant::now() + Duration::from_secs(10))),
            )
            .unwrap(),
        )
        .unwrap();
    state.genesis = Some(prepared.plan.genesis.clone());
    state.cluster_epoch = Some(prepared.plan.genesis.epoch);
    if let Some(stage) = applied_join_stage {
        let req = snapshot(&cfg, prepared, stage);
        let body: serde_json::Value = serde_json::from_slice(&req.data).unwrap();
        state.last_membership = req.meta.last_membership;
        state.last_applied_log = Some(log_id::<TypeConfig>(
            1,
            test_replica(2),
            if stage == "learner" { 7 } else { 8 },
        ));
        state.prepared_join = serde_json::from_value(body["prepared_join"].clone()).unwrap();
    } else {
        let voters = prepared.plan.previous_voters.clone();
        let nodes: BTreeMap<_, _> = voters
            .iter()
            .map(|id| {
                (
                    *id,
                    BasicNode::new(cfg.get_peer(id.physical_id).unwrap().raft_address.clone()),
                )
            })
            .collect();
        state.last_membership = StoredMembershipOf::<TypeConfig>::new(
            Some(log_id::<TypeConfig>(1, test_replica(2), 1)),
            Membership::new(vec![voters], nodes).unwrap(),
        );
        state.last_applied_log = Some(log_id::<TypeConfig>(1, test_replica(2), 3));
    }
    RaftAuthorization::from_state(&cfg, &state, test_replica(2), None).unwrap()
}

#[tokio::test(start_paused = true)]
async fn established_voter_snapshot_requires_verified_membership_chain() {
    let (_, prepared, cfg) = fixture().await;
    let auth = established_voter(&cfg, &prepared, None).await;
    let req = RaftRequest::Snapshot(snapshot(&cfg, &prepared, "final"));
    let error = auth.validate_request(auth.peer, &req).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Raft membership is outside the authorized transition"
    );
    assert_eq!(auth.voters, prepared.plan.previous_voters);
    assert!(auth.verified_bootstrap.is_none());
}

#[tokio::test(start_paused = true)]
async fn established_voter_snapshot_advances_only_the_locally_proven_transition() {
    let (_, prepared, cfg) = fixture().await;
    for (local_stage, received_stage, accepted) in [
        ("learner", "joint", true),
        ("joint", "final", true),
        ("learner", "final", false),
    ] {
        let auth = established_voter(&cfg, &prepared, Some(local_stage)).await;
        let voters = auth.voters.clone();
        let local_membership = auth.committed_membership.clone();
        let req = RaftRequest::Snapshot(snapshot(&cfg, &prepared, received_stage));
        let result = auth.validate_request(auth.peer, &req);
        assert_eq!(
            result.is_ok(),
            accepted,
            "{local_stage} -> {received_stage}: {result:?}"
        );
        assert_eq!(auth.voters, voters);
        assert_eq!(auth.committed_membership, local_membership);
        assert!(auth.verified_bootstrap.is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn bootstrap_snapshot_rejects_a_later_join_without_verified_chain() {
    let (auth, prepared, cfg) = fixture().await;
    let next_consumer = ReplicaId {
        physical_id: 3,
        boot_nonce: [88; 32],
    };
    let mut next_voters = prepared.plan.next_voters.clone();
    next_voters.remove(&test_replica(3));
    next_voters.insert(next_consumer);
    let next_prepared = PreparedJoin {
        plan: JoinPlan {
            genesis: prepared.plan.genesis.clone(),
            consumer: next_consumer,
            request_nonce: [8; 32],
            previous_voters: prepared.plan.next_voters.clone(),
            next_voters: next_voters.clone(),
        },
        prepared: log_id::<TypeConfig>(2, auth.peer, 14),
        learner_applied: Some(log_id::<TypeConfig>(2, auth.peer, 17)),
    };
    let promoted = log_id::<TypeConfig>(2, auth.peer, 19);
    let nodes: BTreeMap<_, _> = next_voters
        .iter()
        .map(|id| {
            (
                *id,
                BasicNode::new(cfg.get_peer(id.physical_id).unwrap().raft_address.clone()),
            )
        })
        .collect();
    let mut req = snapshot(&cfg, &prepared, "final");
    req.meta.last_membership = StoredMembershipOf::<TypeConfig>::new(
        Some(promoted),
        Membership::new(vec![next_voters], nodes).unwrap(),
    );
    req.meta.last_log_id = Some(log_id::<TypeConfig>(2, auth.peer, 20));
    let mut body: serde_json::Value = serde_json::from_slice(&req.data).unwrap();
    body["last_membership"] = serde_json::to_value(&req.meta.last_membership).unwrap();
    body["last_applied"] = serde_json::to_value(req.meta.last_log_id).unwrap();
    body["completed_joins"]["3"] = serde_json::to_value(CompletedJoin {
        prepared: next_prepared,
        promoted,
    })
    .unwrap();
    req.data = serde_json::to_vec(&body).unwrap();
    let error = auth
        .validate_request(auth.peer, &RaftRequest::Snapshot(req))
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "snapshot membership is outside the verified bootstrap transition"
    );
    assert!(!auth.voters.contains(&next_consumer));
}
