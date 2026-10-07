use super::*;
use crate::raft::admission::{JoinPlan, PreparedJoin};
use crate::raft::types::{KafRequest, TypeConfig, test_replica};
use openraft::alias::EntryOf;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, Membership, StoredMembership};
use std::time::Duration;

struct RecoveryNode {
    runtime: Arc<RuntimeDriver>,
    raft: KafRaft,
    network: Arc<RaftNetworkImpl>,
    state: Arc<RwLock<KafStorageState>>,
    controls: crate::raft::RaftControlTasks,
}

impl RecoveryNode {
    async fn stop(&mut self) {
        self.controls.shutdown().await.unwrap();
        self.network.shutdown().await.unwrap();
        self.raft.shutdown().await.unwrap();
    }
}

async fn drive_until(budget: Duration, mut done: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + budget;
    loop {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        if done().await {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "recovery operation exceeded its virtual budget"
        );
        tokio::time::advance(Duration::from_millis(10)).await;
    }
}

async fn drive<T>(work: impl std::future::Future<Output = anyhow::Result<T>>) -> T {
    let mut work = Box::pin(work);
    let mut result = None;
    drive_until(Duration::from_secs(5), async || {
        if let std::task::Poll::Ready(value) = futures::poll!(work.as_mut()) {
            result = Some(value.unwrap());
            true
        } else {
            false
        }
    })
    .await;
    result.unwrap()
}

async fn recovery_cluster() -> Vec<RecoveryNode> {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
    let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
    assert!(sequence < 256);
    let address =
        std::net::Ipv4Addr::from(0x7ff0_0000 | ((std::process::id() % 4095) << 8) | sequence);
    let mut listeners = Vec::new();
    let mut submit = Vec::new();
    let mut peers = Vec::new();
    for id in 1..=3 {
        let raft = tokio::net::TcpListener::bind((address, 0)).await.unwrap();
        let client = tokio::net::TcpListener::bind((address, 0)).await.unwrap();
        peers.push(crate::config::PeerConfig {
            id,
            raft_address: raft.local_addr().unwrap().to_string(),
            client_submit_address: client.local_addr().unwrap().to_string(),
        });
        listeners.push(raft);
        submit.push(client);
    }
    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let mut cfg = (*super::tests::config()).clone();
        cfg.node_id = peers[index].id;
        cfg.raft_listen = peers[index].raft_address.clone();
        cfg.client_submit_listen = peers[index].client_submit_address.clone();
        cfg.peers = peers.clone();
        let (raft, network, state, _, _, controls) = crate::raft::start_raft(
            Arc::new(cfg),
            Arc::new(Vec::new()),
            crate::listener::ListenerSource::Bound(listener),
        )
        .await
        .unwrap();
        nodes.push(RecoveryNode {
            runtime: controls.runtime(),
            raft,
            network,
            state,
            controls,
        });
    }
    let timing = nodes[0].runtime.timing;
    drive_until(
        timing.restart_quarantine() + timing.consumer_use() * 2,
        async || {
            for node in &nodes {
                let state = node.state.read().await;
                if node.runtime.current().is_none()
                    || state.genesis.is_none()
                    || state.last_membership.membership().voter_ids().count() != 3
                    || node.raft.current_leader().await.is_none()
                {
                    return false;
                }
            }
            true
        },
    )
    .await;
    nodes
}

#[tokio::test(start_paused = true)]
async fn recovery_commits_learner_removal_before_cancellation_and_reopens_the_join_slot() {
    use crate::raft::admission::AdmissionCommand;
    use openraft::async_runtime::WatchReceiver;
    let mut nodes = recovery_cluster().await;
    let leader = nodes[0].raft.current_leader().await.unwrap();
    let leader_index = nodes
        .iter()
        .position(|node| node.runtime.local == leader)
        .unwrap();
    let follower_index = nodes
        .iter()
        .position(|node| node.runtime.local != leader)
        .unwrap();
    let physical = nodes[follower_index].runtime.local.physical_id;
    nodes[follower_index].stop().await;
    let node = &nodes[leader_index];
    let genesis = node.state.read().await.genesis.clone().unwrap();
    let consumer = ReplicaId::fresh(physical).unwrap();
    let response = drive(node.runtime.manage(
        consumer,
        ManagementAction::PrepareJoin {
            genesis: genesis.clone(),
            operation_nonce: [41; 32],
        },
    ))
    .await;
    let ManagementResult::Prepared(pending) = response else {
        panic!("missing preparation");
    };
    let mut wrong_plan = pending.plan.clone();
    wrong_plan.request_nonce = [43; 32];
    let error = node
        .runtime
        .cancel_join(&node.raft, leader, wrong_plan, pending.prepared)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("differs from committed operation")
    );
    let wrong_peer = ReplicaId::fresh(leader.physical_id).unwrap();
    let error = node
        .runtime
        .cancel_join(
            &node.raft,
            wrong_peer,
            pending.plan.clone(),
            pending.prepared,
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("only the consumer physical member")
    );
    assert!(
        node.state
            .read()
            .await
            .last_membership
            .membership()
            .nodes()
            .any(|(id, _)| *id == consumer)
    );
    let budget = node.runtime.timing.issuer_reservation() + node.runtime.timing.consumer_use() * 2;
    drive_until(budget, async || {
        node.state.read().await.prepared_join.is_none()
    })
    .await;
    {
        let state = node.state.read().await;
        let removal = state.last_membership.log_id().unwrap();
        assert!(
            !state
                .last_membership
                .membership()
                .nodes()
                .any(|(id, _)| *id == consumer)
        );
        let cancellation = state
            .log
            .values()
            .find(|entry| {
                matches!(
                    &entry.payload, EntryPayload::Normal(KafRequest::AdmissionMembership(
                        AdmissionCommand::CancelJoin { plan, prepared }
                    )) if plan == &pending.plan && prepared == &pending.prepared
                )
            })
            .expect("cancellation must be committed, not a local pending reset");
        assert!(pending.prepared.index < removal.index);
        assert!(removal.index < cancellation.log_id.index);
        assert!(state.last_applied_log.unwrap().index >= cancellation.log_id.index);
        assert_eq!(
            state
                .last_membership
                .membership()
                .voter_ids()
                .collect::<BTreeSet<_>>(),
            pending.plan.previous_voters
        );
    }
    assert_eq!(
        node.raft.metrics().borrow_watched().current_leader,
        Some(leader)
    );
    let response = drive(node.runtime.manage(
        ReplicaId::fresh(physical).unwrap(),
        ManagementAction::PrepareJoin {
            genesis,
            operation_nonce: [42; 32],
        },
    ))
    .await;
    let ManagementResult::Prepared(next) = response else {
        panic!("join slot remained blocked");
    };
    assert_ne!(next.prepared, pending.prepared);
    drive(async {
        node.raft
            .change_membership(
                openraft::ChangeMembers::RemoveNodes([next.plan.consumer].into()),
                false,
            )
            .await?;
        Ok(())
    })
    .await;
    drive(node.runtime.manage(
        leader,
        ManagementAction::CancelJoin {
            plan: next.plan,
            prepared: next.prepared,
        },
    ))
    .await;
    assert!(node.state.read().await.prepared_join.is_none());
    let error = node
        .runtime
        .cancel_join(&node.raft, leader, pending.plan.clone(), pending.prepared)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no committed join remains"));
    for (index, node) in nodes.iter_mut().enumerate() {
        if index != follower_index {
            node.stop().await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn recovery_finishes_an_interrupted_committed_joint_promotion() {
    use crate::raft::admission::{AdmissionCommand, AdmissionMode};
    let (events, _guard) = crate::warning_limit::test_support::LogCapture::start(
        "keepafloatd::raft::store::membership=info",
    );
    let mut nodes = recovery_cluster().await;
    let leader = nodes[0].raft.current_leader().await.unwrap();
    let follower_index = nodes
        .iter()
        .position(|node| node.runtime.local != leader)
        .unwrap();
    let cfg = nodes[follower_index].runtime.cfg.clone();
    nodes[follower_index].stop().await;
    let (raft, network, state, _, _, controls) = crate::raft::start_raft(
        cfg,
        Arc::new(Vec::new()),
        crate::listener::ListenerSource::Configured,
    )
    .await
    .unwrap();
    nodes[follower_index] = RecoveryNode {
        runtime: controls.runtime(),
        raft,
        network,
        state,
        controls,
    };
    let joining = nodes[follower_index].runtime.clone();
    let join_round = joining.round.lock().await;
    let quarantine = joining.timing.quarantine_deadline(joining.boot).unwrap();
    drive_until(joining.timing.restart_quarantine(), async || {
        Instant::now() >= quarantine
    })
    .await;
    let node = nodes
        .iter()
        .find(|node| node.runtime.local == leader)
        .unwrap();
    let learner = &nodes[follower_index];
    let consumer = learner.runtime.local;
    let genesis = node.state.read().await.genesis.clone().unwrap();
    let response = drive(node.runtime.manage(
        consumer,
        ManagementAction::PrepareJoin {
            genesis,
            operation_nonce: [44; 32],
        },
    ))
    .await;
    let ManagementResult::Prepared(pending) = response else {
        panic!("missing preparation");
    };
    let mut round = joining
        .core
        .lock()
        .unwrap()
        .begin(
            pending.plan.genesis.clone(),
            AdmissionMode::Join { prepared: None },
        )
        .unwrap();
    round.bind_prepared(&pending).unwrap();
    let targets: Vec<_> = nodes
        .iter()
        .filter(|node| node.runtime.local != consumer)
        .map(|node| node.runtime.local)
        .collect();
    let mut grants = Vec::new();
    for target in targets {
        grants.push(
            drive(learner.network.admission_rpc(
                target,
                round.request().clone(),
                joining.rpc_budget(),
            ))
            .await,
        );
    }
    joining
        .install(&mut round, &grants, Some(*pending.clone()))
        .await
        .unwrap();
    drive_until(Duration::from_secs(2), async || {
        learner.state.read().await.prepared_join.as_ref() == Some(&pending)
    })
    .await;
    let acknowledged = drive(node.runtime.commit(
        &node.raft,
        KafRequest::AdmissionMembership(AdmissionCommand::LearnerApplied {
            consumer,
            request_nonce: pending.plan.request_nonce,
            prepared: pending.prepared,
        }),
    ))
    .await;
    let mutation = node.runtime.mutation.lock().await;
    let mut promotion = Box::pin(
        node.raft
            .change_membership(pending.plan.next_voters.clone(), false),
    );
    assert!(futures::poll!(promotion.as_mut()).is_pending());
    drive_until(Duration::from_secs(2), async || {
        let state = node.state.read().await;
        state.last_membership.membership().get_joint_config().len() == 2
            && state.last_membership.log_id().unwrap().index > acknowledged.index
    })
    .await;
    // Dropping the caller after the joint commit leaves the final stable commit to recovery.
    drop(promotion);
    assert!(events.text().is_empty(), "promotion is not stable yet");
    let error = node
        .runtime
        .cancel_join(&node.raft, leader, pending.plan.clone(), pending.prepared)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("voting transition cannot be cancelled")
    );
    let mut recovery = Box::pin(node.runtime.recover_pending(&node.network));
    assert!(futures::poll!(recovery.as_mut()).is_pending());
    drop(mutation);
    drive(async {
        if let Err(error) = recovery.await {
            // The supervised driver may finish the same committed promotion first.
            assert!(error.to_string().contains("pending promotion changed"));
        }
        Ok(())
    })
    .await;
    {
        let state = node.state.read().await;
        assert!(state.prepared_join.is_none());
        assert_eq!(
            state.last_membership.membership().get_joint_config().len(),
            1
        );
        assert_eq!(
            state
                .last_membership
                .membership()
                .voter_ids()
                .collect::<BTreeSet<_>>(),
            pending.plan.next_voters
        );
        let completed = state.completed_joins.get(&consumer.physical_id).unwrap();
        assert_eq!(completed.prepared.plan, pending.plan);
        assert_eq!(completed.prepared.learner_applied, Some(acknowledged));
        assert!(completed.promoted.index > acknowledged.index);
    }
    let expected = format!("committed learner promotion consumer={consumer}");
    assert!(
        events.text().contains(&expected),
        "missing exact-boot promotion evidence: {}",
        events.text()
    );
    drop(join_round);
    for node in &mut nodes {
        node.stop().await;
    }
}

fn prepared() -> PreparedJoin {
    let previous_voters = [1, 2, 3].map(test_replica).into();
    let consumer = ReplicaId::fresh(2).unwrap();
    PreparedJoin {
        plan: JoinPlan {
            genesis: Genesis {
                config: super::tests::config().cluster_config_fingerprint().unwrap(),
                epoch: 7,
                voters: previous_voters,
            },
            consumer,
            request_nonce: [7; 32],
            previous_voters: [1, 2, 3].map(test_replica).into(),
            next_voters: [test_replica(1), consumer, test_replica(3)].into(),
        },
        prepared: openraft::testing::log_id::<TypeConfig>(1, test_replica(1), 3),
        learner_applied: Some(openraft::testing::log_id::<TypeConfig>(
            1,
            test_replica(1),
            5,
        )),
    }
}

#[test]
fn cancellation_management_action_carries_the_exact_operation() {
    let pending = prepared();
    let encoded = serde_json::json!({
        "nonce": ([9; 32]),
        "action": {"CancelJoin": {"plan": pending.plan, "prepared": pending.prepared}}
    });
    let decoded = serde_json::from_value::<ManagementRequest>(encoded.clone());
    assert!(
        decoded.is_ok(),
        "exact cancellation action must be supported: {decoded:?}"
    );
    assert_eq!(serde_json::to_value(decoded.unwrap()).unwrap(), encoded);
}

#[tokio::test(start_paused = true)]
async fn reachable_learner_without_progress_reaches_cancellation_deadline() {
    let runtime = super::tests::driver();
    let pending = prepared();
    assert!(!runtime.observe_pending(&pending, None).unwrap());
    tokio::time::advance(runtime.timing.issuer_reservation() / 2).await;
    assert!(!runtime.observe_pending(&pending, None).unwrap());
    tokio::time::advance(runtime.timing.issuer_reservation() / 2).await;
    assert!(
        runtime.observe_pending(&pending, None).unwrap(),
        "successful discovery without learner progress must not renew the stall deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn replication_progress_restarts_the_deadline_for_one_exact_operation() {
    let runtime = super::tests::driver();
    let pending = prepared();
    assert!(!runtime.observe_pending(&pending, None).unwrap());
    tokio::time::advance(runtime.timing.issuer_reservation() / 2).await;
    assert!(!runtime.observe_pending(&pending, None).unwrap());
    assert!(!runtime.observe_pending(&pending, Some(1)).unwrap());
    assert!(!runtime.observe_pending(&pending, Some(1)).unwrap());
    tokio::time::advance(runtime.timing.issuer_reservation()).await;
    assert!(runtime.observe_pending(&pending, Some(1)).unwrap());
    let mut replacement = pending;
    replacement.plan.request_nonce[0] ^= 1;
    assert!(!runtime.observe_pending(&replacement, Some(1)).unwrap());
}

#[tokio::test(start_paused = true)]
async fn repeated_missing_or_regressed_replication_cannot_renew_the_stall_deadline() {
    for later in [None, Some(3), Some(4)] {
        let runtime = super::tests::driver();
        let pending = prepared();
        assert!(!runtime.observe_pending(&pending, Some(4)).unwrap());
        tokio::time::advance(runtime.timing.issuer_reservation() / 2).await;
        assert!(!runtime.observe_pending(&pending, later).unwrap());
        tokio::time::advance(runtime.timing.issuer_reservation() / 2).await;
        assert!(runtime.observe_pending(&pending, Some(4)).unwrap());
    }
}

#[tokio::test(start_paused = true)]
async fn a_committed_learner_acknowledgement_is_progress_but_repetition_is_not() {
    let runtime = super::tests::driver();
    let mut pending = prepared();
    let acknowledgement = pending.learner_applied.take();
    assert!(!runtime.observe_pending(&pending, Some(4)).unwrap());
    tokio::time::advance(runtime.timing.issuer_reservation()).await;
    pending.learner_applied = acknowledgement;
    assert!(!runtime.observe_pending(&pending, Some(4)).unwrap());
    tokio::time::advance(runtime.timing.issuer_reservation()).await;
    assert!(runtime.observe_pending(&pending, Some(4)).unwrap());
}

#[tokio::test(start_paused = true)]
async fn a_new_prepare_barrier_or_boot_gets_its_own_progress_window() {
    let runtime = super::tests::driver();
    let pending = prepared();
    assert!(!runtime.observe_pending(&pending, Some(2)).unwrap());
    tokio::time::advance(runtime.timing.issuer_reservation()).await;
    assert!(runtime.observe_pending(&pending, Some(2)).unwrap());
    let mut replacement = pending;
    replacement.prepared.index += 1;
    assert!(!runtime.observe_pending(&replacement, Some(2)).unwrap());
    tokio::time::advance(runtime.timing.issuer_reservation()).await;
    assert!(runtime.observe_pending(&replacement, Some(2)).unwrap());
    replacement.plan.consumer.boot_nonce[0] ^= 1;
    assert!(!runtime.observe_pending(&replacement, Some(2)).unwrap());
}

#[tokio::test(start_paused = true)]
async fn recovery_discards_observations_without_leadership_or_a_pending_join() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let submit_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = (*super::tests::config()).clone();
    cfg.raft_listen = listener.local_addr().unwrap().to_string();
    cfg.peers[0].raft_address = cfg.raft_listen.clone();
    cfg.client_submit_listen = submit_listener.local_addr().unwrap().to_string();
    cfg.peers[0].client_submit_address = cfg.client_submit_listen.clone();
    let cfg = Arc::new(cfg);
    let timing = LeaseTiming::for_config(&cfg, 0).unwrap();
    let (raft, network, state, _, _, mut controls) = crate::raft::start_raft(
        cfg,
        Arc::new(Vec::new()),
        crate::listener::ListenerSource::Bound(listener),
    )
    .await
    .unwrap();
    let runtime = controls.runtime();
    let pending = prepared();
    assert!(raft.current_leader().await.is_none());
    assert!(!runtime.observe_pending(&pending, Some(2)).unwrap());
    runtime.recover_pending(&network).await.unwrap();
    assert!(runtime.pending_progress.lock().unwrap().is_none());

    let deadline = Instant::now() + timing.restart_quarantine() + timing.consumer_use();
    loop {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        if state.read().await.genesis.is_some() && runtime.current().is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "genesis was not applied");
        tokio::time::advance(Duration::from_millis(10)).await;
    }
    assert_eq!(raft.current_leader().await, Some(runtime.local_replica()));
    assert!(state.read().await.prepared_join.is_none());
    assert!(!runtime.observe_pending(&pending, Some(2)).unwrap());
    runtime.recover_pending(&network).await.unwrap();
    assert!(runtime.pending_progress.lock().unwrap().is_none());
    controls.shutdown().await.unwrap();
    network.shutdown().await.unwrap();
    raft.shutdown().await.unwrap();
}

#[tokio::test]
async fn completed_join_receipt_survives_snapshot_and_later_membership() {
    let (_, mut sm, state) = crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
    let prepared = prepared();
    {
        let mut state = state.write().await;
        state.genesis = Some(prepared.plan.genesis.clone());
        state.prepared_join = Some(prepared.clone());
        state.last_membership = StoredMembership::new(
            Some(prepared.prepared),
            Membership::new_with_defaults(
                vec![prepared.plan.previous_voters.clone()],
                prepared
                    .plan
                    .previous_voters
                    .iter()
                    .copied()
                    .chain([prepared.plan.consumer]),
            ),
        );
    }
    let promoted = openraft::testing::log_id::<TypeConfig>(1, test_replica(1), 7);
    let final_membership = Membership::new_with_defaults(
        vec![prepared.plan.next_voters.clone()],
        prepared.plan.next_voters.clone(),
    );
    sm.apply(futures::stream::iter([Ok((
        EntryOf::<TypeConfig> {
            log_id: promoted,
            payload: EntryPayload::Membership(final_membership.clone()),
        },
        None,
    ))]))
    .await
    .unwrap();
    let snapshot = sm.build_snapshot().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
    let receipt = body["completed_joins"]["2"].clone();
    assert_eq!(
        receipt,
        serde_json::json!({"prepared": prepared, "promoted": promoted})
    );
    assert!(state.read().await.prepared_join.is_none());
    let mut later_voters = prepared.plan.next_voters.clone();
    later_voters.remove(&test_replica(1));
    later_voters.insert(ReplicaId::fresh(1).unwrap());
    sm.apply(futures::stream::iter([Ok((
        EntryOf::<TypeConfig> {
            log_id: openraft::testing::log_id::<TypeConfig>(1, test_replica(1), 8),
            payload: EntryPayload::Membership(Membership::new_with_defaults(
                vec![later_voters.clone()],
                later_voters,
            )),
        },
        None,
    ))]))
    .await
    .unwrap();
    let snapshot = sm.build_snapshot().await.unwrap();
    let (_, mut restored, restored_state) =
        crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
    restored
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    let snapshot = restored.build_snapshot().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
    assert_eq!(body["completed_joins"]["2"], receipt);
    assert!(restored_state.read().await.admission.is_none());
}

#[tokio::test]
async fn promotion_without_committed_acknowledgement_has_no_completion_receipt() {
    let (_, mut sm, state) = crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
    let mut prepared = prepared();
    prepared.learner_applied = None;
    state.write().await.prepared_join = Some(prepared.clone());
    sm.apply(futures::stream::iter([Ok((
        EntryOf::<TypeConfig> {
            log_id: openraft::testing::log_id::<TypeConfig>(1, test_replica(1), 7),
            payload: EntryPayload::Membership(Membership::new_with_defaults(
                vec![prepared.plan.next_voters.clone()],
                prepared.plan.next_voters.clone(),
            )),
        },
        None,
    ))]))
    .await
    .unwrap();
    let snapshot = sm.build_snapshot().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
    assert_eq!(body["completed_joins"], serde_json::json!({}));
}

#[tokio::test]
async fn cancellation_requires_committed_removal_and_preserves_completed_evidence() {
    use crate::raft::admission::AdmissionCommand;
    let (_, mut sm, state) = crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
    let prepared = prepared();
    {
        let mut state = state.write().await;
        state.genesis = Some(prepared.plan.genesis.clone());
        state.prepared_join = Some(prepared.clone());
        state.last_membership = StoredMembership::new(
            Some(prepared.prepared),
            Membership::new_with_defaults(
                vec![prepared.plan.previous_voters.clone()],
                prepared
                    .plan
                    .previous_voters
                    .iter()
                    .copied()
                    .chain([prepared.plan.consumer]),
            ),
        );
    }
    let cancellation = KafRequest::AdmissionMembership(AdmissionCommand::CancelJoin {
        plan: prepared.plan.clone(),
        prepared: prepared.prepared,
    });
    for (index, payload) in [
        (7, EntryPayload::Normal(cancellation.clone())),
        (
            8,
            EntryPayload::Membership(Membership::new_with_defaults(
                vec![prepared.plan.previous_voters.clone()],
                prepared.plan.previous_voters.clone(),
            )),
        ),
        (9, EntryPayload::Normal(cancellation)),
    ] {
        sm.apply(futures::stream::iter([Ok((
            EntryOf::<TypeConfig> {
                log_id: openraft::testing::log_id::<TypeConfig>(1, test_replica(1), index),
                payload,
            },
            None,
        ))]))
        .await
        .unwrap();
        assert_eq!(state.read().await.prepared_join.is_none(), index == 9);
    }
    let snapshot = sm.build_snapshot().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
    assert_eq!(body["completed_joins"], serde_json::json!({}));
}
