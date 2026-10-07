use super::*;
use crate::raft::admission::{AdmissionCommand, Genesis, HealthProgress, JoinPlan, ReplicaId};
use crate::raft::types::{KafResponse, test_replica};

fn join_plan() -> JoinPlan {
    let previous_voters: BTreeSet<_> = [1, 2, 3].map(test_replica).into();
    let consumer = ReplicaId {
        physical_id: 2,
        boot_nonce: [99; 32],
    };
    let next_voters = BTreeSet::from([test_replica(1), consumer, test_replica(3)]);
    JoinPlan {
        genesis: Genesis {
            config: crate::config::ClusterConfigFingerprint {
                version: 1,
                digest: [8; 32],
            },
            epoch: u128::MAX,
            voters: previous_voters.clone(),
        },
        consumer,
        request_nonce: [11; 32],
        previous_voters,
        next_voters,
    }
}

fn boot_membership(
    index: u64,
    voters: BTreeSet<ReplicaId>,
    learners: &[ReplicaId],
) -> EntryOf<TypeConfig> {
    let nodes: BTreeSet<_> = voters
        .iter()
        .copied()
        .chain(learners.iter().copied())
        .collect();
    log_entry(
        index,
        EntryPayload::Membership(Membership::new_with_defaults(vec![voters], nodes)),
    )
}

async fn admitted_store(vips: &[IpAddr], plan: &JoinPlan) -> TestStore {
    let mut store = storage(vips, 3);
    store
        .apply_to_state_machine(&[
            boot_membership(1, plan.previous_voters.clone(), &[]),
            log_entry(
                2,
                EntryPayload::Normal(KafRequest::AdmissionGenesis(plan.genesis.clone())),
            ),
        ])
        .await
        .unwrap();
    store
}

#[tokio::test]
async fn admitted_genesis_enables_v2_and_config_identity() {
    let plan = join_plan();
    let mut store = admitted_store(&[], &plan).await;
    {
        let state = store.state.read().await;
        assert_eq!(state.genesis.as_ref(), Some(&plan.genesis));
        assert_eq!(state.cluster_epoch, Some(plan.genesis.epoch));
        assert_eq!(state.failover_semantics, FailoverSemantics::V2);
        assert!(state.config_identity_enforced);
    }
    let mut foreign = plan.genesis.clone();
    foreign.epoch -= 1;
    store
        .apply_to_state_machine(&[
            log_entry(
                3,
                EntryPayload::Normal(KafRequest::AdmissionGenesis(plan.genesis.clone())),
            ),
            log_entry(
                4,
                EntryPayload::Normal(KafRequest::AdmissionGenesis(foreign)),
            ),
        ])
        .await
        .unwrap();
    let state = store.state.read().await;
    assert_eq!(state.genesis.as_ref(), Some(&plan.genesis));
    assert_eq!(state.cluster_epoch, Some(plan.genesis.epoch));
    assert_eq!(state.failover_semantics, FailoverSemantics::V2);
    assert!(state.config_identity_enforced);
}

async fn command(store: &mut TestStore, index: u64, command: AdmissionCommand) {
    store
        .apply_to_state_machine(&[log_entry(
            index,
            EntryPayload::Normal(KafRequest::AdmissionMembership(command)),
        )])
        .await
        .unwrap();
}

#[tokio::test]
async fn prepare_join_is_serialized_and_idempotent_until_acknowledged_final_membership() {
    let (events, _guard) = crate::warning_limit::test_support::LogCapture::start(
        "keepafloatd::raft::store::membership=info",
    );
    let plan = join_plan();
    let mut store = admitted_store(&[], &plan).await;
    command(&mut store, 3, AdmissionCommand::PrepareJoin(plan.clone())).await;
    let prepared = store
        .state
        .read()
        .await
        .prepared_join
        .clone()
        .expect("prepare was not applied");
    assert_eq!(prepared.prepared, lid(1, 3));
    assert_eq!(prepared.plan, plan);
    command(&mut store, 4, AdmissionCommand::PrepareJoin(plan.clone())).await;
    assert_eq!(
        store.state.read().await.prepared_join,
        Some(prepared.clone())
    );
    let mut competing = plan.clone();
    competing.request_nonce = [12; 32];
    command(&mut store, 5, AdmissionCommand::PrepareJoin(competing)).await;
    assert_eq!(
        store.state.read().await.prepared_join,
        Some(prepared.clone())
    );

    store
        .apply_to_state_machine(&[boot_membership(
            6,
            plan.previous_voters.clone(),
            &[plan.consumer],
        )])
        .await
        .unwrap();
    command(
        &mut store,
        7,
        AdmissionCommand::LearnerApplied {
            consumer: plan.consumer,
            request_nonce: plan.request_nonce,
            prepared: prepared.prepared,
        },
    )
    .await;
    assert_eq!(
        store
            .state
            .read()
            .await
            .prepared_join
            .as_ref()
            .unwrap()
            .learner_applied,
        Some(lid(1, 7))
    );
    let joint = Membership::new_with_defaults(
        vec![plan.previous_voters.clone(), plan.next_voters.clone()],
        plan.previous_voters.union(&plan.next_voters).copied(),
    );
    store
        .apply_to_state_machine(&[log_entry(8, EntryPayload::Membership(joint))])
        .await
        .unwrap();
    assert!(store.state.read().await.prepared_join.is_some());
    assert!(
        events.text().is_empty(),
        "joint membership must not report a completed promotion"
    );
    store
        .apply_to_state_machine(&[boot_membership(9, plan.next_voters.clone(), &[])])
        .await
        .unwrap();
    assert!(store.state.read().await.prepared_join.is_none());
    assert_eq!(store.state.read().await.genesis, Some(plan.genesis));
    let event = format!("committed learner promotion consumer={}", plan.consumer);
    assert!(
        events.text().contains(&event),
        "missing applied promotion evidence: {}",
        events.text()
    );
    assert_eq!(events.text().lines().count(), 1);
    store
        .apply_to_state_machine(&[boot_membership(10, plan.next_voters.clone(), &[])])
        .await
        .unwrap();
    assert_eq!(
        events.text().lines().count(),
        1,
        "stable membership replay must not duplicate evidence"
    );
}

#[tokio::test]
async fn prepare_join_rejects_foreign_genesis_stale_voters_and_unrelated_replacement() {
    let plan = join_plan();
    let mut foreign = plan.clone();
    foreign.genesis.epoch -= 1;
    let mut stale = plan.clone();
    stale.previous_voters.remove(&test_replica(3));
    stale.next_voters.remove(&test_replica(3));
    let mut unrelated = plan.clone();
    unrelated.next_voters.remove(&test_replica(1));
    for invalid in [foreign, stale, unrelated] {
        let mut store = admitted_store(&[], &plan).await;
        command(&mut store, 3, AdmissionCommand::PrepareJoin(invalid)).await;
        assert!(store.state.read().await.prepared_join.is_none());
        assert_eq!(store.state.read().await.genesis, Some(plan.genesis.clone()));
    }
}

#[tokio::test]
async fn learner_applied_requires_exact_boot_nonce_barrier_and_learner_role() {
    let plan = join_plan();
    let mut store = admitted_store(&[], &plan).await;
    command(&mut store, 3, AdmissionCommand::PrepareJoin(plan.clone())).await;
    command(
        &mut store,
        4,
        AdmissionCommand::LearnerApplied {
            consumer: plan.consumer,
            request_nonce: plan.request_nonce,
            prepared: lid(1, 3),
        },
    )
    .await;
    assert!(
        store
            .state
            .read()
            .await
            .prepared_join
            .as_ref()
            .unwrap()
            .learner_applied
            .is_none()
    );
    store
        .apply_to_state_machine(&[boot_membership(
            5,
            plan.previous_voters.clone(),
            &[plan.consumer],
        )])
        .await
        .unwrap();
    for (consumer, nonce, prepared) in [
        (test_replica(2), plan.request_nonce, lid(1, 3)),
        (plan.consumer, [12; 32], lid(1, 3)),
        (plan.consumer, plan.request_nonce, lid(2, 3)),
        (plan.consumer, plan.request_nonce, lid(1, 4)),
    ] {
        command(
            &mut store,
            6,
            AdmissionCommand::LearnerApplied {
                consumer,
                request_nonce: nonce,
                prepared,
            },
        )
        .await;
        assert!(
            store
                .state
                .read()
                .await
                .prepared_join
                .as_ref()
                .unwrap()
                .learner_applied
                .is_none()
        );
    }
    command(
        &mut store,
        7,
        AdmissionCommand::LearnerApplied {
            consumer: plan.consumer,
            request_nonce: plan.request_nonce,
            prepared: lid(1, 3),
        },
    )
    .await;
    command(
        &mut store,
        8,
        AdmissionCommand::LearnerApplied {
            consumer: plan.consumer,
            request_nonce: plan.request_nonce,
            prepared: lid(1, 3),
        },
    )
    .await;
    assert_eq!(
        store
            .state
            .read()
            .await
            .prepared_join
            .as_ref()
            .unwrap()
            .learner_applied,
        Some(lid(1, 7))
    );
}

fn progress(plan: &JoinPlan, replica: ReplicaId) -> HealthProgress {
    HealthProgress {
        node_id: replica.physical_id,
        healthy: Some(true),
        replica,
        epoch: plan.genesis.epoch,
        request_nonce: [27; 32],
        genesis: plan.genesis.clone(),
    }
}

#[tokio::test]
async fn admission_only_progress_records_challenge_without_health_ticks_or_placement_changes() {
    let plan = join_plan();
    let replica = test_replica(2);
    let vip = ip4(192, 0, 2, 43);
    for initial_health in [Some(true), Some(false), None] {
        let mut store = admitted_store(&[vip], &plan).await;
        if let Some(healthy) = initial_health {
            let mut probe = progress(&plan, replica);
            probe.healthy = Some(healthy);
            store
                .apply_to_state_machine(&[log_entry(
                    3,
                    EntryPayload::Normal(KafRequest::HealthProgress(probe)),
                )])
                .await
                .unwrap();
        }
        let before = KafSnapshot::from(&*store.state.read().await);
        assert_eq!(before.node_health.get(&2).copied(), initial_health);
        assert_eq!(
            before.vip_assignments.contains_key(&vip),
            initial_health == Some(true)
        );
        for index in 4..7 {
            let mut renewal = progress(&plan, replica);
            renewal.healthy = None;
            renewal.request_nonce = [index as u8; 32];
            store
                .apply_to_state_machine(&[log_entry(
                    index,
                    EntryPayload::Normal(KafRequest::HealthProgress(renewal.clone())),
                )])
                .await
                .unwrap();
            let state = store.state.read().await;
            assert_eq!(state.applied_progress[&2].request, renewal);
            assert_eq!(state.applied_progress[&2].log_id, lid(1, index));
            assert_eq!(state.node_health, before.node_health);
            assert_eq!(state.node_probe_ticks, before.node_probe_ticks);
            assert_eq!(state.latest_probe_tick, before.latest_probe_tick);
            assert_eq!(state.node_recovery_tick, before.node_recovery_tick);
            assert_eq!(state.vip_assignments, before.vip_assignments);
            assert_eq!(state.vip_generation, before.vip_generation);
            assert_eq!(state.vip_last_holder, before.vip_last_holder);
        }
    }
}

#[tokio::test]
async fn replacement_boot_cannot_inherit_health_or_progress_and_requires_its_own_apply() {
    let plan = join_plan();
    let vip = ip4(192, 0, 2, 41);
    let mut store = admitted_store(&[vip], &plan).await;
    store
        .apply_to_state_machine(&[log_entry(
            3,
            EntryPayload::Normal(KafRequest::HealthProgress(progress(&plan, test_replica(2)))),
        )])
        .await
        .unwrap();
    assert_eq!(store.state.read().await.vip_assignments[&vip].holder, 2);
    store
        .apply_to_state_machine(&[boot_membership(4, plan.next_voters.clone(), &[])])
        .await
        .unwrap();
    {
        let state = store.state.read().await;
        assert!(!state.node_health.get(&2).copied().unwrap_or(false));
        assert!(!state.applied_progress.contains_key(&2));
        assert!(!state.vip_assignments.contains_key(&vip));
    }
    store
        .apply_to_state_machine(&[log_entry(
            5,
            EntryPayload::Normal(KafRequest::HealthProgress(progress(&plan, test_replica(2)))),
        )])
        .await
        .unwrap();
    assert!(!store.state.read().await.applied_progress.contains_key(&2));
    store
        .apply_to_state_machine(&[log_entry(
            6,
            EntryPayload::Normal(KafRequest::HealthProgress(progress(&plan, plan.consumer))),
        )])
        .await
        .unwrap();
    let state = store.state.read().await;
    assert_eq!(state.applied_progress[&2].request.replica, plan.consumer);
    assert_eq!(
        state.vip_assignments[&vip].holder, 2,
        "new proof must precede placement recomputation"
    );
}

#[tokio::test]
async fn learner_progress_and_physical_only_health_cannot_make_a_vip_owner() {
    let plan = join_plan();
    let vip = ip4(192, 0, 2, 42);
    let mut store = admitted_store(&[vip], &plan).await;
    store
        .apply_to_state_machine(&[health_entry(3, 2, true)])
        .await
        .unwrap();
    assert!(store.state.read().await.vip_assignments.is_empty());
    command(&mut store, 4, AdmissionCommand::PrepareJoin(plan.clone())).await;
    store
        .apply_to_state_machine(&[boot_membership(
            5,
            plan.previous_voters.clone(),
            &[plan.consumer],
        )])
        .await
        .unwrap();
    store
        .apply_to_state_machine(&[log_entry(
            6,
            EntryPayload::Normal(KafRequest::HealthProgress(progress(&plan, plan.consumer))),
        )])
        .await
        .unwrap();
    let state = store.state.read().await;
    assert_eq!(state.applied_progress[&2].request.replica, plan.consumer);
    assert!(state.vip_assignments.is_empty());
    assert_eq!(state.node_health.get(&2), Some(&false));
}

#[tokio::test]
async fn final_membership_without_exact_learner_ack_does_not_release_join_slot() {
    let plan = join_plan();
    let mut store = admitted_store(&[], &plan).await;
    command(&mut store, 3, AdmissionCommand::PrepareJoin(plan.clone())).await;
    store
        .apply_to_state_machine(&[boot_membership(4, plan.next_voters.clone(), &[])])
        .await
        .unwrap();
    assert!(store.state.read().await.prepared_join.is_some());
}

#[tokio::test]
async fn cancellation_requires_exact_plan_and_prepare_barrier() {
    let plan = join_plan();
    let mut store = admitted_store(&[], &plan).await;
    let cancel = AdmissionCommand::CancelJoin {
        plan: plan.clone(),
        prepared: lid(1, 3),
    };
    assert!(matches!(
        store
            .state
            .write()
            .await
            .apply_admission_membership(&cancel, lid(1, 4)),
        KafResponse::Rejected(_)
    ));
    command(&mut store, 3, AdmissionCommand::PrepareJoin(plan.clone())).await;
    let original = store.state.read().await.prepared_join.clone();
    assert!(matches!(
        store
            .state
            .write()
            .await
            .apply_admission_membership(&cancel, lid(1, 3)),
        KafResponse::Rejected(_)
    ));
    assert_eq!(store.state.read().await.prepared_join, original);
    store.state.write().await.genesis = None;
    assert!(matches!(
        store
            .state
            .write()
            .await
            .apply_admission_membership(&cancel, lid(1, 4)),
        KafResponse::Rejected(_)
    ));
    assert_eq!(store.state.read().await.prepared_join, original);
    store.state.write().await.genesis = Some(plan.genesis.clone());
    for field in [
        "nonce", "consumer", "genesis", "previous", "next", "barrier",
    ] {
        let mut wrong = plan.clone();
        let mut prepared = lid(1, 3);
        match field {
            "nonce" => wrong.request_nonce[0] ^= 1,
            "consumer" => wrong.consumer.boot_nonce[0] ^= 1,
            "genesis" => wrong.genesis.epoch -= 1,
            "previous" => {
                wrong.previous_voters.remove(&test_replica(1));
            }
            "next" => {
                wrong.next_voters.remove(&test_replica(1));
            }
            _ => prepared = lid(2, 3),
        }
        let response = store.state.write().await.apply_admission_membership(
            &AdmissionCommand::CancelJoin {
                plan: wrong,
                prepared,
            },
            lid(1, 4),
        );
        assert!(matches!(response, KafResponse::Rejected(_)), "{field}");
        assert_eq!(store.state.read().await.prepared_join, original, "{field}");
    }
    command(
        &mut store,
        5,
        AdmissionCommand::CancelJoin {
            plan: plan.clone(),
            prepared: lid(1, 3),
        },
    )
    .await;
    assert!(store.state.read().await.prepared_join.is_none());
    assert_eq!(store.state.read().await.genesis, Some(plan.genesis.clone()));
    command(&mut store, 6, AdmissionCommand::PrepareJoin(plan.clone())).await;
    command(
        &mut store,
        7,
        AdmissionCommand::CancelJoin {
            plan,
            prepared: lid(1, 3),
        },
    )
    .await;
    assert_eq!(
        store
            .state
            .read()
            .await
            .prepared_join
            .as_ref()
            .unwrap()
            .prepared,
        lid(1, 6)
    );
}

#[tokio::test]
async fn cancellation_requires_learner_removal_and_cannot_cancel_joint_or_final_voters() {
    let plan = join_plan();
    for stage in ["learner", "acknowledged", "joint", "final"] {
        let mut store = admitted_store(&[], &plan).await;
        command(&mut store, 3, AdmissionCommand::PrepareJoin(plan.clone())).await;
        store
            .apply_to_state_machine(&[boot_membership(
                4,
                plan.previous_voters.clone(),
                &[plan.consumer],
            )])
            .await
            .unwrap();
        if stage == "acknowledged" {
            command(
                &mut store,
                5,
                AdmissionCommand::LearnerApplied {
                    consumer: plan.consumer,
                    request_nonce: plan.request_nonce,
                    prepared: lid(1, 3),
                },
            )
            .await;
        }
        if stage == "joint" {
            let membership = Membership::new_with_defaults(
                vec![plan.previous_voters.clone(), plan.next_voters.clone()],
                plan.previous_voters.union(&plan.next_voters).copied(),
            );
            store
                .apply_to_state_machine(&[log_entry(5, EntryPayload::Membership(membership))])
                .await
                .unwrap();
        } else if stage == "final" {
            store
                .apply_to_state_machine(&[boot_membership(5, plan.next_voters.clone(), &[])])
                .await
                .unwrap();
        }
        let pending = store.state.read().await.prepared_join.clone();
        let response = store.state.write().await.apply_admission_membership(
            &AdmissionCommand::CancelJoin {
                plan: plan.clone(),
                prepared: lid(1, 3),
            },
            lid(1, 6),
        );
        assert!(matches!(response, KafResponse::Rejected(_)), "{stage}");
        assert_eq!(store.state.read().await.prepared_join, pending, "{stage}");
        if matches!(stage, "learner" | "acknowledged") {
            store
                .apply_to_state_machine(&[boot_membership(7, plan.previous_voters.clone(), &[])])
                .await
                .unwrap();
            command(
                &mut store,
                8,
                AdmissionCommand::CancelJoin {
                    plan: plan.clone(),
                    prepared: lid(1, 3),
                },
            )
            .await;
            assert!(store.state.read().await.prepared_join.is_none(), "{stage}");
        }
    }
}
