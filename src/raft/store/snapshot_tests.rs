use super::*;

#[tokio::test]
async fn prepared_join_snapshot_preserves_exact_barrier_nonce_and_full_epoch() {
    use crate::raft::admission::{Genesis, JoinPlan, PreparedJoin, ReplicaId};
    let old = ReplicaId {
        physical_id: 2,
        boot_nonce: [1; 32],
    };
    let consumer = ReplicaId {
        physical_id: 2,
        boot_nonce: [2; 32],
    };
    let genesis = Genesis {
        config: crate::config::ClusterConfigFingerprint {
            version: 1,
            digest: [8; 32],
        },
        epoch: u128::MAX,
        voters: BTreeSet::from([old]),
    };
    let pending = PreparedJoin {
        plan: JoinPlan {
            genesis: genesis.clone(),
            consumer,
            request_nonce: [255; 32],
            previous_voters: BTreeSet::from([old]),
            next_voters: BTreeSet::from([consumer]),
        },
        prepared: lid(3, 7),
        learner_applied: Some(lid(3, 11)),
    };
    let source = storage(&[], 3);
    {
        let mut state = source.state.write().await;
        state.genesis = Some(genesis.clone());
        state.cluster_epoch = Some(genesis.epoch);
        state.prepared_join = Some(pending.clone());
        state.last_applied_log = Some(lid(3, 11));
    }
    let snapshot = source.state.read().await.snapshot_at(lid(3, 11)).unwrap();
    let mut target = storage(&[], 3);
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    let state = target.state.read().await;
    assert_eq!(state.genesis, Some(genesis));
    assert_eq!(state.prepared_join, Some(pending));
    assert!(state.admission.is_none());
}

fn snapshot_fixture(semantics: FailoverSemantics) -> KafSnapshot {
    let vip = ip4(192, 0, 2, 2);
    let orphan = ip4(192, 0, 2, 10);
    KafSnapshot {
        completed_joins: BTreeMap::new(),
        prepared_join: None,
        genesis: None,
        applied_progress: BTreeMap::new(),
        last_applied: Some(lid(3, 20)),
        last_membership: StoredMembershipOf::<TypeConfig>::new(
            Some(lid(3, 1)),
            Membership::new_with_defaults(
                vec![[2, 3, 10].map(test_replica).into()],
                [2, 3, 10].map(test_replica),
            ),
        ),
        node_health: [(10, false), (3, true), (2, true)].into_iter().collect(),
        node_probe_ticks: [(10, 17), (3, 19), (2, 20)].into_iter().collect(),
        latest_probe_tick: 20,
        vip_assignments: [(
            vip,
            VipAssignment {
                holder: 2,
                generation: 7,
                previous_holder: Some(10),
                previous_holder_released: false,
                activation_tick: 21,
            },
        )]
        .into_iter()
        .collect(),
        vip_generation: [(orphan, 9), (vip, 7)].into_iter().collect(),
        vip_last_holder: [(orphan, 3), (vip, 2)].into_iter().collect(),
        cluster_epoch: Some(9876),
        node_recovery_tick: [(3, 19), (2, 18)].into_iter().collect(),
        node_failback_blocked: [10, 3].into_iter().collect(),
        failover_semantics: semantics,
        config_identity_enforced: true,
        node_nopreempt: [10, 2].into_iter().collect(),
        node_recovery_pending: [3, 10].into_iter().collect(),
    }
}

fn snapshot_value(snapshot: &KafSnapshot) -> serde_json::Value {
    let mut value = serde_json::to_value(snapshot).unwrap();
    for field in [
        "node_failback_blocked",
        "node_nopreempt",
        "node_recovery_pending",
    ] {
        value[field]
            .as_array_mut()
            .unwrap()
            .sort_by_key(|node| node.as_u64().unwrap());
    }
    value
}

fn snapshot_meta(snapshot: &KafSnapshot) -> SnapshotMetaOf<TypeConfig> {
    SnapshotMeta {
        last_log_id: snapshot.last_applied,
        last_membership: snapshot.last_membership.clone(),
        snapshot_id: "fixture".into(),
    }
}

#[tokio::test]
async fn snapshot_capture_preserves_bytes_after_collection_reinsertion() {
    let original = snapshot_fixture(FailoverSemantics::Legacy);
    let expected = serde_json::to_vec(&original).unwrap();
    for rotation in 0..16 {
        let store = storage_failback(&[], 3, false);
        let mut state = store.state.write().await;
        original.restore_into(&mut state);
        macro_rules! reinsert {
            ($($field:ident),+ $(,)?) => {
                $(
                    let mut values: Vec<_> = std::mem::take(&mut state.$field)
                        .into_iter().collect();
                    let offset = rotation % values.len();
                    values.rotate_left(offset);
                    state.$field.extend(values);
                )+
            };
        }
        reinsert!(
            node_health,
            node_probe_ticks,
            vip_assignments,
            vip_generation,
            vip_last_holder,
            node_recovery_tick,
            node_failback_blocked,
            node_nopreempt,
            node_recovery_pending,
        );
        assert_eq!(
            state
                .snapshot_at(original.last_applied.unwrap())
                .unwrap()
                .snapshot
                .into_inner(),
            expected,
            "snapshot bytes changed after collection reinsertion {rotation}",
        );
    }
}

#[tokio::test]
async fn snapshot_roundtrip_preserves_fields_and_normalizes_only_local_policy() {
    let vip = ip4(192, 0, 2, 2);
    for semantics in [FailoverSemantics::Legacy, FailoverSemantics::V2] {
        for failback in [false, true] {
            let original = snapshot_fixture(semantics);
            let mut expected = original.clone();
            match (semantics, failback) {
                (FailoverSemantics::Legacy, true) => expected.node_failback_blocked.clear(),
                (FailoverSemantics::Legacy, false) => {}
                (FailoverSemantics::V2, true) => {
                    expected.node_failback_blocked.clear();
                    expected.node_nopreempt.clear();
                }
                (FailoverSemantics::V2, false) => {
                    expected.node_failback_blocked.clear();
                    expected.node_recovery_tick.clear();
                    expected.node_recovery_pending.clear();
                }
            }
            let mut store = storage_failback_delay(&[vip], 8, failback, 6);
            let table = store.state.read().await.vip_list.clone();
            store
                .save_vote(&Vote::new(7, test_replica(2)))
                .await
                .unwrap();
            store.save_committed(Some(lid(3, 25))).await.unwrap();
            store
                .append_to_log([health_entry(25, 2, true)])
                .await
                .unwrap();
            store
                .install_snapshot(
                    &snapshot_meta(&original),
                    Cursor::new(serde_json::to_vec(&original).unwrap()),
                )
                .await
                .unwrap();

            let built = store
                .get_snapshot_builder()
                .await
                .build_snapshot()
                .await
                .unwrap();
            let current = store.get_current_snapshot().await.unwrap().unwrap();
            for snapshot in [built, current] {
                let decoded: KafSnapshot =
                    serde_json::from_slice(&snapshot.snapshot.into_inner()).unwrap();
                assert_eq!(snapshot_value(&decoded), snapshot_value(&expected));
                assert_eq!(snapshot.meta.last_log_id, original.last_applied);
                assert_eq!(snapshot.meta.last_membership, original.last_membership);
            }
            let state = store.state.read().await;
            assert_eq!(
                snapshot_value(state.current_snapshot.as_ref().unwrap()),
                snapshot_value(&original)
            );
            assert!(Arc::ptr_eq(&table, &state.vip_list));
            assert_eq!(
                (
                    state.stale_missed_probes,
                    state.failback,
                    state.failback_delay_ticks
                ),
                (8, failback, 6)
            );
            assert_eq!(state.vote, Some(Vote::new(7, test_replica(2))));
            assert_eq!(state.committed, Some(lid(3, 25)));
            assert_eq!(state.log.keys().copied().collect::<Vec<_>>(), vec![25]);
        }
    }
}

#[tokio::test]
async fn legacy_snapshot_missing_fields_restores_defaults_and_assignment_fences() {
    let vip = ip4(192, 0, 2, 2);
    let legacy = serde_json::json!({
        "last_applied": lid(1, 4),
        "last_membership": StoredMembershipOf::<TypeConfig>::default(),
        "node_health": { "2": true },
        "vip_assignments": { "192.0.2.2": { "holder": 2, "generation": 1 } }
    });
    let decoded: KafSnapshot = serde_json::from_value(legacy.clone()).unwrap();
    let mut store = storage(&[vip], 3);
    store.state.write().await.cluster_epoch = Some(123);
    store
        .install_snapshot(
            &snapshot_meta(&decoded),
            Cursor::new(serde_json::to_vec(&legacy).unwrap()),
        )
        .await
        .unwrap();
    let rebuilt = store
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let actual: KafSnapshot = serde_json::from_slice(&rebuilt.snapshot.into_inner()).unwrap();
    let mut expected = KafSnapshot {
        last_applied: Some(lid(1, 4)),
        ..KafSnapshot::default()
    };
    expected.node_health.insert(2, true);
    expected.vip_assignments.insert(
        vip,
        VipAssignment {
            holder: 2,
            generation: 1,
            previous_holder: None,
            previous_holder_released: false,
            activation_tick: 0,
        },
    );
    assert_eq!(snapshot_value(&actual), snapshot_value(&expected));
}

#[tokio::test]
async fn restored_snapshots_preserve_deterministic_ownership_and_release_fences() {
    let vip = ip4(192, 0, 2, 2);
    let mut first = storage(&[vip], 3);
    let mut second = storage(&[vip], 3);
    let original = snapshot_fixture(FailoverSemantics::Legacy);
    let encoded = serde_json::to_vec(&original).unwrap();
    let reordered = serde_json::to_vec(&snapshot_value(&original)).unwrap();
    assert_ne!(encoded, reordered);
    for (store, bytes) in [(&mut first, encoded), (&mut second, reordered)] {
        store
            .install_snapshot(&snapshot_meta(&original), Cursor::new(bytes))
            .await
            .unwrap();
        store
            .apply_to_state_machine(&[
                health_entry(21, 2, true),
                release_entry(22, 3, vip, 7),
                release_entry(23, 10, vip, 6),
            ])
            .await
            .unwrap();
        let fenced = assignment_of(store, vip).await.unwrap();
        assert_eq!(fenced, original.vip_assignments[&vip]);
        store
            .apply_to_state_machine(&[release_entry(24, 10, vip, 7)])
            .await
            .unwrap();
        assert!(
            assignment_of(store, vip)
                .await
                .unwrap()
                .previous_holder_released
        );
    }
    assert_eq!(
        first.state.read().await.vip_assignments,
        second.state.read().await.vip_assignments
    );
    assert_eq!(
        first
            .get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .snapshot
            .into_inner(),
        second
            .get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .snapshot
            .into_inner()
    );
}

#[tokio::test]
async fn snapshots_preserve_coherent_prefix_and_vip_fences_between_apply_entries() {
    use futures::StreamExt;
    use std::task::Poll;

    let vip = ip4(192, 0, 2, 2);
    let mut store = storage(&[vip], 3);
    store
        .apply_to_state_machine(&[
            membership_entry(1, &[1, 2]),
            health_entry(2, 1, true),
            health_entry(3, 2, true),
        ])
        .await
        .unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
    let first = stream::iter([Ok::<_, io::Error>((health_entry(4, 1, false), None))]);
    let second = stream::once(async move {
        started_tx.send(()).unwrap();
        continue_rx.await.unwrap();
        Ok((health_entry(5, 2, true), None))
    });
    let mut writer = store.sm.clone();
    let applying = tokio::spawn(async move { writer.apply(first.chain(second).boxed()).await });
    started_rx.await.unwrap();
    let mut builder = store.sm.clone();
    let prefix_snapshot = {
        let mut snapshot = Box::pin(builder.build_snapshot());
        match futures::poll!(&mut snapshot) {
            Poll::Ready(result) => result.unwrap(),
            Poll::Pending => panic!("snapshot blocked while apply awaited the next entry"),
        }
    };
    let prefix: KafSnapshot = serde_json::from_slice(prefix_snapshot.snapshot.get_ref()).unwrap();
    assert!(!applying.is_finished());
    assert_eq!(prefix.last_applied, Some(lid(1, 4)));
    assert_eq!(prefix_snapshot.meta.last_log_id, Some(lid(1, 4)));
    assert_eq!(prefix_snapshot.meta.last_log_id, prefix.last_applied);
    assert_eq!(prefix_snapshot.meta.last_membership, prefix.last_membership);
    assert_eq!(prefix.node_health, BTreeMap::from([(1, false), (2, true)]));
    assert_eq!(prefix.node_probe_ticks, BTreeMap::from([(1, 2), (2, 1)]));
    assert_eq!(prefix.latest_probe_tick, 2);
    assert_eq!(
        prefix.vip_assignments,
        BTreeMap::from([(
            vip,
            VipAssignment {
                holder: 2,
                generation: 2,
                previous_holder: Some(1),
                previous_holder_released: false,
                activation_tick: 2 + OWNERSHIP_ACTIVATION_HOLDOFF_TICKS,
            },
        )])
    );
    assert_eq!(prefix.vip_generation.get(&vip), Some(&2));
    {
        let state = store.state.read().await;
        assert_eq!(
            serde_json::to_value(&prefix).unwrap(),
            serde_json::to_value(KafSnapshot::from(&*state)).unwrap()
        );
    }
    continue_tx.send(()).unwrap();
    applying.await.unwrap().unwrap();
    let snapshot = builder.build_snapshot().await.unwrap();
    let decoded: KafSnapshot = serde_json::from_slice(&snapshot.snapshot.into_inner()).unwrap();
    assert_eq!(snapshot.meta.last_log_id, Some(lid(1, 5)));
    assert_eq!(snapshot.meta.last_log_id, decoded.last_applied);
    assert_eq!(snapshot.meta.last_membership, decoded.last_membership);
    assert_eq!(decoded.last_applied, Some(lid(1, 5)));
    assert_eq!(decoded.node_health.get(&1), Some(&false));
    assert_eq!(decoded.node_health.get(&2), Some(&true));
    assert_eq!(decoded.node_probe_ticks, BTreeMap::from([(1, 2), (2, 2)]));
    assert_eq!(decoded.latest_probe_tick, 2);
    assert_eq!(decoded.vip_assignments, prefix.vip_assignments);
    assert_eq!(decoded.vip_generation, prefix.vip_generation);
    assert_eq!(decoded.vip_assignments[&vip].holder, 2);
    assert_eq!(decoded.vip_assignments[&vip].previous_holder, Some(1));
    assert!(!decoded.vip_assignments[&vip].previous_holder_released);
    assert_eq!(
        decoded.vip_assignments[&vip],
        store.state.read().await.vip_assignments[&vip]
    );
}

#[tokio::test]
async fn snapshot_serialization_orders_all_replicated_maps_and_sets() {
    let store = storage(&[], 3);
    let mut state = store.state.write().await;
    state.last_applied_log = Some(lid(1, 30));
    for node in (1..=16).rev() {
        let vip = ip4(192, 0, 2, node as u8);
        state.node_health.insert(node, true);
        state.node_probe_ticks.insert(node, node);
        state.node_recovery_tick.insert(node, node);
        state.node_failback_blocked.insert(node);
        state.node_nopreempt.insert(node);
        state.node_recovery_pending.insert(node);
        state.vip_generation.insert(vip, node);
        state.vip_last_holder.insert(vip, node);
        state.vip_assignments.insert(
            vip,
            VipAssignment {
                holder: node,
                generation: node,
                previous_holder: None,
                previous_holder_released: true,
                activation_tick: node,
            },
        );
    }
    macro_rules! ordered_field {
        ($field:ident, $collection:ident) => {
            (
                stringify!($field),
                serde_json::to_string(&state.$field.iter().collect::<$collection<_, _>>()).unwrap(),
            )
        };
    }
    let mut expected = vec![
        ordered_field!(node_health, BTreeMap),
        ordered_field!(node_probe_ticks, BTreeMap),
        ordered_field!(node_recovery_tick, BTreeMap),
        ordered_field!(vip_assignments, BTreeMap),
        ordered_field!(vip_generation, BTreeMap),
        ordered_field!(vip_last_holder, BTreeMap),
    ];
    for (name, values) in [
        ("node_failback_blocked", &state.node_failback_blocked),
        ("node_nopreempt", &state.node_nopreempt),
        ("node_recovery_pending", &state.node_recovery_pending),
    ] {
        expected.push((
            name,
            serde_json::to_string(&values.iter().collect::<BTreeSet<_>>()).unwrap(),
        ));
    }
    let encoded =
        String::from_utf8(state.snapshot_at(lid(1, 30)).unwrap().snapshot.into_inner()).unwrap();
    for (name, value) in expected {
        let field = format!("\"{name}\":{value}");
        assert!(
            encoded.contains(&field),
            "snapshot field {name} is not ordered: {encoded}"
        );
    }
}
