use super::*;
use crate::bind_policy::should_bind_vip;

async fn ownerless_store(vip: IpAddr, v2: bool) -> TestStore {
    let mut store = storage(&[vip], 3);
    if v2 {
        enable_v2(&store).await;
    }
    store
        .apply_to_state_machine(&[
            membership_entry(1, &[1, 2]),
            health_entry(2, 1, true),
            health_entry(3, 1, false),
            health_entry(4, 2, false),
        ])
        .await
        .unwrap();
    assert!(store.state.read().await.vip_assignments.is_empty());
    store
}

async fn may_bind(store: &TestStore, vip: IpAddr, node: u64) -> bool {
    let state = store.state.read().await;
    should_bind_vip(
        true,
        true,
        true,
        node,
        state.vip_assignments.get(&vip),
        &state.node_probe_ticks,
        state.latest_probe_tick,
        state.stale_missed_probes,
    )
}

#[tokio::test]
async fn fresh_previous_holder_fences_reassignment_after_ownerless_gap() {
    for v2 in [false, true] {
        let vip = ip4(192, 0, 2, 1);
        let mut store = ownerless_store(vip, v2).await;
        store
            .apply_to_state_machine(&[
                health_entry(5, 2, true),
                health_entry(6, 1, false),
                health_entry(7, 2, true),
            ])
            .await
            .unwrap();

        assert!(
            !may_bind(&store, vip, 2).await,
            "a fresh unhealthy holder has not confirmed kernel cleanup"
        );
        let assignment = assignment_of(&store, vip).await.unwrap();
        assert_eq!(assignment.previous_holder, Some(1));
        assert!(!assignment.previous_holder_released);
        assert_eq!(assignment.generation, 2);

        store
            .apply_to_state_machine(&[
                release_entry(8, 2, vip, assignment.generation),
                release_entry(9, 1, vip, assignment.generation - 1),
            ])
            .await
            .unwrap();
        assert!(!may_bind(&store, vip, 2).await);
        store
            .apply_to_state_machine(&[release_entry(10, 1, vip, assignment.generation)])
            .await
            .unwrap();
        assert!(may_bind(&store, vip, 2).await);
    }
}

#[tokio::test]
async fn ownerless_snapshot_retains_the_previous_holder_fence() {
    let vip = ip4(192, 0, 2, 1);
    let mut source = ownerless_store(vip, true).await;
    let snapshot = source
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let mut restored = storage(&[vip], 3);
    restored
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    restored
        .apply_to_state_machine(&[
            health_entry(5, 2, true),
            health_entry(6, 1, false),
            health_entry(7, 2, true),
        ])
        .await
        .unwrap();
    assert!(
        !may_bind(&restored, vip, 2).await,
        "snapshot catch-up must not turn a handoff into a cold assignment"
    );
}

#[tokio::test]
async fn returning_same_holder_can_acknowledge_its_ownerless_gap_cleanup() {
    let vip = ip4(192, 0, 2, 1);
    let mut store = ownerless_store(vip, true).await;
    store
        .apply_to_state_machine(&[health_entry(5, 1, true), health_entry(6, 1, true)])
        .await
        .unwrap();
    assert!(!may_bind(&store, vip, 1).await);
    let assignment = assignment_of(&store, vip).await.unwrap();
    assert_eq!(assignment.previous_holder, Some(1));
    store
        .apply_to_state_machine(&[release_entry(7, 1, vip, assignment.generation)])
        .await
        .unwrap();
    assert!(may_bind(&store, vip, 1).await);
}

#[tokio::test]
async fn repeated_ownerless_gaps_fence_against_the_latest_holder() {
    let vip = ip4(192, 0, 2, 1);
    let mut store = ownerless_store(vip, true).await;
    store
        .apply_to_state_machine(&[
            health_entry(5, 2, true),
            health_entry(6, 1, false),
            health_entry(7, 2, true),
            release_entry(8, 1, vip, 2),
        ])
        .await
        .unwrap();
    assert!(may_bind(&store, vip, 2).await);

    store
        .apply_to_state_machine(&[
            health_entry(9, 2, false),
            health_entry(10, 1, false),
            health_entry(11, 2, false),
        ])
        .await
        .unwrap();
    assert!(store.state.read().await.vip_assignments.is_empty());
    store
        .apply_to_state_machine(&[
            health_entry(12, 1, true),
            health_entry(13, 2, false),
            health_entry(14, 1, true),
        ])
        .await
        .unwrap();
    let assignment = assignment_of(&store, vip).await.unwrap();
    assert_eq!(assignment.generation, 3);
    assert_eq!(assignment.previous_holder, Some(2));
    assert!(!may_bind(&store, vip, 1).await);

    store
        .apply_to_state_machine(&[release_entry(15, 2, vip, 3)])
        .await
        .unwrap();
    assert!(may_bind(&store, vip, 1).await);
}

#[tokio::test]
async fn active_legacy_snapshot_can_seed_holder_history_before_a_gap() {
    let vip = ip4(192, 0, 2, 1);
    let mut source = storage(&[vip], 3);
    source
        .apply_to_state_machine(&[membership_entry(1, &[1, 2]), health_entry(2, 1, true)])
        .await
        .unwrap();
    let snapshot = source
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let mut payload: serde_json::Value =
        serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
    payload.as_object_mut().unwrap().remove("vip_last_holder");
    let mut restored = storage(&[vip], 3);
    restored
        .install_snapshot(
            &snapshot.meta,
            Cursor::new(serde_json::to_vec(&payload).unwrap()),
        )
        .await
        .unwrap();
    restored
        .apply_to_state_machine(&[
            health_entry(3, 1, false),
            health_entry(4, 2, false),
            health_entry(5, 2, true),
            health_entry(6, 1, false),
            health_entry(7, 2, true),
        ])
        .await
        .unwrap();
    assert_eq!(
        assignment_of(&restored, vip).await.unwrap().previous_holder,
        Some(1)
    );
    assert!(!may_bind(&restored, vip, 2).await);
}

#[tokio::test]
async fn stale_previous_holder_does_not_permanently_block_reassignment() {
    let vip = ip4(192, 0, 2, 1);
    let mut store = ownerless_store(vip, true).await;
    store
        .apply_to_state_machine(&[health_entry(5, 2, true), health_entry(6, 2, true)])
        .await
        .unwrap();
    assert!(!may_bind(&store, vip, 2).await);
    store
        .apply_to_state_machine(&[
            health_entry(7, 2, true),
            health_entry(8, 2, true),
            health_entry(9, 2, true),
        ])
        .await
        .unwrap();
    let assignment = assignment_of(&store, vip).await.unwrap();
    assert_eq!(assignment.previous_holder, Some(1));
    assert!(!assignment.previous_holder_released);
    assert!(may_bind(&store, vip, 2).await);
}
