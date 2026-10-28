use super::*;
use crate::consensus_freshness::ConsensusFreshness;
use std::time::Duration;

fn progress(
    runtime: &RuntimeDriver,
    started: Instant,
    index: u64,
    healthy: Option<bool>,
) -> VerifiedProgress {
    VerifiedProgress::for_test(
        runtime,
        started,
        openraft::testing::log_id::<TypeConfig>(1, runtime.local, index),
        healthy,
    )
    .unwrap()
}

async fn fixture() -> (Arc<RuntimeDriver>, Arc<ConsensusFreshness>) {
    let runtime = super::super::tests::admit_driver().await;
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_millis(100)));
    freshness
        .record_applied(progress(&runtime, Instant::now(), 1, Some(true)))
        .unwrap();
    (runtime, freshness)
}

#[tokio::test(start_paused = true)]
async fn unavailable_disables_acquisition_without_changing_completed_retention() {
    let (runtime, freshness) = fixture().await;
    let acquisition = freshness.acquire(true).unwrap();
    let stamp = acquisition.stamp();
    let retained = freshness.retain(&stamp).unwrap();
    let deadline = retained.deadline();
    freshness.mark_unavailable();
    assert!(!acquisition.check());
    assert!(freshness.acquire(true).is_none());
    assert!(freshness.acquire(false).is_none());
    assert!(retained.check());
    assert_eq!(freshness.retain(&stamp).unwrap().deadline(), deadline);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(!retained.check());
    assert!(freshness.retain(&stamp).is_none());
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn late_applied_proof_cannot_revive_a_completed_stamp_even_without_polling() {
    let (runtime, freshness) = fixture().await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let retained = freshness.retain(&stamp).unwrap();
    let operation = retained.run(std::future::pending::<()>());
    tokio::pin!(operation);
    assert!(futures::poll!(&mut operation).is_pending());
    tokio::time::advance(Duration::from_millis(100)).await;
    freshness
        .record_applied(progress(&runtime, Instant::now(), 2, Some(true)))
        .unwrap();
    assert_eq!(operation.await, None);
    assert!(freshness.acquire(true).is_some());
    assert!(freshness.retain(&stamp).is_none());
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn timely_validated_retention_can_replace_but_not_extend_an_old_permit() {
    let (runtime, freshness) = fixture().await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let retained = freshness.retain(&stamp).unwrap();
    tokio::time::advance(Duration::from_millis(75)).await;
    freshness
        .record_applied(progress(&runtime, Instant::now(), 2, Some(true)))
        .unwrap();
    assert_eq!(retained.run(async { true }).await, Some(true));
    let renewed = freshness.retain(&stamp).unwrap();
    assert!(renewed.deadline() > retained.deadline());
    tokio::time::advance(Duration::from_millis(50)).await;
    assert_eq!(retained.run(async { 42 }).await, None);
    assert_eq!(renewed.run(async { 42 }).await, Some(42));
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn running_retention_is_cancelled_by_hard_invalidation_despite_new_proof() {
    let (runtime, freshness) = fixture().await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let retained = freshness.retain(&stamp).unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        retained
            .run(async {
                started_tx.send(()).unwrap();
                std::future::pending::<bool>().await
            })
            .await
    });
    started_rx.await.unwrap();
    freshness.invalidate();
    freshness
        .record_applied(progress(&runtime, Instant::now(), 2, Some(true)))
        .unwrap();
    assert_eq!(task.await.unwrap(), None);
    assert!(freshness.acquire(true).is_some());
    assert!(freshness.retain(&stamp).is_none());
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn blocked_retention_validation_keeps_its_original_deadline_despite_renewals() {
    let (runtime, freshness) = fixture().await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let retained = freshness.retain(&stamp).unwrap();
    let operation = retained.run(std::future::pending::<bool>());
    tokio::pin!(operation);
    assert!(futures::poll!(&mut operation).is_pending());
    for index in 2..=4 {
        tokio::time::advance(Duration::from_millis(25)).await;
        freshness
            .record_applied(progress(&runtime, Instant::now(), index, Some(true)))
            .unwrap();
        assert!(futures::poll!(&mut operation).is_pending());
    }
    tokio::time::advance(Duration::from_millis(25)).await;
    assert_eq!(operation.await, None);
    assert!(freshness.acquire(true).is_some());
    runtime.shutdown();
}

struct Cancelled(Arc<std::sync::atomic::AtomicBool>);

impl Drop for Cancelled {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn expired_acquisition_drops_blocked_effect_and_new_proof_only_permits_new_work() {
    let (runtime, freshness) = fixture().await;
    let acquisition = freshness.acquire(true).unwrap();
    let old_stamp = acquisition.stamp();
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guard = Cancelled(cancelled.clone());
    let operation = acquisition.run(async move {
        let _guard = guard;
        std::future::pending::<()>().await
    });
    tokio::pin!(operation);
    assert!(futures::poll!(&mut operation).is_pending());
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(operation.await, None);
    assert!(cancelled.load(std::sync::atomic::Ordering::SeqCst));
    assert!(freshness.acquire(true).is_none());
    freshness
        .record_applied(progress(&runtime, Instant::now(), 2, Some(true)))
        .unwrap();
    assert_eq!(acquisition.run(async { 42 }).await, None);
    assert_eq!(
        freshness.acquire(true).unwrap().run(async { 42 }).await,
        Some(42)
    );
    assert!(freshness.retain(&old_stamp).is_none());
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn missing_or_expired_verified_proof_cannot_poll_effect_work() {
    let runtime = super::super::tests::admit_driver().await;
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_millis(100)));
    assert!(freshness.acquire(true).is_none());
    freshness
        .record_applied(progress(&runtime, Instant::now(), 1, Some(true)))
        .unwrap();
    assert!(freshness.acquire(false).is_none());
    let acquisition = freshness.acquire(true).unwrap();
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(freshness.acquire(true).is_none());
    let polled = std::sync::atomic::AtomicBool::new(false);
    assert_eq!(
        acquisition
            .run(async {
                polled.store(true, std::sync::atomic::Ordering::SeqCst);
                42
            })
            .await,
        None
    );
    assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn timely_new_full_log_at_equal_start_preserves_the_same_deadline() {
    let (runtime, freshness) = fixture().await;
    let started = Instant::now();
    let stamp = freshness.acquire(true).unwrap().stamp();
    let deadline = freshness.retain(&stamp).unwrap().deadline();
    freshness
        .record_applied(progress(&runtime, started, 2, Some(true)))
        .unwrap();
    assert_eq!(freshness.retain(&stamp).unwrap().deadline(), deadline);
    assert!(
        freshness
            .record_applied(progress(&runtime, started, 2, Some(true)))
            .is_err()
    );
    let mut changed_term = progress(&runtime, started, 2, Some(true));
    changed_term.applied.log_id = openraft::testing::log_id::<TypeConfig>(2, runtime.local, 2);
    assert!(freshness.record_applied(changed_term).is_err());
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn admission_only_and_unhealthy_progress_do_not_extend_a_stamp() {
    let (runtime, freshness) = fixture().await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let deadline = freshness.retain(&stamp).unwrap().deadline();
    tokio::time::advance(Duration::from_millis(50)).await;
    for healthy in [None, Some(false)] {
        assert!(
            freshness
                .record_applied(progress(&runtime, Instant::now(), 2, healthy))
                .is_err()
        );
        assert_eq!(freshness.retain(&stamp).unwrap().deadline(), deadline);
    }
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn pending_operations_capture_revocation_before_their_first_poll() {
    let (runtime, freshness) = fixture().await;
    let acquisition = freshness.acquire(true).unwrap();
    let stamp = acquisition.stamp();
    let retained = freshness.retain(&stamp).unwrap();
    freshness.mark_unavailable();
    freshness
        .record_applied(progress(&runtime, Instant::now(), 2, Some(true)))
        .unwrap();
    assert_eq!(acquisition.run(async { 7 }).await, None);
    assert_eq!(retained.run(async { 7 }).await, Some(7));
    freshness.invalidate();
    freshness
        .record_applied(progress(&runtime, Instant::now(), 3, Some(true)))
        .unwrap();
    assert_eq!(retained.run(async { 7 }).await, None);
    assert!(freshness.retain(&stamp).is_none());
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn hard_invalidation_does_not_allow_replaying_the_last_completed_proof() {
    let (runtime, freshness) = fixture().await;
    let duplicate = progress(&runtime, Instant::now(), 1, Some(true));
    freshness.invalidate();
    assert!(freshness.record_applied(duplicate).is_err());
    assert!(freshness.acquire(true).is_none());
    freshness
        .record_applied(progress(&runtime, Instant::now(), 2, Some(true)))
        .unwrap();
    assert!(freshness.acquire(true).is_some());
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn terminal_runtime_seal_cancels_completed_retention_with_fresh_health() {
    let (runtime, freshness) = fixture().await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let retained = freshness.retain(&stamp).unwrap();
    let blocked = retained.run(std::future::pending::<()>());
    tokio::pin!(blocked);
    assert!(futures::poll!(&mut blocked).is_pending());
    runtime.shutdown();
    assert_eq!(blocked.await, None);
    assert!(!retained.check());
    assert!(freshness.retain(&stamp).is_none());
}

#[tokio::test(start_paused = true)]
async fn retention_is_capped_by_the_captured_admission_deadline() {
    let runtime = super::super::tests::admit_driver().await;
    let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(100)));
    let proof = progress(&runtime, Instant::now(), 1, Some(true));
    let deadline = proof.accepted_deadline();
    freshness.record_applied(proof).unwrap();
    let stamp = freshness.acquire(true).unwrap().stamp();
    assert_eq!(freshness.retain(&stamp).unwrap().deadline(), deadline);
    tokio::time::advance(deadline - Instant::now()).await;
    assert!(freshness.retain(&stamp).is_none());
    assert!(freshness.acquire(true).is_none());
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn a_stamp_cannot_cross_freshness_instances() {
    let (runtime, freshness) = fixture().await;
    let stamp = freshness.acquire(true).unwrap().stamp();
    let other = Arc::new(ConsensusFreshness::new(Duration::from_millis(100)));
    other
        .record_applied(progress(&runtime, Instant::now(), 1, Some(true)))
        .unwrap();
    assert!(other.retain(&stamp).is_none());
    runtime.shutdown();
}

#[tokio::test(start_paused = true)]
async fn proof_constructor_rejects_changed_nonce_boot_genesis_or_full_log_id() {
    let runtime = super::super::tests::admit_driver().await;
    let proof = progress(&runtime, Instant::now(), 1, Some(true));
    for fault in 0..5 {
        let mut applied = proof.applied.clone();
        match fault {
            1 => applied.request.request_nonce[0] ^= 1,
            2 => applied.request.replica.boot_nonce[0] ^= 1,
            3 => applied.request.genesis.epoch ^= 1,
            4 => applied.log_id = openraft::testing::log_id::<TypeConfig>(2, runtime.local, 1),
            _ => (),
        }
        let installed = InstalledAdmission {
            session: proof.session.clone(),
            deadline: proof.accepted_deadline,
            started: proof.started,
            request: crate::raft::admission::AdmissionRequest {
                consumer: runtime.local,
                genesis: proof.context().genesis.clone(),
                request_nonce: proof.applied.request.request_nonce,
                mode: AdmissionMode::Renew {
                    progress: Some(proof.log_id()),
                },
            },
        };
        let result =
            VerifiedProgress::new(proof.started, applied, installed, runtime.authority.clone());
        assert_eq!(result.is_ok(), fault == 0);
    }
    runtime.shutdown();
}
