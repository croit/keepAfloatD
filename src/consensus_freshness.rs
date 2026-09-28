//! Process-local quorum proof lifetime; no wall clock enters replicated placement (#26).
use std::future::Future;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

/// A successful submit proves quorum contact only for a bounded probe/handoff window.
/// Measure from request start so a delayed response cannot manufacture a new lease.
pub(crate) struct ConsensusFreshness {
    proof: watch::Sender<Proof>,
    lifetime: Duration,
}

#[derive(Clone, Copy)]
struct Proof {
    started: Option<Instant>,
    invalidation_epoch: u64,
}

enum ProofEvent<T> {
    Renewed(Result<(), watch::error::RecvError>),
    Expired,
    Finished(T),
}

impl ConsensusFreshness {
    /// Normal probe work can finish just after the next interval boundary (#26).
    /// Permit half a configured activation interval for renewal jitter. Committed
    /// rounds can arrive in bursts, so the successor separately waits in local time.
    pub(crate) fn for_probe_cadence(interval_ms: u64, stale_missed_probes: u64) -> Self {
        let stale = Duration::from_millis(interval_ms.saturating_mul(stale_missed_probes));
        let handoff = Duration::from_millis(
            interval_ms.saturating_mul(crate::raft::store::OWNERSHIP_ACTIVATION_HOLDOFF_TICKS),
        );
        Self::new(stale.saturating_add(handoff / 2))
    }

    pub(crate) fn new(lifetime: Duration) -> Self {
        Self {
            proof: watch::channel(Proof {
                started: None,
                invalidation_epoch: 0,
            })
            .0,
            lifetime,
        }
    }

    pub(crate) fn record_success(&self, started: Instant) {
        self.proof.send_if_modified(|proof| {
            if proof.started.is_none_or(|previous| started > previous) {
                proof.started = Some(started);
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn invalidate(&self) {
        self.proof.send_modify(|proof| {
            proof.started = None;
            proof.invalidation_epoch = proof.invalidation_epoch.wrapping_add(1);
        });
    }

    pub(crate) fn is_fresh(&self) -> bool {
        self.proof
            .borrow()
            .started
            .is_some_and(|started| started.elapsed() < self.lifetime)
    }

    pub(crate) fn lifetime(&self) -> Duration {
        self.lifetime
    }

    pub(crate) async fn wait_until_fresh(&self) -> Result<(), watch::error::RecvError> {
        self.proof
            .subscribe()
            .wait_for(|proof| {
                proof
                    .started
                    .is_some_and(|started| started.elapsed() < self.lifetime)
            })
            .await
            .map(|_| ())
    }

    #[cfg(test)]
    async fn wait_until_stale(&self) {
        let mut proof = self.proof.subscribe();
        loop {
            let deadline = proof
                .borrow_and_update()
                .started
                .and_then(|started| started.checked_add(self.lifetime));
            let Some(deadline) = deadline else { return };
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    // #26: renewal may already be queued when the old timer wakes.
                    if !self.is_fresh() { return; }
                },
                changed = proof.changed() => {
                    if changed.is_err() { return; }
                }
            }
        }
    }

    /// A new applied proof cancels work captured from the previous assignment snapshot (#26).
    /// The caller restarts reconciliation while fresh, or withdraws after expiry/invalidation.
    #[cfg(test)]
    pub(crate) async fn run_while_fresh<F: Future>(&self, operation: F) -> Option<F::Output> {
        self.run_while_fresh_if(operation, || async { false }).await
    }

    /// Preserve a running effect across renewal only after validating its captured intent (#26).
    pub(crate) async fn run_while_fresh_if<F, V, Check>(
        &self,
        operation: F,
        mut valid: V,
    ) -> Option<F::Output>
    where
        F: Future,
        V: FnMut() -> Check,
        Check: Future<Output = bool>,
    {
        let mut changes = self.proof.subscribe();
        let accepted = *changes.borrow_and_update();
        let mut started = accepted.started?;
        tokio::pin!(operation);
        loop {
            let deadline = started.checked_add(self.lifetime)?;
            let event = tokio::select! {
                biased;
                changed = changes.changed() => ProofEvent::Renewed(changed),
                _ = tokio::time::sleep_until(deadline) => ProofEvent::Expired,
                result = &mut operation => ProofEvent::Finished(result),
            };
            match event {
                ProofEvent::Renewed(changed) => {
                    let candidate = *changes.borrow_and_update();
                    if changed.is_err()
                        || candidate.invalidation_epoch != accepted.invalidation_epoch
                    {
                        return None;
                    }
                    let next_started = candidate.started?;
                    if next_started.elapsed() >= self.lifetime {
                        return None;
                    }
                    let mut invalidations = self.proof.subscribe();
                    let remains_valid = tokio::select! {
                        biased;
                        valid = valid() => valid,
                        _ = tokio::time::sleep_until(deadline) => false,
                        _ = invalidations.wait_for(|proof| proof.invalidation_epoch != accepted.invalidation_epoch) => false,
                    };
                    if !remains_valid {
                        return None;
                    }
                    // #26: only this validated proof can extend the running intent's deadline.
                    started = next_started;
                }
                ProofEvent::Expired => return None,
                ProofEvent::Finished(result) => return Some(result),
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[tokio::test(start_paused = true)]
    async fn spawned_effect_completes_after_a_validated_renewal_extends_its_deadline() {
        let freshness = Arc::new(ConsensusFreshness::new(Duration::from_millis(100)));
        freshness.record_success(Instant::now());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let (validated_tx, mut validated_rx) = tokio::sync::mpsc::unbounded_channel();
        let copy = freshness.clone();
        let task = tokio::spawn(async move {
            copy.run_while_fresh_if(
                async {
                    started_tx.send(()).unwrap();
                    finish_rx.await.unwrap()
                },
                || async {
                    validated_tx.send(()).unwrap();
                    true
                },
            )
            .await
        });
        started_rx.await.unwrap();
        tokio::time::advance(Duration::from_millis(75)).await;
        freshness.record_success(Instant::now());
        validated_rx.recv().await.unwrap();
        tokio::time::advance(Duration::from_millis(50)).await;
        finish_tx.send(42).unwrap();
        assert_eq!(
            task.await.unwrap(),
            Some(42),
            "validated proof must permit completion past the original deadline"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn spawned_validation_is_interrupted_by_invalidation_even_after_new_success() {
        let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
        freshness.record_success(Instant::now());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (validating_tx, mut validating_rx) = tokio::sync::mpsc::unbounded_channel();
        let copy = freshness.clone();
        let task = tokio::spawn(async move {
            copy.run_while_fresh_if(
                async {
                    started_tx.send(()).unwrap();
                    std::future::pending::<()>().await
                },
                || async {
                    validating_tx.send(()).unwrap();
                    std::future::pending::<bool>().await
                },
            )
            .await
        });
        started_rx.await.unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        freshness.record_success(Instant::now());
        validating_rx.recv().await.unwrap();
        freshness.invalidate();
        tokio::time::advance(Duration::from_millis(1)).await;
        freshness.record_success(Instant::now());
        assert_eq!(task.await.unwrap(), None);
        assert!(
            freshness.is_fresh(),
            "global freshness cannot conceal invalidated running work"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_intent_validation_cannot_extend_its_accepted_proof_deadline() {
        let freshness = ConsensusFreshness::new(Duration::from_millis(100));
        freshness.record_success(Instant::now());
        let operation = freshness.run_while_fresh_if(std::future::pending::<()>(), || {
            std::future::pending::<bool>()
        });
        tokio::pin!(operation);
        assert!(futures::poll!(&mut operation).is_pending());
        for _ in 0..3 {
            tokio::time::advance(Duration::from_millis(25)).await;
            freshness.record_success(Instant::now());
            assert!(futures::poll!(&mut operation).is_pending());
        }
        tokio::time::advance(Duration::from_millis(25)).await;
        freshness.record_success(Instant::now());
        assert!(
            matches!(futures::poll!(&mut operation), std::task::Poll::Ready(None)),
            "globally renewed proofs cannot prolong an intent whose validation is blocked"
        );
        assert!(freshness.is_fresh());
    }

    #[tokio::test(start_paused = true)]
    async fn invalidation_cannot_be_hidden_by_a_coalesced_successful_renewal() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(1));
        freshness.record_success(Instant::now());
        let operation =
            freshness.run_while_fresh_if(std::future::pending::<()>(), || async { true });
        tokio::pin!(operation);
        assert!(futures::poll!(&mut operation).is_pending());
        freshness.invalidate();
        tokio::time::advance(Duration::from_millis(1)).await;
        freshness.record_success(Instant::now());
        assert!(
            matches!(futures::poll!(&mut operation), std::task::Poll::Ready(None)),
            "a new proof cannot conceal an invalidation requiring cleanup"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn renewal_margin_is_bounded_to_half_a_configured_probe_interval() {
        let freshness = ConsensusFreshness::for_probe_cadence(1_000, 1);
        freshness.record_success(Instant::now());
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(
            freshness.is_fresh(),
            "ordinary probe jitter must not withdraw a VIP"
        );
        tokio::time::advance(Duration::from_millis(800)).await;
        assert!(!freshness.is_fresh(), "renewal grace must remain bounded");
    }

    #[tokio::test(start_paused = true)]
    async fn proof_expires_at_the_exact_deadline_without_another_probe() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(1));
        assert!(!freshness.is_fresh());
        freshness.record_success(Instant::now());
        assert!(freshness.is_fresh());
        tokio::time::advance(Duration::from_millis(999)).await;
        assert!(freshness.is_fresh());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!freshness.is_fresh());
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_acknowledgement_does_not_start_a_new_lease() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(1));
        let started = Instant::now();
        tokio::time::advance(Duration::from_secs(2)).await;
        freshness.record_success(started);
        assert!(!freshness.is_fresh());
        freshness.wait_until_stale().await;
    }

    #[tokio::test(start_paused = true)]
    async fn older_completion_cannot_replace_a_newer_proof() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(1));
        let older = Instant::now();
        tokio::time::advance(Duration::from_millis(500)).await;
        freshness.record_success(Instant::now());
        freshness.record_success(older);
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(freshness.is_fresh());
    }

    #[tokio::test(start_paused = true)]
    async fn renewal_moves_the_waiter_deadline_and_failure_invalidates_immediately() {
        let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
        freshness.record_success(Instant::now());
        let copy = freshness.clone();
        let waiter = tokio::spawn(async move { copy.wait_until_stale().await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(500)).await;
        freshness.record_success(Instant::now());
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(!waiter.is_finished());
        freshness.invalidate();
        waiter.await.unwrap();
        assert!(!freshness.is_fresh());
    }

    #[tokio::test(start_paused = true)]
    async fn queued_renewal_at_the_old_deadline_does_not_expire_current_proof() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(1));
        freshness.record_success(Instant::now());
        let waiter = freshness.wait_until_stale();
        tokio::pin!(waiter);
        assert!(futures::poll!(&mut waiter).is_pending());
        tokio::time::advance(Duration::from_millis(500)).await;
        freshness.record_success(Instant::now());
        // #26: simulate the waiter being descheduled until its old timer is also ready.
        // Both renewal and timer notifications must be handled using the current proof.
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(
            freshness.is_fresh(),
            "the renewed proof must still be valid"
        );
        assert!(
            futures::poll!(&mut waiter).is_pending(),
            "a queued renewal must supersede the previous deadline"
        );
        freshness.invalidate();
        waiter.await;
    }

    struct Cancelled(Arc<AtomicBool>);
    impl Drop for Cancelled {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn proof_renewal_cancels_an_in_flight_assignment_snapshot() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(1));
        freshness.record_success(Instant::now());
        let cancelled = Arc::new(AtomicBool::new(false));
        let guard = Cancelled(cancelled.clone());
        let active = freshness.run_while_fresh(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        tokio::pin!(active);
        assert!(futures::poll!(&mut active).is_pending());
        tokio::time::advance(Duration::from_millis(100)).await;
        freshness.record_success(Instant::now());
        assert!(
            matches!(futures::poll!(&mut active), std::task::Poll::Ready(None)),
            "renewal must cancel work captured before its applied-state proof"
        );
        assert!(cancelled.load(Ordering::SeqCst));
        assert!(
            freshness.is_fresh(),
            "snapshot restart must not invalidate a valid renewal"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_cancels_blocked_effect_work_and_a_new_proof_recovers() {
        let freshness = Arc::new(ConsensusFreshness::new(Duration::from_secs(1)));
        freshness.record_success(Instant::now());
        let cancelled = Arc::new(AtomicBool::new(false));
        let drop_guard = Cancelled(cancelled.clone());
        let copy = freshness.clone();
        let operation = tokio::spawn(async move {
            copy.run_while_fresh(async move {
                let _guard = drop_guard;
                std::future::pending::<()>().await;
            })
            .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(operation.await.unwrap().is_none());
        assert!(cancelled.load(Ordering::SeqCst));
        let copy = freshness.clone();
        let recovered = tokio::spawn(async move { copy.wait_until_fresh().await });
        tokio::task::yield_now().await;
        assert!(!recovered.is_finished());
        freshness.record_success(Instant::now());
        recovered.await.unwrap().unwrap();
        assert_eq!(freshness.run_while_fresh(async { 42 }).await, Some(42));
    }

    #[tokio::test(start_paused = true)]
    async fn missing_or_expired_proof_cannot_start_effect_work() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(1));
        assert_eq!(freshness.run_while_fresh(async { 42 }).await, None);
        freshness.record_success(Instant::now());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(freshness.run_while_fresh(async { 42 }).await, None);
    }
}

#[cfg(test)]
mod timing_tests;
