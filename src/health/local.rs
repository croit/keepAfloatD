//! Local eligibility combines the service probe with a process-lifetime bind fault.

use tokio::sync::watch;

/// Why a VIP effect failed. Either way the fault lasts for the life of the process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BindFault {
    /// The command failed outright; restarting does not repair the cause.
    Failed,
    /// The command missed its deadline. A supervised restart re-enters placement once the host
    /// recovers (#44).
    TimedOut,
}

#[derive(Clone, Copy)]
struct Status {
    probe_healthy: bool,
    bind_fault: Option<BindFault>,
    loss_epoch: u64,
}

impl Status {
    fn healthy(self) -> bool {
        self.probe_healthy && self.bind_fault.is_none()
    }
}

pub(crate) struct LocalHealth {
    status: watch::Sender<Status>,
}

#[derive(Clone)]
pub(crate) struct HealthContinuity {
    status: watch::Receiver<Status>,
    loss_epoch: u64,
}

impl HealthContinuity {
    pub(crate) fn check(&self) -> bool {
        let status = self.status.borrow();
        status.healthy() && status.loss_epoch == self.loss_epoch && self.loss_epoch != u64::MAX
    }

    pub(crate) async fn run<F: std::future::Future>(&self, operation: F) -> Option<F::Output> {
        if !self.check() {
            return None;
        }
        let mut status = self.status.clone();
        tokio::pin!(operation);
        loop {
            tokio::select! {
                biased;
                changed = status.changed() => {
                    if changed.is_err() || !self.check() { return None; }
                }
                result = &mut operation => return self.check().then_some(result),
            }
        }
    }
}

impl LocalHealth {
    pub(crate) fn new(probe_healthy: bool) -> Self {
        Self {
            status: watch::channel(Status {
                probe_healthy,
                bind_fault: None,
                loss_epoch: 0,
            })
            .0,
        }
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.status.borrow().healthy()
    }

    pub(crate) fn observe_continuity(&self) -> HealthContinuity {
        let status = self.status.subscribe();
        let loss_epoch = status.borrow().loss_epoch;
        HealthContinuity { status, loss_epoch }
    }

    /// Update only the probe result, returning the previous effective health.
    pub(crate) fn observe_probe(&self, healthy: bool) -> bool {
        let mut previous = false;
        self.status.send_modify(|status| {
            previous = status.healthy();
            status.probe_healthy = healthy;
            if previous && !status.healthy() {
                status.loss_epoch = status.loss_epoch.saturating_add(1);
            }
        });
        previous
    }

    /// A service probe cannot prove bind recovery; only a fresh process resets this fault.
    /// The first fault decides whether the process asks for a restart.
    pub(crate) fn fail_binding(&self, fault: BindFault) {
        self.status.send_if_modified(|status| {
            if status.bind_fault.is_some() {
                return false;
            }
            if status.healthy() {
                status.loss_epoch = status.loss_epoch.saturating_add(1);
            }
            status.bind_fault = Some(fault);
            true
        });
    }

    pub(crate) async fn wait_for_bind_failure(&self) -> Result<(), watch::error::RecvError> {
        self.wait_for_fault(|fault| fault.is_some()).await
    }

    pub(crate) async fn wait_for_restart_request(&self) -> Result<(), watch::error::RecvError> {
        self.wait_for_fault(|fault| fault == Some(BindFault::TimedOut))
            .await
    }

    async fn wait_for_fault(
        &self,
        matches: impl Fn(Option<BindFault>) -> bool,
    ) -> Result<(), watch::error::RecvError> {
        self.status
            .subscribe()
            .wait_for(|status| matches(status.bind_fault))
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn continuity_observes_loss_and_recovery_before_first_poll() {
        let health = LocalHealth::new(true);
        let continuity = health.observe_continuity();
        let operation = continuity.run(std::future::pending::<()>());
        tokio::pin!(operation);
        health.observe_probe(false);
        health.observe_probe(true);
        assert!(health.is_healthy());
        assert!(
            !continuity.check(),
            "a recovered probe must not revive the old binding"
        );
        assert!(matches!(
            futures::poll!(&mut operation),
            std::task::Poll::Ready(None)
        ));
        assert!(health.observe_continuity().check());
    }

    #[tokio::test]
    async fn continuity_allows_repeated_healthy_probes_but_rejects_bind_fault() {
        let health = LocalHealth::new(true);
        let continuity = health.observe_continuity();
        for _ in 0..3 {
            health.observe_probe(true);
        }
        assert!(continuity.check());
        assert_eq!(continuity.run(async { 7 }).await, Some(7));
        health.fail_binding(BindFault::Failed);
        health.observe_probe(true);
        assert!(!continuity.check());
        assert_eq!(continuity.run(async { 7 }).await, None);
    }

    #[tokio::test]
    async fn probe_loss_and_recovery_do_not_become_a_bind_fault() {
        let health = LocalHealth::new(true);
        let waiting = health.wait_for_bind_failure();
        tokio::pin!(waiting);

        assert!(health.observe_probe(false));
        assert!(!health.is_healthy());
        assert!(!health.observe_probe(true));
        assert!(health.is_healthy());
        assert!(futures::poll!(&mut waiting).is_pending());

        health.fail_binding(BindFault::Failed);
        assert!(matches!(
            futures::poll!(&mut waiting),
            std::task::Poll::Ready(Ok(()))
        ));
        assert!(!health.is_healthy());
    }

    #[tokio::test]
    async fn bind_fault_before_first_waiter_poll_survives_probe_recovery() {
        let health = LocalHealth::new(true);
        let waiting = health.wait_for_bind_failure();
        tokio::pin!(waiting);

        health.fail_binding(BindFault::Failed);
        for probe in [false, true, true] {
            assert!(!health.observe_probe(probe));
        }
        assert!(matches!(
            futures::poll!(&mut waiting),
            std::task::Poll::Ready(Ok(()))
        ));
        assert!(!health.is_healthy());
    }

    #[tokio::test]
    async fn bind_fault_survives_probes_and_requires_a_fresh_process() {
        let health = LocalHealth::new(false);
        assert!(!health.is_healthy());
        assert!(!health.observe_probe(true));
        assert!(health.is_healthy());
        assert!(health.observe_probe(false));
        assert!(!health.is_healthy());
        health.observe_probe(true);
        let waiting = health.wait_for_bind_failure();
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        health.fail_binding(BindFault::Failed);
        waiting.await.unwrap();
        for probe in [true, false, true] {
            assert!(!health.observe_probe(probe));
            assert!(!health.is_healthy());
        }
        health.fail_binding(BindFault::Failed);
        health.wait_for_bind_failure().await.unwrap();
        let restarted = LocalHealth::new(false);
        restarted.observe_probe(true);
        assert!(restarted.is_healthy());
    }

    #[tokio::test]
    async fn only_a_timed_out_bind_requests_a_restart() {
        let failed = LocalHealth::new(true);
        let failed_restart = failed.wait_for_restart_request();
        tokio::pin!(failed_restart);
        failed.fail_binding(BindFault::Failed);
        assert!(!failed.is_healthy());
        assert!(futures::poll!(&mut failed_restart).is_pending());
        failed.fail_binding(BindFault::TimedOut);
        assert!(
            futures::poll!(&mut failed_restart).is_pending(),
            "a latched failure must not turn into a restart request"
        );

        let timed_out = LocalHealth::new(true);
        let restart = timed_out.wait_for_restart_request();
        tokio::pin!(restart);
        assert!(futures::poll!(&mut restart).is_pending());
        timed_out.fail_binding(BindFault::TimedOut);
        assert!(!timed_out.is_healthy());
        timed_out.observe_probe(true);
        assert!(!timed_out.is_healthy());
        assert!(matches!(
            futures::poll!(&mut restart),
            std::task::Poll::Ready(Ok(()))
        ));
        timed_out.wait_for_bind_failure().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_probe_updates_cannot_clear_a_bind_fault() {
        let health = Arc::new(LocalHealth::new(true));
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let observer = health.clone();
        let start = barrier.clone();
        let probe = tokio::spawn(async move {
            start.wait().await;
            for _ in 0..100 {
                observer.observe_probe(true);
                tokio::task::yield_now().await;
            }
        });
        barrier.wait().await;
        health.fail_binding(BindFault::Failed);
        probe.await.unwrap();
        assert!(!health.is_healthy());
    }
}
