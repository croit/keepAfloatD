//! Local eligibility combines the service probe with a process-lifetime bind fault.

use tokio::sync::watch;

#[derive(Clone, Copy)]
struct Status {
    probe_healthy: bool,
    bind_failed: bool,
}

impl Status {
    fn healthy(self) -> bool {
        self.probe_healthy && !self.bind_failed
    }
}

pub(crate) struct LocalHealth {
    status: watch::Sender<Status>,
}

impl LocalHealth {
    pub(crate) fn new(probe_healthy: bool) -> Self {
        Self {
            status: watch::channel(Status {
                probe_healthy,
                bind_failed: false,
            })
            .0,
        }
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.status.borrow().healthy()
    }

    /// Update only the probe result, returning the previous effective health.
    pub(crate) fn observe_probe(&self, healthy: bool) -> bool {
        let mut previous = false;
        self.status.send_modify(|status| {
            previous = status.healthy();
            status.probe_healthy = healthy;
        });
        previous
    }

    /// A service probe cannot prove bind recovery; only a fresh process resets this fault.
    pub(crate) fn fail_binding(&self) {
        self.status.send_if_modified(|status| {
            let changed = !status.bind_failed;
            status.bind_failed = true;
            changed
        });
    }

    pub(crate) async fn wait_for_bind_failure(&self) -> Result<(), watch::error::RecvError> {
        self.status
            .subscribe()
            .wait_for(|status| status.bind_failed)
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
        health.fail_binding();
        waiting.await.unwrap();
        for probe in [true, false, true] {
            assert!(!health.observe_probe(probe));
            assert!(!health.is_healthy());
        }
        health.fail_binding();
        health.wait_for_bind_failure().await.unwrap();
        let restarted = LocalHealth::new(false);
        restarted.observe_probe(true);
        assert!(restarted.is_healthy());
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
        health.fail_binding();
        probe.await.unwrap();
        assert!(!health.is_healthy());
    }
}
