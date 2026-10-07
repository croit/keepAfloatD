//! Translate an applied probe into local VIP eligibility.

use crate::consensus_freshness::ConsensusFreshness;
use tokio::time::Instant;

use crate::config::{Config, HealthConfig};
use crate::health::{self, LocalHealth};
pub(crate) use crate::raft::admission::runtime::PublicationReadiness;
use crate::raft::admission::runtime::RuntimeDriver;
use crate::raft::{RaftNetworkImpl, TypeConfig};
use openraft::alias::LogIdOf;
use std::sync::Arc;

pub(crate) struct AppliedProbe {
    pub(crate) started: Instant,
    pub(crate) log_id: LogIdOf<TypeConfig>,
}

pub(crate) trait Probe: Send + Sync + 'static {
    fn check(&self) -> impl std::future::Future<Output = bool> + Send;
}

pub(crate) struct CommandProbe {
    cfg: HealthConfig,
}

impl CommandProbe {
    pub(crate) fn new(cfg: HealthConfig) -> Self {
        Self { cfg }
    }
}

impl Probe for CommandProbe {
    async fn check(&self) -> bool {
        health::run_health_check(&self.cfg).await
    }
}

pub(crate) trait Publisher: Send + Sync + 'static {
    fn readiness(
        &self,
    ) -> impl std::future::Future<Output = anyhow::Result<PublicationReadiness>> + Send;
    fn publish(
        &self,
        healthy: bool,
    ) -> impl std::future::Future<Output = anyhow::Result<AppliedProbe>> + Send;
    fn activation_ready(&self) -> impl std::future::Future<Output = bool> + Send;
}

pub(crate) struct RuntimePublisher {
    runtime: Arc<RuntimeDriver>,
    network: Arc<RaftNetworkImpl>,
}

impl RuntimePublisher {
    pub(crate) fn new(runtime: Arc<RuntimeDriver>, network: Arc<RaftNetworkImpl>) -> Self {
        Self { runtime, network }
    }
}

impl Publisher for RuntimePublisher {
    async fn readiness(&self) -> anyhow::Result<PublicationReadiness> {
        self.runtime.health_publication_ready().await
    }

    async fn publish(&self, healthy: bool) -> anyhow::Result<AppliedProbe> {
        let (started, log_id) = self.runtime.submit_health(&self.network, healthy).await?;
        Ok(AppliedProbe { started, log_id })
    }

    async fn activation_ready(&self) -> bool {
        self.runtime.vip_activation_ready().await
    }
}

#[cfg(test)]
pub(crate) async fn run(
    cfg: Arc<Config>,
    local_healthy: Arc<LocalHealth>,
    consensus_fresh: Arc<ConsensusFreshness>,
    publisher: impl Publisher,
) {
    let probe = CommandProbe::new(cfg.health.clone());
    run_with_probe(cfg, local_healthy, consensus_fresh, publisher, probe).await;
}

pub(crate) async fn run_with_probe(
    cfg: Arc<Config>,
    local_healthy: Arc<LocalHealth>,
    consensus_fresh: Arc<ConsensusFreshness>,
    publisher: impl Publisher,
    probe: impl Probe,
) {
    let failover_delay_ticks = cfg.effective_failover_delay_ticks();
    let mut failure_dampener = health::FailureDampener::new(failover_delay_ticks);
    let mut tick =
        tokio::time::interval(tokio::time::Duration::from_millis(cfg.health.interval_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut bind_fault_reported = false;
    let mut bind_fault_committed = false;
    loop {
        let raw_ok = tokio::select! {
            biased;
            result = local_healthy.wait_for_bind_failure(), if !bind_fault_reported => {
                if let Err(error) = result {
                    tracing::error!(%error, "local health watch failed");
                    return;
                }
                bind_fault_reported = true;
                false
            }
            ok = async {
                tick.tick().await;
                probe.check().await
            } => ok,
        };
        let previous_ok = local_healthy.observe_probe(failure_dampener.observe(raw_ok));
        let ok = local_healthy.is_healthy();
        if !raw_ok && ok && failure_dampener.failure_ticks() == 1 {
            tracing::warn!(
                failover_delay_secs = cfg.failover_delay_secs,
                "health probe failed; delaying failover"
            );
        } else if previous_ok && !ok {
            tracing::warn!("effective local health became unhealthy");
        } else if !previous_ok && ok {
            tracing::info!("effective local health became healthy");
        }
        match publisher.readiness().await {
            Ok(PublicationReadiness::Pending) => {
                consensus_fresh.invalidate();
                continue;
            }
            Ok(PublicationReadiness::Ready) => (),
            Err(error) => {
                consensus_fresh.invalidate();
                tracing::warn!(%error, "health publication readiness failed");
                continue;
            }
        }
        match publisher.publish(ok).await {
            Ok(proof) => {
                tracing::debug!(log_id = %proof.log_id, "health proof applied");
                record_probe(
                    &consensus_fresh,
                    publisher.activation_ready().await,
                    proof.started,
                );
                if bind_fault_reported && !bind_fault_committed && !ok {
                    tracing::info!("bind fault committed as unhealthy");
                    bind_fault_committed = true;
                }
            }
            Err(e) => {
                consensus_fresh.invalidate();
                tracing::warn!("health raft submit: {}", e);
            }
        }
    }
}

pub(crate) fn record_probe(
    freshness: &ConsensusFreshness,
    activation_ready: bool,
    started: Instant,
) {
    if activation_ready {
        freshness.record_success(started);
    } else {
        freshness.invalidate();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn activation_fence_blocks_first_and_previously_fresh_proofs() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(3));
        record_probe(&freshness, false, Instant::now());
        assert!(!freshness.is_fresh());
        record_probe(&freshness, true, Instant::now());
        assert!(freshness.is_fresh());
        record_probe(&freshness, false, Instant::now());
        assert!(!freshness.is_fresh());
    }

    #[tokio::test(start_paused = true)]
    async fn accepted_probe_retains_its_original_challenge_start() {
        let freshness = ConsensusFreshness::new(Duration::from_secs(3));
        let started = Instant::now();
        tokio::time::advance(Duration::from_secs(3)).await;
        record_probe(&freshness, true, started);
        assert!(!freshness.is_fresh());
        record_probe(&freshness, true, Instant::now());
        assert!(freshness.is_fresh());
    }
}
