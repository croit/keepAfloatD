//! Translate an applied probe into local VIP eligibility.

use crate::consensus_freshness::{ConsensusFreshness, FailureKind, failure_kind};

use crate::config::{Config, HealthConfig};
use crate::health::{self, LocalHealth};
use crate::raft::RaftNetworkImpl;
pub(crate) use crate::raft::admission::runtime::PublicationReadiness;
use crate::raft::admission::runtime::RuntimeDriver;
pub(crate) use crate::raft::admission::runtime::VerifiedProgress as AppliedProbe;
use std::sync::Arc;

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
        self.runtime
            .submit_health_proof(&self.network, healthy)
            .await
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
        if !ok {
            consensus_fresh.invalidate();
        }
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
        // Renew admission without moving VIPs to a node that cannot activate them yet.
        let placement_healthy = ok && publisher.activation_ready().await;
        match publisher.publish(placement_healthy).await {
            Ok(proof) => {
                tracing::debug!(log_id = %proof.log_id(), "health proof applied");
                record_probe(&consensus_fresh, publisher.activation_ready().await, proof);
                if bind_fault_reported && !bind_fault_committed && !ok {
                    tracing::info!("bind fault committed as unhealthy");
                    bind_fault_committed = true;
                }
            }
            Err(e) => {
                match failure_kind(&e) {
                    FailureKind::Unavailable => consensus_fresh.mark_unavailable(),
                    FailureKind::Rejected => consensus_fresh.invalidate(),
                }
                tracing::warn!("health raft submit: {}", e);
            }
        }
    }
}

pub(crate) fn record_probe(
    freshness: &ConsensusFreshness,
    activation_ready: bool,
    proof: AppliedProbe,
) {
    if activation_ready {
        if let Err(error) = freshness.record_applied(proof) {
            freshness.invalidate();
            tracing::debug!(%error, "applied health proof cannot authorize VIP effects");
        }
    } else {
        freshness.invalidate();
    }
}
