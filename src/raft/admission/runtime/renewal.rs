//! Independent bounds for serialized renewal and optional membership work.

use super::*;
use crate::raft::admission::{AppliedHealthProgress, HealthProgress};
use crate::raft::types::TypeConfig;
use openraft::alias::LogIdOf;
use tokio::sync::MutexGuard;

impl RuntimeDriver {
    pub(super) async fn bounded_phase<T>(
        &self,
        phase: &'static str,
        work: impl std::future::Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        tokio::time::timeout(self.timing.renewal_round_budget(), work)
            .await
            .map_err(|_| anyhow::anyhow!("{phase} phase timed out"))?
    }

    async fn renewal_lock(&self) -> anyhow::Result<MutexGuard<'_, ()>> {
        tokio::time::timeout(self.timing.renewal_round_budget(), self.round.lock())
            .await
            .map_err(|_| anyhow::anyhow!("admission round lock timed out"))
    }

    pub(super) async fn renew(
        &self,
        network: &RaftNetworkImpl,
        healthy: Option<bool>,
    ) -> anyhow::Result<(Instant, LogIdOf<TypeConfig>)> {
        self.within_admission(async {
            let _round = self.renewal_lock().await?;
            self.bounded_phase("renewal", async {
                let session = self.coherent_session().await?;
                self.ensure_running()?;
                session.check()?;
                anyhow::ensure!(
                    self.bootstrap_join
                        .lock()
                        .map_err(|_| AdmissionDenied("bootstrap lock poisoned"))?
                        .is_none(),
                    "health progress awaits learner promotion"
                );
                let mut round = self.begin(
                    session.context().genesis.clone(),
                    AdmissionMode::Renew { progress: None },
                )?;
                let progress = HealthProgress {
                    node_id: self.local.physical_id,
                    healthy,
                    replica: self.local,
                    epoch: session.context().genesis.epoch,
                    request_nonce: round.request().request_nonce,
                    genesis: session.context().genesis.clone(),
                };
                let discovery = self.discover(network, self.rpc_budget()).await?;
                let response = self
                    .route(
                        network,
                        ManagementAction::Progress(progress.clone()),
                        &discovery.boots,
                    )
                    .await?;
                let ManagementResult::Applied(log_id) = response else {
                    anyhow::bail!("progress response has no applied log");
                };
                let applied = AppliedHealthProgress {
                    request: progress,
                    log_id,
                };
                self.wait_progress(&applied).await?;
                round.bind_progress(&applied)?;
                let grants = self.grants(network, &round, &discovery.boots).await?;
                self.install(&mut round, &grants, None).await?;
                Ok((round.request_started(), log_id))
            })
            .await
        })
        .await
    }

    pub(super) async fn extend_join(&self, network: &RaftNetworkImpl) -> anyhow::Result<()> {
        self.within_admission(async {
            let _round = self.renewal_lock().await?;
            self.bounded_phase("learner renewal", async {
                let bootstrap = self
                    .bootstrap_join
                    .lock()
                    .map_err(|_| AdmissionDenied("bootstrap lock poisoned"))?
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("no learner admission to extend"))?;
                let prepared = bootstrap.prepared();
                let mut round = self.begin(
                    prepared.plan.genesis.clone(),
                    AdmissionMode::Join { prepared: None },
                )?;
                round.bind_prepared(prepared)?;
                let discovery = self.discover(network, self.rpc_budget()).await?;
                let grants = self.grants(network, &round, &discovery.boots).await?;
                self.install(&mut round, &grants, None).await
            })
            .await
        })
        .await
    }
}
