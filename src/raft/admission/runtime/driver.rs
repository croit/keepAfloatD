//! Bounded cold, learner and renewal rounds driven by the supervised daemon task.

use super::*;
use crate::raft::admission::{AdmissionRound, AppliedHealthProgress, ReceivedGrant};
use crate::raft::types::{KafRequest, TypeConfig};
use futures::future::join_all;
use openraft::alias::LogIdOf;
use openraft::async_runtime::WatchReceiver;
use std::collections::BTreeMap;
use std::time::Duration;

struct StopOnDrop<'a>(&'a RuntimeDriver);
impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

impl RuntimeDriver {
    pub(crate) async fn run(&self, network: Arc<RaftNetworkImpl>) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.running.swap(true, Ordering::SeqCst),
            "admission driver already running"
        );
        let _stop = StopOnDrop(self);
        let result = async {
            self.raft()?;
            tokio::select! {
                biased;
                _ = network.wait_until_shutdown() => Ok(()),
                result = self.drive(&network) => result,
                _ = self.authority.wait_until_sealed() => {
                    anyhow::bail!("runtime admission expired terminally")
                }
            }
        }
        .await;
        if self.stopped.load(Ordering::SeqCst) {
            Ok(())
        } else {
            result
        }
    }

    pub(crate) async fn submit_health(
        &self,
        network: &RaftNetworkImpl,
        healthy: bool,
    ) -> anyhow::Result<(Instant, LogIdOf<TypeConfig>)> {
        self.renew(network, Some(healthy)).await
    }

    pub(super) fn rpc_budget(&self) -> Duration {
        self.timing.rpc_budget()
    }

    async fn drive(&self, network: &RaftNetworkImpl) -> anyhow::Result<()> {
        tokio::time::sleep_until(self.timing.quarantine_deadline(self.boot)?).await;
        let operation_nonce = fresh_nonce()?;
        loop {
            self.ensure_running()?;
            if self.authority.current().is_some() {
                break;
            }
            match tokio::time::timeout(
                self.timing.consumer_use(),
                self.enter(network, operation_nonce),
            )
            .await
            {
                Ok(Ok(())) => break,
                Ok(Err(error)) => tracing::debug!(%error, "admission formation round deferred"),
                Err(_) => tracing::debug!("admission formation round timed out"),
            }
            tokio::time::sleep(self.rpc_budget()).await;
        }
        tracing::info!(replica = %self.local, "runtime admission acquired");
        loop {
            if let Err(error) = self.within_admission(self.drive_round(network)).await {
                self.authority
                    .current()
                    .ok_or(AdmissionDenied("runtime admission expired during renewal"))?;
                tracing::debug!(%error, "admission renewal round deferred");
            }
            tokio::time::sleep(self.timing.renewal_interval()).await;
        }
    }

    pub(super) async fn drive_round(&self, network: &RaftNetworkImpl) -> anyhow::Result<()> {
        self.bounded_phase("genesis", self.establish_genesis())
            .await?;
        let joining = self
            .bootstrap_join
            .lock()
            .map_err(|_| AdmissionDenied("bootstrap lock poisoned"))?
            .is_some();
        if joining
            && let Err(error) = self
                .bounded_phase("learner promotion", self.finish_join(network))
                .await
        {
            tracing::debug!(%error, "local learner promotion deferred");
        }
        let still_joining = self
            .bootstrap_join
            .lock()
            .map_err(|_| AdmissionDenied("bootstrap lock poisoned"))?
            .is_some();
        if still_joining {
            self.extend_join(network).await?;
        } else {
            self.renew(network, None).await?;
        }
        if let Err(error) = self
            .bounded_phase("membership recovery", self.recover_pending(network))
            .await
        {
            tracing::debug!(%error, "pending membership recovery deferred");
        }
        Ok(())
    }

    async fn enter(
        &self,
        network: &RaftNetworkImpl,
        operation_nonce: [u8; 32],
    ) -> anyhow::Result<()> {
        let _round = self.round.lock().await;
        let discovery = self.discover(network, self.rpc_budget()).await?;
        let history = discovery.histories.values().next().cloned();
        if let Some(genesis) = &history {
            anyhow::ensure!(
                discovery.histories.values().all(|other| other == genesis),
                "reachable peers report conflicting genesis"
            );
        }
        let genesis = match history {
            Some(genesis) => genesis,
            None => discovery::cohort_genesis(&self.cfg, discovery.boots.clone())?,
        };
        let cold = genesis.voters.contains(&self.local);
        let mut round = self.begin(
            genesis.clone(),
            if cold {
                AdmissionMode::Cold
            } else {
                AdmissionMode::Join { prepared: None }
            },
        )?;
        let prepared = if cold {
            None
        } else {
            let response = self
                .route(
                    network,
                    ManagementAction::PrepareJoin {
                        genesis: genesis.clone(),
                        operation_nonce,
                    },
                    &discovery.boots,
                )
                .await?;
            let ManagementResult::Prepared(prepared) = response else {
                anyhow::bail!("join preparation response missing");
            };
            anyhow::ensure!(
                prepared.plan.request_nonce == operation_nonce,
                "join operation nonce changed"
            );
            round.bind_prepared(&prepared)?;
            Some(*prepared)
        };
        let targets = if cold {
            &genesis.voters
        } else {
            &discovery.boots
        };
        let grants = self.grants(network, &round, targets).await?;
        self.install(&mut round, &grants, prepared).await?;
        if cold {
            let members = genesis
                .voters
                .iter()
                .map(|replica| Ok((*replica, self.node(*replica)?)))
                .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
            let raft = self.raft()?;
            if !raft.is_initialized().await? {
                raft.initialize(members).await?;
            }
        }
        Ok(())
    }

    pub(super) fn begin(
        &self,
        genesis: Genesis,
        mode: AdmissionMode,
    ) -> anyhow::Result<AdmissionRound> {
        self.ensure_running()?;
        Ok(self
            .core
            .lock()
            .map_err(|_| AdmissionDenied("admission lock poisoned"))?
            .begin(genesis, mode)?)
    }

    pub(super) async fn install(
        &self,
        round: &mut AdmissionRound,
        grants: &[ReceivedGrant],
        bootstrap: Option<PreparedJoin>,
    ) -> anyhow::Result<()> {
        let verified = self
            .core
            .lock()
            .map_err(|_| AdmissionDenied("admission lock poisoned"))?
            .complete(round, grants)?;
        if let Some(prepared) = &bootstrap {
            anyhow::ensure!(
                verified.join_bootstrap().is_some_and(|proof| {
                    proof.prepared().plan == prepared.plan
                        && proof.prepared().prepared == prepared.prepared
                }),
                "bootstrap differs from quorum-verified operation"
            );
        }
        let mut state = self.state.write().await;
        self.ensure_running()?;
        let session = self.authority.accept(&verified)?;
        if matches!(
            verified.mode(),
            AdmissionMode::Cold | AdmissionMode::Join { .. }
        ) {
            self.first_verified_admission
                .lock()
                .map_err(|_| AdmissionDenied("activation lock poisoned"))?
                .get_or_insert(Instant::now());
        }
        if state.admission.is_none() {
            state.bind_admission(session)?;
            *self
                .bootstrap_join
                .lock()
                .map_err(|_| AdmissionDenied("bootstrap lock poisoned"))? =
                verified.join_bootstrap().cloned();
        }
        Ok(())
    }

    pub(super) async fn grants(
        &self,
        network: &RaftNetworkImpl,
        round: &AdmissionRound,
        targets: &BTreeSet<ReplicaId>,
    ) -> anyhow::Result<Vec<ReceivedGrant>> {
        let request = round.request().clone();
        let results = join_all(targets.iter().map(|target| async {
            if *target == self.local {
                let binding = fresh_nonce()?;
                let record = SignedAdmission::sign(
                    self.cfg.cluster_secret.as_deref(),
                    10,
                    self.local,
                    self.local,
                    binding,
                    request.clone(),
                )?;
                let response = tokio::time::timeout(
                    self.rpc_budget(),
                    self.dispatch(self.local, binding, record),
                )
                .await
                .map_err(|_| anyhow::anyhow!("local admission grant timed out"))??;
                Ok(ReceivedGrant::authenticate(
                    self.cfg.cluster_secret.as_deref(),
                    self.local,
                    self.local,
                    binding,
                    &request,
                    response,
                )?)
            } else {
                network
                    .admission_rpc(*target, request.clone(), self.rpc_budget())
                    .await
            }
        }))
        .await;
        let mut grants = Vec::new();
        for result in results {
            match result {
                Ok(grant) => grants.push(grant),
                Err(error) => tracing::debug!(%error, "admission issuer did not grant this round"),
            }
        }
        Ok(grants)
    }

    pub(super) async fn wait_progress(
        &self,
        applied: &AppliedHealthProgress,
    ) -> anyhow::Result<()> {
        tokio::time::timeout(self.rpc_budget(), async {
            loop {
                {
                    let state = self.state.read().await;
                    if state.applied_progress.get(&self.local.physical_id) == Some(applied)
                        && state
                            .last_applied_log
                            .is_some_and(|log| log.index >= applied.log_id.index)
                    {
                        return Ok(());
                    }
                    if state
                        .last_applied_log
                        .is_some_and(|log| log.index >= applied.log_id.index)
                    {
                        anyhow::bail!(
                            "locally applied progress differs from outstanding challenge"
                        );
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("local progress apply timed out"))?
    }

    async fn establish_genesis(&self) -> anyhow::Result<()> {
        let session = self.coherent_session().await?;
        if self.state.read().await.genesis.is_some() {
            return Ok(());
        }
        let raft = self.raft()?;
        if raft.metrics().borrow_watched().current_leader == Some(self.local) {
            let _mutation = self.mutation.lock().await;
            self.commit(
                &raft,
                KafRequest::AdmissionGenesis(session.context().genesis.clone()),
            )
            .await?;
        }
        Ok(())
    }

    async fn finish_join(&self, network: &RaftNetworkImpl) -> anyhow::Result<()> {
        let mut prepared = match self
            .bootstrap_join
            .lock()
            .map_err(|_| AdmissionDenied("bootstrap lock poisoned"))?
            .clone()
        {
            Some(bootstrap) => bootstrap.prepared().clone(),
            None => return Ok(()),
        };
        let state = self.state.read().await;
        if state.genesis.as_ref() != Some(&prepared.plan.genesis)
            || state
                .last_applied_log
                .is_none_or(|log| log.index < prepared.prepared.index)
        {
            return Ok(());
        }
        if let Some(local) = state
            .prepared_join
            .as_ref()
            .filter(|local| local.plan == prepared.plan)
        {
            prepared = local.clone();
        }
        if let Some(completed) =
            state
                .completed_joins
                .get(&self.local.physical_id)
                .filter(|completed| {
                    completed.prepared.plan == prepared.plan
                        && completed.prepared.prepared == prepared.prepared
                })
        {
            prepared = completed.prepared.clone();
        }
        if self.authority.mark_promoted(&prepared, &state).is_ok() {
            drop(state);
            *self
                .bootstrap_join
                .lock()
                .map_err(|_| AdmissionDenied("bootstrap lock poisoned"))? = None;
            return Ok(());
        }
        drop(state);
        let discovery = self.discover(network, self.rpc_budget()).await?;
        let response = self
            .route(
                network,
                ManagementAction::LearnerReady {
                    genesis: prepared.plan.genesis.clone(),
                    operation_nonce: prepared.plan.request_nonce,
                    prepared: prepared.prepared,
                },
                &discovery.boots,
            )
            .await?;
        let ManagementResult::Prepared(acknowledged) = response else {
            anyhow::bail!("learner promotion acknowledgement missing");
        };
        anyhow::ensure!(
            acknowledged.plan == prepared.plan
                && acknowledged.prepared == prepared.prepared
                && acknowledged.learner_applied.is_some(),
            "learner promotion response differs from prepared operation"
        );
        // Only locally applied state can complete promotion; retain the immutable quorum proof.
        Ok(())
    }

    pub(super) async fn route(
        &self,
        network: &RaftNetworkImpl,
        action: ManagementAction,
        peers: &BTreeSet<ReplicaId>,
    ) -> anyhow::Result<ManagementResult> {
        let mut pending: Vec<_> = peers.iter().copied().collect();
        let mut visited = BTreeSet::new();
        while let Some(peer) = pending.pop() {
            if !visited.insert(peer) {
                continue;
            }
            let response = if peer == self.local {
                tokio::time::timeout(self.rpc_budget(), self.manage(peer, action.clone()))
                    .await
                    .map_err(|_| anyhow::anyhow!("local management timed out"))
                    .and_then(|result| result)
            } else {
                network
                    .management_rpc(peer, action.clone(), self.rpc_budget())
                    .await
                    .map(|response| response.result)
            };
            match response {
                Ok(ManagementResult::Redirect(target)) => {
                    if peers.contains(&target) {
                        pending.push(target);
                    }
                }
                Ok(result) => return Ok(result),
                Err(error) => {
                    tracing::debug!(%error, peer = %peer, "management operation deferred")
                }
            }
        }
        anyhow::bail!("no reachable admitted leader completed management operation")
    }
}

fn fresh_nonce() -> anyhow::Result<[u8; 32]> {
    let mut nonce = [0; 32];
    getrandom::fill(&mut nonce)
        .map_err(|_| anyhow::anyhow!("admission driver entropy unavailable"))?;
    Ok(nonce)
}
