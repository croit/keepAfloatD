//! Process-local admission orchestration for the daemon.

mod discovery;
mod driver;
mod management;
mod recovery;
mod renewal;

use super::{
    AdmissionController as CoreController, AdmissionDenied, AdmissionFence, AdmissionMode,
    AdmissionSession, Genesis, PreparedJoin, ReplicaId, SignedAdmission, VerifiedJoinBootstrap,
};
use crate::config::Config;
use crate::raft::network::authorization::management::{
    ManagementAction, ManagementRequest, ManagementResponse, ManagementResult, REQUEST_ROLE,
    RESPONSE_ROLE,
};
use crate::raft::network::authorization::{
    AdmissionController as NetworkController, AdmissionRpc, RaftAuthorization,
};
use crate::raft::{KafRaft, KafStorageState, RaftNetworkImpl};
use crate::runtime_permission::{LeaseTiming, RuntimeAuthority};
use futures::future::BoxFuture;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublicationReadiness {
    Pending,
    Ready,
}

pub(crate) struct RuntimeDriver {
    cfg: Arc<Config>,
    state: Arc<RwLock<KafStorageState>>,
    core: Mutex<CoreController>,
    authority: Arc<RuntimeAuthority>,
    local: ReplicaId,
    timing: LeaseTiming,
    boot: Instant,
    raft: Mutex<Option<KafRaft>>,
    bootstrap_join: Mutex<Option<VerifiedJoinBootstrap>>,
    first_verified_admission: Mutex<Option<Instant>>,
    pending_progress: Mutex<Option<recovery::PendingProgress>>,
    round: AsyncMutex<()>,
    mutation: AsyncMutex<()>,
    stopped: AtomicBool,
    running: AtomicBool,
}

impl RuntimeDriver {
    pub(crate) fn new(
        cfg: Arc<Config>,
        state: Arc<RwLock<KafStorageState>>,
        timing: LeaseTiming,
        boot: Instant,
    ) -> anyhow::Result<Arc<Self>> {
        let local = ReplicaId::fresh(cfg.node_id)?;
        let core = CoreController::new(cfg.clone(), local, boot, Arc::new(timing))?;
        let authority = RuntimeAuthority::new(local, timing, boot)?;
        Ok(Arc::new(Self {
            cfg,
            state,
            core: Mutex::new(core),
            authority,
            local,
            timing,
            boot,
            raft: Mutex::new(None),
            bootstrap_join: Mutex::new(None),
            round: AsyncMutex::new(()),
            first_verified_admission: Mutex::new(None),
            pending_progress: Mutex::new(None),
            mutation: AsyncMutex::new(()),
            stopped: AtomicBool::new(false),
            running: AtomicBool::new(false),
        }))
    }

    pub(crate) fn attach(&self, raft: KafRaft) -> anyhow::Result<()> {
        let mut attached = self
            .raft
            .lock()
            .map_err(|_| AdmissionDenied("raft lock poisoned"))?;
        self.ensure_running()?;
        anyhow::ensure!(
            *raft.node_id() == self.local,
            "attached Raft has another boot identity"
        );
        anyhow::ensure!(attached.is_none(), "Raft is already attached");
        *attached = Some(raft);
        Ok(())
    }

    pub(crate) fn local_replica(&self) -> ReplicaId {
        self.local
    }

    pub(crate) fn current(&self) -> Option<AdmissionSession> {
        self.ensure_running().ok()?;
        let state = self.state.try_read().ok()?;
        let session = state.admission.as_ref()?;
        self.authority.check(session.context()).ok()?;
        session.check().ok()?;
        Some(session.clone())
    }

    pub(crate) async fn health_publication_ready(&self) -> anyhow::Result<PublicationReadiness> {
        use futures::FutureExt;

        self.ensure_running()?;
        if self.authority.current().is_none() {
            if self.authority.wait_until_sealed().now_or_never().is_some() {
                return Err(AdmissionDenied("runtime admission is sealed").into());
            }
            self.ensure_running()?;
            return Ok(PublicationReadiness::Pending);
        }
        self.coherent_session().await?;
        Ok(PublicationReadiness::Ready)
    }

    async fn coherent_session(&self) -> anyhow::Result<AdmissionSession> {
        self.within_admission(async {
            let state = self.state.read().await;
            let session = state
                .admission
                .as_ref()
                .ok_or(AdmissionDenied("runtime has no bound admission session"))?;
            self.authority.check(session.context())?;
            session.check()?;
            Ok(session.clone())
        })
        .await
    }

    async fn within_admission<T>(
        &self,
        work: impl std::future::Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        tokio::pin!(work);
        loop {
            self.ensure_running()?;
            let (_, deadline) = self
                .authority
                .current()
                .ok_or(AdmissionDenied("runtime has no active admission"))?;
            tokio::select! {
                biased;
                _ = self.authority.wait_until_sealed() => {
                    return Err(AdmissionDenied("runtime admission is sealed").into());
                }
                result = tokio::time::timeout_at(deadline, &mut work) => {
                    if let Ok(result) = result {
                        self.ensure_running()?;
                        self.authority.current()
                            .ok_or(AdmissionDenied("runtime admission expired during work"))?;
                        return result;
                    }
                    // A concurrent verified renewal may have superseded this timer.
                }
            }
        }
    }

    pub(crate) async fn vip_activation_ready(&self) -> bool {
        let elapsed = {
            let Ok(first) = self.first_verified_admission.lock() else {
                return false;
            };
            first
                .and_then(|first| self.timing.vip_activation_deadline(first).ok())
                .is_some_and(|deadline| Instant::now() >= deadline)
        };
        elapsed && self.coherent_session().await.is_ok()
    }

    /// Revoke synchronously and detach the Raft handle before awaiting task cleanup.
    pub(crate) fn shutdown(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.authority.seal();
        self.raft
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }

    fn ensure_running(&self) -> Result<(), AdmissionDenied> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(AdmissionDenied("admission runtime is shut down"));
        }
        Ok(())
    }

    fn raft(&self) -> anyhow::Result<KafRaft> {
        self.ensure_running()?;
        self.raft
            .lock()
            .map_err(|_| AdmissionDenied("raft lock poisoned"))?
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Raft has not been attached"))
    }
}

impl NetworkController for RuntimeDriver {
    fn local_replica(&self) -> ReplicaId {
        self.local
    }

    fn authorize_raft(&self, peer: ReplicaId) -> Result<RaftAuthorization, AdmissionDenied> {
        self.ensure_running()?;
        let state = self
            .state
            .try_read()
            .map_err(|_| AdmissionDenied("coherent admission state is busy"))?;
        self.authorize_state(&state, peer)
    }

    fn authorize_raft_async(
        &self,
        peer: ReplicaId,
    ) -> BoxFuture<'_, Result<RaftAuthorization, AdmissionDenied>> {
        Box::pin(async move {
            self.ensure_running()?;
            let state = self.state.read().await;
            self.authorize_state(&state, peer)
        })
    }

    fn dispatch(
        &self,
        peer: ReplicaId,
        binding: [u8; 32],
        request: AdmissionRpc,
    ) -> BoxFuture<'_, anyhow::Result<AdmissionRpc>> {
        Box::pin(async move {
            self.ensure_running()?;
            request.verify(
                self.cfg.cluster_secret.as_deref(),
                10,
                peer,
                self.local,
                binding,
            )?;
            anyhow::ensure!(
                request.payload.consumer == peer,
                "grant request names another boot"
            );
            if matches!(request.payload.mode, AdmissionMode::Renew { .. }) {
                let deadline = Instant::now() + self.rpc_budget();
                let reserve = self.reserve_grant(&request, binding, Some(deadline));
                tokio::time::timeout_at(deadline, self.within_admission(reserve))
                    .await
                    .map_err(|_| anyhow::anyhow!("issuer progress apply timed out"))?
            } else {
                self.reserve_grant(&request, binding, None).await
            }
        })
    }

    fn management(
        &self,
        peer: ReplicaId,
        binding: [u8; 32],
        request: SignedAdmission<ManagementRequest>,
    ) -> BoxFuture<'_, anyhow::Result<SignedAdmission<ManagementResponse>>> {
        Box::pin(async move {
            self.ensure_running()?;
            request.verify(
                self.cfg.cluster_secret.as_deref(),
                REQUEST_ROLE,
                peer,
                self.local,
                binding,
            )?;
            anyhow::ensure!(
                self.cfg.get_peer(peer.physical_id).is_some(),
                "management peer is not configured"
            );
            let result = self.manage(peer, request.payload.action).await?;
            self.ensure_running()?;
            Ok(SignedAdmission::sign(
                self.cfg.cluster_secret.as_deref(),
                RESPONSE_ROLE,
                self.local,
                peer,
                binding,
                ManagementResponse {
                    nonce: request.payload.nonce,
                    result,
                },
            )?)
        })
    }
}

impl RuntimeDriver {
    async fn reserve_grant(
        &self,
        request: &AdmissionRpc,
        binding: [u8; 32],
        deadline: Option<Instant>,
    ) -> anyhow::Result<AdmissionRpc> {
        loop {
            let state = self.state.read().await;
            self.ensure_running()?;
            if let AdmissionMode::Renew {
                progress: Some(progress),
            } = &request.payload.mode
            {
                let session = state
                    .admission
                    .as_ref()
                    .ok_or(AdmissionDenied("issuer has no continuing authority"))?;
                self.authority.check(session.context())?;
                session.check()?;
                anyhow::ensure!(
                    session.context().genesis == request.payload.genesis
                        && state.genesis.as_ref() == Some(&request.payload.genesis),
                    "renewal genesis differs from issuer authority"
                );
                if state
                    .last_applied_log
                    .is_none_or(|applied| applied.index < progress.index)
                {
                    // Commit at the consumer can precede this issuer's state-machine apply.
                    drop(state);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
            }
            let mut core = self
                .core
                .lock()
                .map_err(|_| AdmissionDenied("admission lock poisoned"))?;
            anyhow::ensure!(
                deadline.is_none_or(|deadline| Instant::now() < deadline),
                "issuer progress apply timed out"
            );
            return Ok(core.reserve(request, binding, Some(&state))?);
        }
    }

    fn authorize_state(
        &self,
        state: &KafStorageState,
        peer: ReplicaId,
    ) -> Result<RaftAuthorization, AdmissionDenied> {
        self.ensure_running()?;
        let session = state
            .admission
            .as_ref()
            .ok_or(AdmissionDenied("runtime has no admission session"))?;
        self.authority.check(session.context())?;
        let bootstrap = self
            .bootstrap_join
            .lock()
            .map_err(|_| AdmissionDenied("bootstrap admission state is poisoned"))?
            .clone();
        RaftAuthorization::from_state(&self.cfg, state, peer, bootstrap.as_ref())
    }
}

#[cfg(test)]
mod bootstrap_tests;
#[cfg(test)]
mod budget_tests;
#[cfg(test)]
mod discovery_tests;
#[cfg(test)]
mod formation_tests;
#[cfg(test)]
mod grant_tests;
#[cfg(test)]
mod handoff_tests;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod tests;
