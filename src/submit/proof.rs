//! Signed release records tied to exact admitted boots and a fresh authenticated channel.
use super::*;
use crate::raft::admission::{AdmissionSession, Genesis, ReplicaId, SignedAdmission};

#[derive(Serialize, Deserialize)]
pub(super) struct ReleaseEnvelope {
    pub genesis: Genesis,
    pub request: KafRequest,
}

pub(super) const RELEASE_REQUEST_ROLE: u8 = 14;
pub(super) const RELEASE_RESPONSE_ROLE: u8 = 15;

pub(super) struct ReleaseAuthority {
    pub session: AdmissionSession,
    pub(super) state: Arc<tokio::sync::RwLock<crate::raft::KafStorageState>>,
}

impl ReleaseAuthority {
    pub fn local(&self) -> ReplicaId {
        self.session.context().local_replica
    }

    pub async fn check(&self, peer: ReplicaId) -> anyhow::Result<()> {
        let deadline = self.session.check()?;
        let state = tokio::time::timeout_at(deadline, self.state.read()).await?;
        self.session.check()?;
        let bound = state
            .admission
            .as_ref()
            .context("runtime admission is not bound")?;
        bound.check()?;
        anyhow::ensure!(
            bound.context() == self.session.context()
                && state.genesis.as_ref() == Some(&self.session.context().genesis),
            "release admission differs from committed genesis"
        );
        for replica in [self.local(), peer] {
            let boots: Vec<_> = state
                .last_membership
                .membership()
                .voter_ids()
                .filter(|id| id.physical_id == replica.physical_id)
                .collect();
            anyhow::ensure!(
                boots == [replica],
                "release boot is not the unique committed voter"
            );
        }
        Ok(())
    }
}

pub(super) async fn release_authority(
    cfg: &Config,
    raft: &KafRaft,
) -> anyhow::Result<ReleaseAuthority> {
    tokio::time::timeout(Duration::from_millis(cfg.submit_timeout_ms), async {
        let state = raft
            .with_state_machine(|machine| {
                let state = machine.shared_state();
                Box::pin(async move { state })
            })
            .await?;
        let session = state
            .read()
            .await
            .admission
            .clone()
            .context("runtime has no bound admission session")?;
        session.check()?;
        anyhow::ensure!(
            session.context().local_replica == *raft.node_id()
                && raft.node_id().physical_id == cfg.node_id
                && session.context().genesis.config == cfg.cluster_config_fingerprint()?,
            "release authority differs from local boot or configuration"
        );
        let authority = ReleaseAuthority { session, state };
        authority.check(*raft.node_id()).await?;
        Ok(authority)
    })
    .await
    .context("release admission lookup timed out")?
}

pub(super) async fn forward_release(
    cfg: &Config,
    authority: &ReleaseAuthority,
    leader: ReplicaId,
    request: KafRequest,
) -> anyhow::Result<SubmitResponse> {
    authority.check(leader).await?;
    let address = &cfg
        .get_peer(leader.physical_id)
        .context("release leader is not configured")?
        .client_submit_address;
    let mut stream = connect_from_advertised(&cfg.client_submit_listen, address).await?;
    let authenticated = crate::auth::client_bound(
        &mut stream,
        crate::auth::Peer::for_replica(
            authority.local(),
            Some(authority.session.context().genesis.epoch),
            true,
        ),
        leader.physical_id,
        cfg.cluster_secret.as_deref(),
        crate::auth::Listener::Submit,
    )
    .await?;
    anyhow::ensure!(
        authenticated.peer.replica() == Some(leader),
        "submit leader boot changed"
    );
    authority.check(leader).await?;
    let envelope = SignedAdmission::sign(
        cfg.cluster_secret.as_deref(),
        RELEASE_REQUEST_ROLE,
        authority.local(),
        leader,
        authenticated.binding,
        ReleaseEnvelope {
            genesis: authority.session.context().genesis.clone(),
            request,
        },
    )?;
    let bytes = serde_json::to_vec(&envelope)?;
    write_submit_frame_with_timeout(&mut stream, &bytes, SUBMIT_WRITE_TIMEOUT).await?;
    let bytes = read_framed_bounded(&mut stream, SUBMIT_FRAME_MAX_BYTES).await?;
    let response: SignedAdmission<SubmitResponse> = serde_json::from_slice(&bytes)?;
    response.verify(
        cfg.cluster_secret.as_deref(),
        RELEASE_RESPONSE_ROLE,
        leader,
        authority.local(),
        authenticated.binding,
    )?;
    authority.check(leader).await?;
    let response = response.payload.accepted()?;
    anyhow::ensure!(
        response.log_id.is_some(),
        "release acknowledgement lacks a committed log ID"
    );
    Ok(response)
}

#[cfg(test)]
mod tests;
