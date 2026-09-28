//! Submit-only proof negotiation; replicated HealthUpdate remains unchanged (#26).
use super::*;
use crate::raft::TypeConfig;
use openraft::alias::LogIdOf;

#[derive(Serialize, Deserialize)]
enum ProofRequest {
    HealthUpdateWithProof { node_id: u64, healthy: bool },
}

#[derive(Serialize)]
struct ProofEnvelope<'a> {
    secret: Option<&'a str>,
    request: ProofRequest,
}

pub(super) fn decode_request<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<KafRequest, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum WireRequest {
        Proof(ProofRequest),
        Ordinary(KafRequest),
    }
    Ok(match WireRequest::deserialize(deserializer)? {
        WireRequest::Proof(ProofRequest::HealthUpdateWithProof { node_id, healthy }) => {
            KafRequest::HealthUpdate { node_id, healthy }
        }
        WireRequest::Ordinary(request) => request,
    })
}

/// None means the peer accepted only an unhealthy legacy report; never arm VIPs from it.
pub(crate) async fn submit_health(
    cfg: &Arc<Config>,
    raft: &KafRaft,
    healthy: bool,
    on_unavailable: impl FnOnce(),
) -> anyhow::Result<Option<LogIdOf<TypeConfig>>> {
    let budget = Duration::from_millis(cfg.submit_timeout_ms);
    tokio::time::timeout(budget, async {
        let request = KafRequest::HealthUpdate {
            node_id: cfg.node_id,
            healthy,
        };
        let id = match raft.client_write(request).await {
            Ok(response) => Some(response.log_id),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                let leader = match forward.leader_id {
                    Some(leader) => leader,
                    None => raft
                        .current_leader()
                        .await
                        .context("health submit has no leader")?,
                };
                let address = &cfg
                    .get_peer(leader)
                    .context("health leader is not configured")?
                    .client_submit_address;
                forward_health(cfg, address, healthy, on_unavailable).await?
            }
            Err(error) => anyhow::bail!("health raft write: {error:?}"),
        };
        if let Some(id) = id {
            wait_for_applied(raft, id, budget).await?;
        }
        Ok(id)
    })
    .await
    .context("health submit/application proof timed out")?
}

async fn forward_health(
    cfg: &Config,
    address: &str,
    healthy: bool,
    on_unavailable: impl FnOnce(),
) -> anyhow::Result<Option<LogIdOf<TypeConfig>>> {
    let proof = ProofEnvelope {
        secret: cfg.cluster_secret.as_deref(),
        request: ProofRequest::HealthUpdateWithProof {
            node_id: cfg.node_id,
            healthy,
        },
    };
    match forward_client_submit(&cfg.client_submit_listen, address, &proof).await {
        Ok(response) if response.log_id.is_some() => return Ok(response.log_id),
        Ok(_) => tracing::warn!("health acknowledgement lacks an applied log ID; remaining fenced"),
        Err(error) => tracing::warn!("health proof unavailable; publishing unhealthy: {error}"),
    }
    // #26: invalidate the previous lease before the unhealthy fallback can change placement.
    on_unavailable();
    // An old server cannot decode HealthUpdateWithProof, so healthy=true was never applied.
    // The same destination receives only this ordinary unhealthy fallback, never a healthy one.
    let fallback = SubmitEnvelope {
        secret: cfg.cluster_secret.clone(),
        request: KafRequest::HealthUpdate {
            node_id: cfg.node_id,
            healthy: false,
        },
    };
    forward_client_submit(&cfg.client_submit_listen, address, &fallback).await?;
    Ok(None)
}

async fn wait_for_applied(
    raft: &KafRaft,
    id: LogIdOf<TypeConfig>,
    budget: Duration,
) -> anyhow::Result<()> {
    raft.wait(Some(budget))
        .applied_index_at_least(Some(id.index), "health proof applied locally")
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
