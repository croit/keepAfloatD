//! Authenticated discovery only; status never creates admission authority.

use super::super::KafRaft;
use super::super::probe::{ClusterStatusRequest, ClusterStatusResponse};
use super::request::{Operation, decode_payload, encode};
use super::wire::{STATUS_FRAME_MAX_BYTES, read_framed_bounded, write_framed, write_handshake};
use crate::config::ClusterConfigFingerprint;
use crate::connection_admission::connect_from_advertised;
use openraft::async_runtime::WatchReceiver;
use std::time::Duration;

#[derive(Debug)]
pub(in crate::raft) struct UnauthenticatedDiscovery;

impl std::fmt::Display for UnauthenticatedDiscovery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("discovery peer did not authenticate")
    }
}

impl std::error::Error for UnauthenticatedDiscovery {}

/// Build this node's answer from local Raft state without holding a metrics borrow across await.
pub(super) async fn answer_cluster_status(
    raft: &KafRaft,
    local_epoch: Option<u128>,
    local_fingerprint: ClusterConfigFingerprint,
    config_identity_enforced: bool,
    _req: ClusterStatusRequest,
) -> anyhow::Result<ClusterStatusResponse> {
    let initialized = raft
        .is_initialized()
        .await
        .map_err(|e| anyhow::anyhow!("is_initialized: {:?}", e))?;
    let (current_leader, member_count) = {
        let metrics = raft.metrics();
        let m = metrics.borrow_watched();
        (m.current_leader, m.membership_config.nodes().count())
    };
    Ok(ClusterStatusResponse {
        replica: None,
        supports_pre_vote: true,
        initialized,
        current_leader,
        member_count,
        cluster_epoch: local_epoch,
        supports_failover_semantics_v2: true,
        config_fingerprint: Some(local_fingerprint),
        supports_config_identity_v1: true,
        config_identity_enforced,
    })
}

/// Probe one peer over a short-lived connection without disturbing long-lived replication links.
pub(in crate::raft) async fn probe_peer_status(
    cfg: &crate::config::Config,
    address: &str,
    target_id: u64,
    epoch: Option<u128>,
    config_fingerprint: ClusterConfigFingerprint,
    budget: Duration,
) -> anyhow::Result<ClusterStatusResponse> {
    let io = async {
        let mut stream = connect_from_advertised(&cfg.raft_listen, address).await?;
        // Discovery authenticates independently of configuration and epoch compatibility.
        let authenticated = write_handshake(
            &mut stream,
            cfg.node_id,
            target_id,
            cfg.cluster_secret.as_deref(),
            epoch,
            false,
        )
        .await
        .map_err(|error| anyhow::Error::new(error).context(UnauthenticatedDiscovery))?;
        let body = encode(
            Operation::Status,
            &ClusterStatusRequest {
                probe_from: cfg.node_id,
                config_fingerprint: Some(config_fingerprint),
                supports_cancellation_safe_rpc_v1: true,
            },
        )?;
        write_framed(&mut stream, &body).await?;
        let resp_buf = read_framed_bounded(&mut stream, STATUS_FRAME_MAX_BYTES).await?;
        let mut response: ClusterStatusResponse = decode_payload(&resp_buf, Operation::Status)?;
        response.replica = authenticated.replica();
        Ok(response)
    };
    tokio::time::timeout(budget, io).await.map_err(|error| {
        anyhow::Error::new(error).context(format!("probe to {address} timed out"))
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus_freshness::{FailureKind, failure_kind};

    #[tokio::test(start_paused = true)]
    async fn status_timeout_preserves_elapsed_for_publication_classification() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let network = super::super::tests::test_network(65_536, &[1, 2]);
        let result = probe_peer_status(
            &network.config,
            &address,
            2,
            None,
            network.config.cluster_config_fingerprint().unwrap(),
            Duration::ZERO,
        )
        .await;
        drop(listener);
        network.shutdown().await.unwrap();
        let error = result.expect_err("an unanswered status exchange must time out");
        assert!(error.to_string().contains("timed out"), "{error:#}");
        assert!(
            error
                .downcast_ref::<tokio::time::error::Elapsed>()
                .is_some(),
            "status timeout discarded its typed cause: {error:#}"
        );
        assert_eq!(failure_kind(&error), FailureKind::Unavailable);
    }
}
