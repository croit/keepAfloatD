//! Authenticated, legacy-compatible cluster status probing.

use super::super::KafRaft;
use super::super::probe::{ClusterStatusRequest, ClusterStatusResponse};
use super::wire::{STATUS_FRAME_MAX_BYTES, read_framed_bounded, write_framed, write_handshake};
use crate::config::ClusterConfigFingerprint;
use crate::connection_admission::connect_from_advertised;
use openraft::async_runtime::WatchReceiver;
use std::time::Duration;

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
    local_address: &str,
    address: &str,
    node_id: u64,
    secret: Option<&str>,
    epoch: Option<u128>,
    config_fingerprint: ClusterConfigFingerprint,
    budget: Duration,
) -> anyhow::Result<ClusterStatusResponse> {
    let io = async {
        let mut stream = connect_from_advertised(local_address, address).await?;
        // Legacy handshake lets upgraded nodes discover old peers before V2 activation.
        write_handshake(&mut stream, node_id, secret, epoch, false).await?;
        let body = serde_json::to_vec(&ClusterStatusRequest {
            probe_from: node_id,
            config_fingerprint: Some(config_fingerprint),
            supports_cancellation_safe_rpc_v1: true,
        })?;
        write_framed(&mut stream, &body).await?;
        let resp_buf = read_framed_bounded(&mut stream, STATUS_FRAME_MAX_BYTES).await?;
        Ok::<_, anyhow::Error>(serde_json::from_slice(&resp_buf)?)
    };
    tokio::time::timeout(budget, io)
        .await
        .map_err(|_| anyhow::anyhow!("probe to {address} timed out"))?
}
