//! Inbound peer stream identity gates and OpenRaft RPC dispatch.

use super::SnapshotTransfer;
use super::status;
use super::wire::{
    epochs_compatible, peer_semantics_compatible, read_framed_bounded_with_timeout_and_budget,
    write_framed_tcp,
};
use crate::config::ClusterConfigFingerprint;
use crate::connection_admission::FrameByteBudget;
use crate::raft::KafRaft;
use crate::raft::probe::{ClusterStatusRequest, config_identity_compatible};
use crate::raft::store::KafStorageState;
use crate::raft::types::TypeConfig;
use anyhow::Context;
use openraft::Snapshot;
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::Instant;

pub(super) struct InboundPeer {
    pub(super) id: u64,
    pub(super) epoch: Option<u128>,
    pub(super) supports_v2: bool,
}

pub(super) struct InboundStreamPolicy {
    pub(super) local_config_fingerprint: ClusterConfigFingerprint,
    pub(super) max_frame_bytes: u32,
    pub(super) legacy_response_budget: Duration,
    pub(super) frame_byte_budget: FrameByteBudget,
    pub(super) idle_timeout: Duration,
}

#[derive(Debug, PartialEq, Eq)]
struct ConfigFrameGate {
    is_status: bool,
    compatible: bool,
    peer_supports_cancellation_safe_rpc: Option<bool>,
}

impl ConfigFrameGate {
    fn may_dispatch(&self) -> bool {
        self.is_status || self.compatible
    }
}

fn classify_config_frame(
    frame: &[u8],
    peer_fingerprint: &mut Option<ClusterConfigFingerprint>,
    local_fingerprint: ClusterConfigFingerprint,
    enforcement_active: bool,
) -> ConfigFrameGate {
    let status_request = serde_json::from_slice::<ClusterStatusRequest>(frame).ok();
    if let Some(request) = &status_request
        && let Some(fingerprint) = request.config_fingerprint
    {
        *peer_fingerprint = Some(fingerprint);
    }
    ConfigFrameGate {
        is_status: status_request.is_some(),
        compatible: config_identity_compatible(
            local_fingerprint,
            *peer_fingerprint,
            enforcement_active,
        ),
        peer_supports_cancellation_safe_rpc: status_request
            .as_ref()
            .map(|request| request.supports_cancellation_safe_rpc_v1),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseAction {
    WriteAndKeepOpen,
    DropAndClose,
}

const RAFT_RESPONSE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const RAFT_FRAME_READ_TIMEOUT: Duration = Duration::from_secs(5);
const RAFT_PROCESSING_TIMEOUT: Duration = Duration::from_secs(5);

fn response_action(
    peer_supports_cancellation_safe_rpc: bool,
    is_status: bool,
    dispatch_elapsed: Duration,
    legacy_response_budget: Duration,
) -> ResponseAction {
    if is_status || peer_supports_cancellation_safe_rpc || dispatch_elapsed < legacy_response_budget
    {
        ResponseAction::WriteAndKeepOpen
    } else {
        ResponseAction::DropAndClose
    }
}

async fn write_response_with_budget<F>(
    peer_supports_cancellation_safe_rpc: bool,
    is_status: bool,
    dispatch_elapsed: Duration,
    legacy_response_budget: Duration,
    response_write_timeout: Duration,
    write: F,
) -> std::io::Result<ResponseAction>
where
    F: Future<Output = std::io::Result<()>>,
{
    let action = response_action(
        peer_supports_cancellation_safe_rpc,
        is_status,
        dispatch_elapsed,
        legacy_response_budget,
    );
    if action == ResponseAction::DropAndClose {
        return Ok(action);
    }
    if peer_supports_cancellation_safe_rpc || is_status {
        return match tokio::time::timeout(response_write_timeout, write).await {
            Ok(result) => {
                result?;
                Ok(action)
            }
            Err(_) => Ok(ResponseAction::DropAndClose),
        };
    }

    let remaining = legacy_response_budget
        .saturating_sub(dispatch_elapsed)
        .min(response_write_timeout);
    match tokio::time::timeout(remaining, write).await {
        Ok(result) => {
            result?;
            Ok(action)
        }
        Err(_) => Ok(ResponseAction::DropAndClose),
    }
}

pub(super) fn legacy_response_budget(heartbeat_interval_ms: u64) -> Duration {
    Duration::from_millis(heartbeat_interval_ms.saturating_mul(4) / 5).max(Duration::from_millis(1))
}

pub(super) async fn serve_raft_stream(
    raft: KafRaft,
    state_ref: Arc<RwLock<KafStorageState>>,
    mut stream: TcpStream,
    peer: InboundPeer,
    policy: InboundStreamPolicy,
) -> anyhow::Result<()> {
    let mut peer_config_fingerprint = None;
    // Failover-semantics support and cancellation-safe stream ownership were introduced in
    // different releases. A V2 handshake therefore says nothing about whether this reader drops
    // an incomplete RPC stream; require the dedicated status capability before relaxing the
    // legacy response budget.
    let mut peer_supports_cancellation_safe_rpc = false;
    loop {
        let first = match tokio::time::timeout(policy.idle_timeout, stream.read_u8())
            .await
            .context("raft idle read timed out")?
        {
            Ok(first) => first,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        };
        let mut framed = Cursor::new([first]).chain(&mut stream);
        let buf = match read_framed_bounded_with_timeout_and_budget(
            &mut framed,
            policy.max_frame_bytes,
            RAFT_FRAME_READ_TIMEOUT,
            &policy.frame_byte_budget,
        )
        .await
        {
            Ok(b) => b,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(e.into());
            }
        };

        // #26: state contention counts toward both processing and legacy-response deadlines.
        let dispatch_started = Instant::now();
        let processing = async {
            // Re-read state per frame: activation mid-connection must fence the next Raft RPC.
            let (local_epoch, local_semantics, config_identity_enforced) = {
                let state = state_ref.read().await;
                (
                    state.cluster_epoch,
                    state.failover_semantics,
                    state.config_identity_enforced,
                )
            };
            let config_gate = classify_config_frame(
                buf.as_ref(),
                &mut peer_config_fingerprint,
                policy.local_config_fingerprint,
                config_identity_enforced,
            );
            if let Some(supports_safe_rpc) = config_gate.peer_supports_cancellation_safe_rpc {
                peer_supports_cancellation_safe_rpc = supports_safe_rpc;
            }
            if !peer_semantics_compatible(local_semantics, peer.supports_v2)
                && !config_gate.is_status
            {
                tracing::warn!(
                    "raft inbound rpc from {}: legacy peer rejected after V2 activation",
                    peer.id
                );
                return Ok(None);
            }
            if !config_gate.may_dispatch() {
                tracing::warn!(
                    "raft inbound rpc from {}: cluster configuration identity mismatch; dropping before dispatch",
                    peer.id
                );
                return Ok(None);
            }
            let body = dispatch_incoming(
                &raft,
                local_epoch,
                peer.epoch,
                policy.local_config_fingerprint,
                config_identity_enforced,
                buf.as_ref(),
            )
            .await?;
            Ok::<_, anyhow::Error>(Some((config_gate, body)))
        };
        let (config_gate, body) = match tokio::time::timeout(RAFT_PROCESSING_TIMEOUT, processing)
            .await
            .context("raft request processing timed out")?
        {
            Ok(Some(result)) => result,
            Ok(None) => break,
            Err(e) => {
                tracing::warn!("raft inbound rpc from {}: {}", peer.id, e);
                break;
            }
        };
        if body.len() as u64 > policy.max_frame_bytes as u64 {
            tracing::warn!(
                "raft inbound rpc from {}: response {} bytes exceeds max_frame_bytes {}",
                peer.id,
                body.len(),
                policy.max_frame_bytes
            );
            break;
        }

        let action = write_response_with_budget(
            peer_supports_cancellation_safe_rpc,
            config_gate.is_status,
            dispatch_started.elapsed(),
            policy.legacy_response_budget,
            RAFT_RESPONSE_WRITE_TIMEOUT,
            write_framed_tcp(&stream, &body),
        )
        .await?;
        if action == ResponseAction::DropAndClose {
            tracing::debug!(
                "raft inbound rpc from {}: dropping a late response for a cancellation-unsafe legacy reader",
                peer.id
            );
            break;
        }
        if !config_gate.compatible {
            tracing::warn!(
                "raft status from {}: cluster configuration identity mismatch; preflight answered then stream closed",
                peer.id
            );
            break;
        }
    }
    Ok(())
}

async fn dispatch_incoming(
    raft: &KafRaft,
    local_epoch: Option<u128>,
    peer_epoch: Option<u128>,
    local_config_fingerprint: ClusterConfigFingerprint,
    config_identity_enforced: bool,
    buf: &[u8],
) -> anyhow::Result<Vec<u8>> {
    // Status stays reachable across an identity/epoch mismatch so guards can diagnose and fence.
    if let Ok(req) = serde_json::from_slice::<ClusterStatusRequest>(buf) {
        let resp = status::answer_cluster_status(
            raft,
            local_epoch,
            local_config_fingerprint,
            config_identity_enforced,
            req,
        )
        .await?;
        return Ok(serde_json::to_vec(&resp)?);
    }
    if !epochs_compatible(local_epoch, peer_epoch) {
        anyhow::bail!(
            "cluster_epoch mismatch (local {:?}, peer {:?}); dropping raft rpc",
            local_epoch,
            peer_epoch
        );
    }
    if let Ok(req) = serde_json::from_slice::<AppendEntriesRequest<TypeConfig>>(buf) {
        let resp = raft
            .append_entries(req)
            .await
            .map_err(|e| anyhow::anyhow!("append_entries: {:?}", e))?;
        return Ok(serde_json::to_vec(&resp)?);
    }
    if let Ok(req) = serde_json::from_slice::<SnapshotTransfer>(buf) {
        let snapshot = Snapshot {
            meta: req.meta,
            snapshot: Cursor::new(req.data),
        };
        let resp = raft
            .install_full_snapshot(req.vote, snapshot)
            .await
            .map_err(|e| anyhow::anyhow!("install_full_snapshot: {:?}", e))?;
        return Ok(serde_json::to_vec(&resp)?);
    }
    if let Ok(req) = serde_json::from_slice::<VoteRequest<TypeConfig>>(buf) {
        let resp = raft
            .vote(req)
            .await
            .map_err(|e| anyhow::anyhow!("vote: {:?}", e))?;
        return Ok(serde_json::to_vec(&resp)?);
    }
    Err(anyhow::anyhow!("unrecognized raft rpc frame"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        Config, DEFAULT_MAX_FRAME_BYTES, DEFAULT_SUBMIT_TIMEOUT_MS, HealthConfig, PeerConfig,
        RaftTuneConfig,
    };
    use crate::raft::network::wire::{read_framed_bounded, write_framed, write_handshake};
    use crate::raft::types::TypeConfig;
    use openraft::alias::VoteOf;
    use openraft::raft::VoteRequest;
    use std::net::TcpListener as StdTcpListener;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn fingerprint(byte: u8) -> ClusterConfigFingerprint {
        ClusterConfigFingerprint {
            version: 1,
            digest: [byte; 32],
        }
    }

    #[test]
    fn mismatched_status_is_answered_but_following_raft_frame_is_not_dispatched() {
        let local = fingerprint(1);
        let mut peer = None;
        let status = serde_json::to_vec(&ClusterStatusRequest {
            probe_from: 2,
            config_fingerprint: Some(fingerprint(2)),
            supports_cancellation_safe_rpc_v1: true,
        })
        .unwrap();

        let status_gate = classify_config_frame(&status, &mut peer, local, false);
        assert!(status_gate.is_status);
        assert!(status_gate.may_dispatch());
        assert!(!status_gate.compatible);
        assert_eq!(status_gate.peer_supports_cancellation_safe_rpc, Some(true));

        let legacy_status = serde_json::to_vec(&serde_json::json!({"probe_from": 2})).unwrap();
        let legacy_gate = classify_config_frame(&legacy_status, &mut peer, local, false);
        assert_eq!(legacy_gate.peer_supports_cancellation_safe_rpc, Some(false));

        let raft_gate = classify_config_frame(br#"{"term":1}"#, &mut peer, local, false);
        assert!(!raft_gate.is_status);
        assert!(!raft_gate.may_dispatch());
        assert_eq!(raft_gate.peer_supports_cancellation_safe_rpc, None);
    }

    #[test]
    fn activation_rejects_missing_identity_before_raft_dispatch() {
        let gate = classify_config_frame(br#"{"term":1}"#, &mut None, fingerprint(1), true);

        assert!(!gate.is_status);
        assert!(!gate.may_dispatch());
    }

    #[test]
    fn matching_identity_allows_raft_dispatch() {
        let local = fingerprint(1);
        let gate = classify_config_frame(br#"{"term":1}"#, &mut Some(local), local, true);

        assert!(!gate.is_status);
        assert!(gate.may_dispatch());
    }

    #[test]
    fn legacy_response_policy_reuses_only_work_completed_inside_the_safety_margin() {
        let budget = std::time::Duration::from_millis(200);

        assert_eq!(
            response_action(true, false, std::time::Duration::from_secs(10), budget),
            ResponseAction::WriteAndKeepOpen
        );
        assert_eq!(
            response_action(false, true, std::time::Duration::from_secs(10), budget),
            ResponseAction::WriteAndKeepOpen
        );
        assert_eq!(
            response_action(false, false, std::time::Duration::from_millis(199), budget),
            ResponseAction::WriteAndKeepOpen
        );
        assert_eq!(
            response_action(false, false, budget, budget),
            ResponseAction::DropAndClose
        );
    }

    #[tokio::test]
    async fn legacy_write_that_misses_its_remaining_budget_is_dropped() {
        let action = write_response_with_budget(
            false,
            false,
            std::time::Duration::from_millis(199),
            std::time::Duration::from_millis(200),
            std::time::Duration::from_secs(1),
            std::future::pending::<std::io::Result<()>>(),
        )
        .await
        .unwrap();

        assert_eq!(action, ResponseAction::DropAndClose);
    }

    #[tokio::test]
    async fn authenticated_write_cannot_pin_a_connection_task() {
        let bounded = write_response_with_budget(
            true,
            false,
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(1),
            std::time::Duration::from_millis(20),
            std::future::pending::<std::io::Result<()>>(),
        )
        .await;

        assert_eq!(bounded.unwrap(), ResponseAction::DropAndClose);
    }

    #[tokio::test]
    async fn v2_peer_without_cancellation_capability_uses_legacy_response_budget() {
        let reserved = StdTcpListener::bind(("127.0.0.1", 0)).unwrap();
        let raft_port = reserved.local_addr().unwrap().port();
        drop(reserved);
        let raft_address = format!("127.0.0.1:{raft_port}");
        let cfg = Arc::new(Config {
            node_id: 1,
            raft_listen: raft_address.clone(),
            client_submit_listen: "127.0.0.1:1".into(),
            peers: vec![
                PeerConfig {
                    id: 1,
                    raft_address: raft_address.clone(),
                    client_submit_address: "127.0.0.1:1".into(),
                },
                PeerConfig {
                    id: 2,
                    raft_address: "127.0.0.1:2".into(),
                    client_submit_address: "127.0.0.1:3".into(),
                },
            ],
            vips: Vec::new(),
            health: HealthConfig {
                command: vec!["/bin/true".into()],
                interval_ms: 1_000,
                timeout_ms: 500,
                stale_secs: Some(3),
            },
            raft: RaftTuneConfig::default(),
            cluster_secret: None,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            submit_timeout_ms: DEFAULT_SUBMIT_TIMEOUT_MS,
            address_protocol: crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL,
            dry_run: true,
            notify: None,
            failover_delay_secs: 0,
            failback: true,
            failback_delay_secs: 0,
        });
        let local_fingerprint = cfg.cluster_config_fingerprint().unwrap();
        let (raft, network, state, _fatal_rx, _network_failure_rx, mut control_tasks) =
            crate::raft::start_raft(cfg, Arc::new(Vec::new()))
                .await
                .unwrap();

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let listener_address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(listener_address).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let raft_for_server = raft.clone();
        let server_task = tokio::spawn(async move {
            serve_raft_stream(
                raft_for_server,
                state,
                server,
                InboundPeer {
                    id: 2,
                    epoch: None,
                    supports_v2: true,
                },
                InboundStreamPolicy {
                    local_config_fingerprint: local_fingerprint,
                    max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
                    legacy_response_budget: Duration::ZERO,
                    frame_byte_budget: FrameByteBudget::new(128 * 1024 * 1024),
                    idle_timeout: Duration::from_secs(5),
                },
            )
            .await
        });

        let status = serde_json::to_vec(&ClusterStatusRequest {
            probe_from: 2,
            config_fingerprint: Some(local_fingerprint),
            supports_cancellation_safe_rpc_v1: false,
        })
        .unwrap();
        write_framed(&mut client, &status).await.unwrap();
        read_framed_bounded(&mut client, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap();

        let vote = VoteRequest::<TypeConfig>::new(VoteOf::<TypeConfig>::new(1, 2), None);
        write_framed(&mut client, &serde_json::to_vec(&vote).unwrap())
            .await
            .unwrap();
        let late_response = read_framed_bounded(&mut client, DEFAULT_MAX_FRAME_BYTES).await;
        assert!(
            late_response.is_err(),
            "a V2-only handshake must not imply cancellation-safe stream ownership"
        );

        server_task.await.unwrap().unwrap();
        control_tasks.shutdown().await.unwrap();
        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
    }

    #[test]
    fn legacy_budget_reserves_twenty_percent_of_the_heartbeat() {
        assert_eq!(
            legacy_response_budget(250),
            std::time::Duration::from_millis(200)
        );
    }

    #[tokio::test]
    async fn unrecognized_authenticated_frame_is_closed_without_a_response() {
        let reserved = StdTcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = reserved.local_addr().unwrap().port();
        drop(reserved);
        let address = format!("127.0.0.1:{port}");
        let cfg = Arc::new(Config {
            node_id: 1,
            raft_listen: address.clone(),
            client_submit_listen: "127.0.0.1:1".into(),
            peers: vec![PeerConfig {
                id: 1,
                raft_address: address.clone(),
                client_submit_address: "127.0.0.1:1".into(),
            }],
            vips: Vec::new(),
            health: HealthConfig {
                command: vec!["/bin/true".into()],
                interval_ms: 1_000,
                timeout_ms: 500,
                stale_secs: Some(3),
            },
            raft: RaftTuneConfig::default(),
            cluster_secret: None,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            submit_timeout_ms: DEFAULT_SUBMIT_TIMEOUT_MS,
            address_protocol: crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL,
            dry_run: true,
            notify: None,
            failover_delay_secs: 0,
            failback: true,
            failback_delay_secs: 0,
        });
        let (raft, network, _state, _fatal_rx, _network_failure_rx, mut control_tasks) =
            crate::raft::start_raft(cfg, Arc::new(Vec::new()))
                .await
                .unwrap();

        let mut stream = TcpStream::connect(address).await.unwrap();
        write_handshake(&mut stream, 1, None, None, false)
            .await
            .unwrap();
        stream.write_all(&2_u32.to_be_bytes()).await.unwrap();
        stream.write_all(b"{}").await.unwrap();
        let mut byte = [0_u8; 1];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);

        control_tasks.shutdown().await.unwrap();
        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn legacy_peer_stream_reuses_a_fresh_raft_response() {
        // #26: isolate the reservation/startup gap from parallel localhost socket tests.
        let reserved = StdTcpListener::bind(("127.250.0.2", 0)).unwrap();
        let port = reserved.local_addr().unwrap().port();
        drop(reserved);
        let address = format!("127.250.0.2:{port}");
        let cfg = Arc::new(Config {
            node_id: 1,
            raft_listen: address.clone(),
            client_submit_listen: "127.0.0.1:1".into(),
            peers: vec![PeerConfig {
                id: 1,
                raft_address: address.clone(),
                client_submit_address: "127.0.0.1:1".into(),
            }],
            vips: Vec::new(),
            health: HealthConfig {
                command: vec!["/bin/true".into()],
                interval_ms: 1_000,
                timeout_ms: 500,
                stale_secs: Some(3),
            },
            raft: RaftTuneConfig::default(),
            cluster_secret: None,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            submit_timeout_ms: DEFAULT_SUBMIT_TIMEOUT_MS,
            address_protocol: crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL,
            dry_run: true,
            notify: None,
            failover_delay_secs: 0,
            failback: true,
            failback_delay_secs: 0,
        });
        let (raft, network, _state, _fatal_rx, _network_failure_rx, mut control_tasks) =
            crate::raft::start_raft(cfg, Arc::new(Vec::new()))
                .await
                .unwrap();

        let mut stream = crate::connection_admission::connect_from_advertised(&address, &address)
            .await
            .unwrap();
        write_handshake(&mut stream, 1, None, None, false)
            .await
            .unwrap();
        let status = serde_json::to_vec(&serde_json::json!({"probe_from": 1})).unwrap();
        write_framed(&mut stream, &status).await.unwrap();
        read_framed_bounded(&mut stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap();

        let vote = VoteRequest::<TypeConfig>::new(VoteOf::<TypeConfig>::new(1, 1), None);
        let vote = serde_json::to_vec(&vote).unwrap();
        write_framed(&mut stream, &vote).await.unwrap();
        read_framed_bounded(&mut stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap();

        let second = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            write_framed(&mut stream, &vote).await?;
            read_framed_bounded(&mut stream, DEFAULT_MAX_FRAME_BYTES).await
        })
        .await
        .expect("a fresh legacy response must preserve the rolling-upgrade stream")
        .expect("legacy connection must serve the second fresh Raft response");
        serde_json::from_slice::<openraft::raft::VoteResponse<TypeConfig>>(&second).unwrap();

        control_tasks.shutdown().await.unwrap();
        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
    }
}
