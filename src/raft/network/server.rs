//! Raft listener admission, authentication, and inbound stream supervision.

use super::inbound;
use super::wire::{read_handshake, secrets_match};
use crate::config::{ClusterConfigFingerprint, Config, canonical_socket_addr};
use crate::connection_admission::{ConnectionAdmission, FrameByteBudget};
use crate::raft::tasks::{CleanExit, SupervisedTask, spawn_supervised_task};
use crate::raft::{KafRaft, KafStorageState};
use anyhow::Context;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinSet;

const HANDSHAKE_READ_TIMEOUT: Duration = Duration::from_secs(5);
const RAFT_UNAUTHENTICATED_CONNECTION_LIMIT: usize = 32;
const RAFT_AUTHENTICATED_CONNECTION_LIMIT: usize = 64;
const RAFT_INFLIGHT_FRAME_BYTE_LIMIT: usize = 128 * 1024 * 1024;

pub(super) struct RaftAcceptContext {
    pub(super) config: Arc<Config>,
    pub(super) raft: KafRaft,
    pub(super) state_ref: Arc<RwLock<KafStorageState>>,
    pub(super) config_fingerprint: ClusterConfigFingerprint,
    pub(super) shutdown: Arc<AtomicBool>,
}

fn advertised_source_ip(address: &str) -> Option<std::net::IpAddr> {
    address
        .parse::<std::net::SocketAddr>()
        .ok()
        .map(canonical_socket_addr)
        .map(|address| address.ip())
}

fn canonical_source_ip(source: std::net::IpAddr) -> std::net::IpAddr {
    canonical_socket_addr(std::net::SocketAddr::new(source, 0)).ip()
}

pub(super) fn is_known_raft_source(cfg: &Config, source: std::net::IpAddr) -> bool {
    let source = canonical_source_ip(source);
    cfg.peers
        .iter()
        .any(|peer| advertised_source_ip(&peer.raft_address) == Some(source))
}

pub(super) fn raft_source_matches_peer(
    cfg: &Config,
    peer_id: u64,
    source: std::net::IpAddr,
) -> bool {
    let source = canonical_source_ip(source);
    cfg.get_peer(peer_id)
        .and_then(|peer| advertised_source_ip(&peer.raft_address))
        == Some(source)
}

pub(super) fn spawn_raft_accept_task(
    listener: TcpListener,
    context: RaftAcceptContext,
    failure_tx: mpsc::UnboundedSender<String>,
) -> SupervisedTask {
    let shutdown_for_task = context.shutdown.clone();
    spawn_supervised_task(
        "Raft accept task",
        shutdown_for_task,
        failure_tx,
        CleanExit::Unexpected,
        async move {
            let admission = ConnectionAdmission::new(
                RAFT_UNAUTHENTICATED_CONNECTION_LIMIT,
                RAFT_AUTHENTICATED_CONNECTION_LIMIT,
            );
            let frame_byte_budget = FrameByteBudget::new(RAFT_INFLIGHT_FRAME_BYTE_LIMIT);
            let source_admission = crate::admission::ConnectionAdmission::new(
                context
                    .config
                    .peers
                    .iter()
                    .filter_map(|peer| peer.raft_address.parse().ok()),
            );
            let mut inbound_tasks = JoinSet::new();
            loop {
                if context.shutdown.load(Ordering::SeqCst) {
                    break;
                }
                tokio::select! {
                    accepted = listener.accept() => match accepted {
                        Ok((stream, addr)) => spawn_inbound_stream(
                            &mut inbound_tasks,
                            &context,
                            &admission,
                            &source_admission,
                            &frame_byte_budget,
                            stream,
                            addr,
                        ),
                        Err(error) => return Err(error).context("accept Raft connection"),
                    },
                    joined = inbound_tasks.join_next(), if !inbound_tasks.is_empty() => {
                        if let Some(Err(error)) = joined {
                            return Err(anyhow::anyhow!("Raft inbound task failed: {error}"));
                        }
                    }
                }
            }
            Ok(())
        },
    )
}

fn spawn_inbound_stream(
    inbound_tasks: &mut JoinSet<()>,
    context: &RaftAcceptContext,
    admission: &ConnectionAdmission,
    source_admission: &crate::admission::ConnectionAdmission,
    frame_byte_budget: &FrameByteBudget,
    mut stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
) {
    if !is_known_raft_source(&context.config, addr.ip()) {
        tracing::warn!(
            "raft accept from {}: unknown source rejected before admission",
            addr
        );
        return;
    }
    let Some(unauthenticated) = admission.try_begin() else {
        tracing::warn!(
            "raft accept from {}: unauthenticated connection limit reached",
            addr
        );
        return;
    };
    let Some(source_permit) = source_admission.try_acquire(addr.ip()) else {
        tracing::warn!("raft accept from {}: source connection limit reached", addr);
        return;
    };
    let cfg = context.config.clone();
    let raft = context.raft.clone();
    let state_ref = context.state_ref.clone();
    let config_fingerprint = context.config_fingerprint;
    let frame_byte_budget = frame_byte_budget.clone();
    // Lifetime: the accept task owns this stream and its source quota through dispatch.
    inbound_tasks.spawn(async move {
        let _source_permit = source_permit;
        let handshake =
            tokio::time::timeout(HANDSHAKE_READ_TIMEOUT, read_handshake(&mut stream)).await;
        let (peer_id, peer_secret, peer_epoch, peer_supports_v2) = match handshake {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                tracing::warn!("raft accept from {}: handshake io: {}", addr, error);
                return;
            }
            Err(_) => {
                tracing::warn!("raft accept from {}: handshake timeout", addr);
                return;
            }
        };
        if !cfg.peers.iter().any(|peer| peer.id == peer_id) {
            tracing::warn!("raft accept from {}: unknown peer_id {}", addr, peer_id);
            return;
        }
        if !raft_source_matches_peer(&cfg, peer_id, addr.ip()) {
            tracing::warn!(
                "raft accept from {}: source does not match peer_id {}; dropping",
                addr,
                peer_id
            );
            return;
        }
        if !secrets_match(cfg.cluster_secret.as_deref(), peer_secret.as_deref()) {
            tracing::warn!(
                "raft accept from {} (peer_id {}): cluster_secret mismatch; dropping",
                addr,
                peer_id
            );
            return;
        }
        let Ok(_authenticated_connection) = unauthenticated.try_authenticate() else {
            tracing::warn!(
                "raft accept from {} (peer_id {}): authenticated connection limit reached",
                addr,
                peer_id
            );
            return;
        };
        if let Err(error) = inbound::serve_raft_stream(
            raft,
            state_ref,
            stream,
            inbound::InboundPeer {
                id: peer_id,
                epoch: peer_epoch,
                supports_v2: peer_supports_v2,
            },
            inbound::InboundStreamPolicy {
                local_config_fingerprint: config_fingerprint,
                max_frame_bytes: cfg.max_frame_bytes,
                legacy_response_budget: inbound::legacy_response_budget(
                    cfg.raft.heartbeat_interval_ms,
                ),
                frame_byte_budget,
                idle_timeout: Duration::from_secs(5)
                    .max(Duration::from_millis(cfg.raft.heartbeat_interval_ms).saturating_mul(2)),
            },
        )
        .await
        {
            tracing::debug!("raft inbound {} ended: {}", peer_id, error);
        }
    });
}
