//! TCP transport between Raft peers (length-prefixed JSON), reworked for production safety.
//!
//! Wire format
//! -----------
//! 1. Handshake (sender → receiver): 8-byte BE [`Config::node_id`]; then 4-byte BE secret length
//!    `s` followed by `s` bytes of [`Config::cluster_secret`] (zero-length means "no secret");
//!    then a 1-byte cluster-incarnation flag (`0`/`1`) and, when `1`, 16 bytes BE of the sender's
//!    committed cluster incarnation. The incarnation lets a receiver fence Raft RPCs from a peer in
//!    a different cluster lineage (see [`epochs_compatible`]); this is a v1 flag-day wire change.
//! 2. Frames (both directions): 4-byte BE `u32` length followed by JSON body. Receivers refuse
//!    frames larger than [`Config::max_frame_bytes`] before allocation.
//!
//! Concurrency model
//! -----------------
//! Each ordered pair of peers uses two TCP connections (each side initiates one outbound stream
//! to the other's `raft_listen`). Outbound state is a per-peer [`tokio::sync::Mutex`] over an
//! `Option<TcpStream>` inside an immutable `HashMap<u64, Arc<PeerLink>>`. This way a slow or
//! stuck RPC to peer X cannot block heartbeats to peer Y, and no global lock is taken on the
//! send path.
//!
//! Failure handling
//! ----------------
//! On any I/O error (write, read, framing or timeout violation) the offending stream is dropped
//! from its slot, so the next RPC will see `None` and immediately return `Unreachable` while a
//! background reconnect task re-establishes the stream. Each [`RaftNetworkV2`] call honours
//! [`openraft::network::RPCOption::hard_ttl`] via [`tokio::time::timeout`]; this is what turns a
//! half-open connection into a prompt `Timeout` error instead of an unbounded `read_exact`.

use super::probe::{ClusterStatusRequest, ClusterStatusResponse};
use super::types::TypeConfig;
use super::{KafRaft, KafStorageState};
use crate::config::Config;
use anyhow::Context;
use openraft::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::async_runtime::WatchReceiver;
use openraft::error::{
    NetworkError, RPCError, ReplicationClosed, StreamingError, Timeout, Unreachable,
};
use openraft::network::v2::RaftNetworkV2;
use openraft::network::{RPCOption, RPCTypes, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, OptionalSend, Snapshot};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};

/// Per-peer outbound channel. The `address` is fixed at construction time; only the inner
/// `TcpStream` is replaced on (re)connect. Locking is exclusive but per-peer.
struct PeerLink {
    address: String,
    stream: Mutex<Option<TcpStream>>,
}

/// Default RPC budget if OpenRaft passes a zero or absurdly small `hard_ttl`.
///
/// OpenRaft normally derives `hard_ttl` from `heartbeat_interval`; this floor protects against
/// an accidental misconfiguration that would otherwise produce immediate timeouts.
const RPC_MIN_TIMEOUT: Duration = Duration::from_millis(50);

/// Background reconnect loop period when the outbound stream is currently `Some`.
const RECONNECT_PROBE_INTERVAL: Duration = Duration::from_millis(500);

/// Background reconnect loop period when actively retrying a failed connect.
const RECONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(250);

/// Read deadline applied to the handshake on the accepting side; protects against slowloris-style
/// peers that open a TCP socket but never send the prefix.
const HANDSHAKE_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Length cap (bytes) on the cluster_secret field. Mirrors [`Config`] validation.
const MAX_SECRET_BYTES: u32 = 256;

/// Read a length-prefixed JSON frame, refusing oversized payloads before allocating.
async fn read_framed_bounded<R: AsyncReadExt + Unpin>(
    stream: &mut R,
    max_frame_bytes: u32,
) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let n = u32::from_be_bytes(len_buf);
    if n > max_frame_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "frame size {n} exceeds max_frame_bytes {max_frame_bytes}; refusing to allocate"
            ),
        ));
    }
    let mut buf = vec![0u8; n as usize];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Write the handshake: 8 bytes BE node_id, 4 bytes BE secret length, the secret bytes, then a
/// 1-byte incarnation flag and (when set) 16 bytes BE of the local cluster incarnation.
async fn write_handshake<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    node_id: u64,
    secret: Option<&str>,
    epoch: Option<u128>,
) -> std::io::Result<()> {
    stream.write_all(&node_id.to_be_bytes()).await?;
    let secret_bytes = secret.map(str::as_bytes).unwrap_or(&[]);
    let len = secret_bytes.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    if !secret_bytes.is_empty() {
        stream.write_all(secret_bytes).await?;
    }
    match epoch {
        Some(e) => {
            stream.write_all(&[1u8]).await?;
            stream.write_all(&e.to_be_bytes()).await?;
        }
        None => stream.write_all(&[0u8]).await?,
    }
    Ok(())
}

/// Read and validate the inbound handshake. Returns `(peer_id, peer_secret, peer_epoch)` on success.
async fn read_handshake<R: AsyncReadExt + Unpin>(
    stream: &mut R,
) -> std::io::Result<(u64, Option<String>, Option<u128>)> {
    let mut hb = [0u8; 8];
    stream.read_exact(&mut hb).await?;
    let peer_id = u64::from_be_bytes(hb);
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let secret_len = u32::from_be_bytes(len_buf);
    if secret_len > MAX_SECRET_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("handshake secret length {secret_len} too large"),
        ));
    }
    let secret = if secret_len == 0 {
        None
    } else {
        let mut buf = vec![0u8; secret_len as usize];
        stream.read_exact(&mut buf).await?;
        Some(String::from_utf8(buf).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("utf8: {e}"))
        })?)
    };
    let mut flag = [0u8; 1];
    stream.read_exact(&mut flag).await?;
    let epoch = match flag[0] {
        0 => None,
        1 => {
            let mut eb = [0u8; 16];
            stream.read_exact(&mut eb).await?;
            Some(u128::from_be_bytes(eb))
        }
        other => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("handshake incarnation flag {other} invalid"),
            ));
        }
    };
    Ok((peer_id, secret, epoch))
}

/// Whether a peer carrying incarnation `peer` may drive Raft RPCs against a local node holding
/// incarnation `local`. Two concrete, *different* incarnations are incompatible (separate cluster
/// lineages); if either side is `None` (blank / pre-first-commit) the RPC is allowed, so a freshly
/// rebooted diskless node is still absorbed by replication exactly as before this fence existed.
fn epochs_compatible(local: Option<u128>, peer: Option<u128>) -> bool {
    match (local, peer) {
        (Some(l), Some(p)) => l == p,
        _ => true,
    }
}

#[derive(Clone)]
pub struct RaftNetworkImpl {
    config: Arc<Config>,
    /// Immutable map keyed by peer id (excludes self). Per-peer links are interior-mutable via
    /// their own `Mutex<Option<TcpStream>>`; the map shape never changes after construction.
    peers: Arc<HashMap<u64, Arc<PeerLink>>>,
    /// Shared state machine, read to learn this node's committed cluster incarnation for the
    /// handshake (outbound) and for fencing inbound Raft RPCs.
    state_ref: Arc<RwLock<KafStorageState>>,
    shutdown: Arc<AtomicBool>,
}

impl RaftNetworkImpl {
    pub fn new(config: Arc<Config>, state_ref: Arc<RwLock<KafStorageState>>) -> Self {
        let mut peers: HashMap<u64, Arc<PeerLink>> = HashMap::new();
        for p in config.other_peers() {
            peers.insert(
                p.id,
                Arc::new(PeerLink {
                    address: p.raft_address.clone(),
                    stream: Mutex::new(None),
                }),
            );
        }
        Self {
            config,
            peers: Arc::new(peers),
            state_ref,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Bind `raft_listen`, accept peer handshakes, spawn per-peer reconnect loops, and serve
    /// inbound Raft RPCs on accepted-stream tasks.
    pub async fn start(&self, raft: KafRaft) -> anyhow::Result<()> {
        let addr: std::net::SocketAddr = self
            .config
            .raft_listen
            .parse()
            .with_context(|| format!("parse raft_listen {}", self.config.raft_listen))?;
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("Raft listening on {}", addr);

        // Inbound accept loop.
        {
            let shutdown = self.shutdown.clone();
            let cfg = self.config.clone();
            let raft = raft.clone();
            let state_ref = self.state_ref.clone();
            tokio::spawn(async move {
                loop {
                    if shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    match listener.accept().await {
                        Ok((mut stream, addr)) => {
                            let cfg = cfg.clone();
                            let raft = raft.clone();
                            let state_ref = state_ref.clone();
                            tokio::spawn(async move {
                                let handshake = tokio::time::timeout(
                                    HANDSHAKE_READ_TIMEOUT,
                                    read_handshake(&mut stream),
                                )
                                .await;
                                let (peer_id, peer_secret, peer_epoch) = match handshake {
                                    Ok(Ok(v)) => v,
                                    Ok(Err(e)) => {
                                        tracing::warn!(
                                            "raft accept from {}: handshake io: {}",
                                            addr,
                                            e
                                        );
                                        return;
                                    }
                                    Err(_) => {
                                        tracing::warn!(
                                            "raft accept from {}: handshake timeout",
                                            addr
                                        );
                                        return;
                                    }
                                };
                                if !cfg.peers.iter().any(|p| p.id == peer_id) {
                                    tracing::warn!(
                                        "raft accept from {}: unknown peer_id {}",
                                        addr,
                                        peer_id
                                    );
                                    return;
                                }
                                if !secrets_match(
                                    cfg.cluster_secret.as_deref(),
                                    peer_secret.as_deref(),
                                ) {
                                    tracing::warn!(
                                        "raft accept from {} (peer_id {}): cluster_secret mismatch; dropping",
                                        addr,
                                        peer_id
                                    );
                                    return;
                                }
                                if let Err(e) = serve_raft_stream(
                                    raft,
                                    state_ref,
                                    stream,
                                    peer_id,
                                    peer_epoch,
                                    cfg.max_frame_bytes,
                                )
                                .await
                                {
                                    tracing::debug!("raft inbound {} ended: {}", peer_id, e);
                                }
                            });
                        }
                        Err(e) => tracing::error!("raft accept: {}", e),
                    }
                }
            });
        }

        // Per-peer outbound reconnect loop.
        for (peer_id, link) in self.peers.iter() {
            let peer_id = *peer_id;
            let link = link.clone();
            let shutdown = self.shutdown.clone();
            let cfg = self.config.clone();
            let state_ref = self.state_ref.clone();
            tokio::spawn(async move {
                let mut attempts: u32 = 0;
                loop {
                    if shutdown.load(Ordering::SeqCst) {
                        return;
                    }
                    let needs_reconnect = link.stream.lock().await.is_none();
                    if !needs_reconnect {
                        tokio::time::sleep(RECONNECT_PROBE_INTERVAL).await;
                        attempts = 0;
                        continue;
                    }
                    // Advertise the incarnation held at connect time. The inbound side re-reads its
                    // own incarnation per frame, so that direction is always current; a stale
                    // survivor reconnects only after healing, by which point it carries its real
                    // (old) incarnation and is fenced by the peer.
                    let epoch = state_ref.read().await.cluster_epoch;
                    match try_connect_with_handshake(&link.address, &cfg, epoch).await {
                        Ok(stream) => {
                            tracing::info!(
                                "connected raft peer {} at {} (after {} attempts)",
                                peer_id,
                                link.address,
                                attempts.saturating_add(1)
                            );
                            *link.stream.lock().await = Some(stream);
                            attempts = 0;
                        }
                        Err(e) => {
                            attempts = attempts.wrapping_add(1);
                            if attempts == 1 || attempts.is_multiple_of(20) {
                                tracing::debug!(
                                    "raft outbound to peer {} (attempt {}): {}",
                                    peer_id,
                                    attempts,
                                    e
                                );
                            }
                            tokio::time::sleep(RECONNECT_RETRY_INTERVAL).await;
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// Whether [`Self::shutdown`] has been requested. Lets background tasks (e.g. cluster
    /// auto-formation) exit promptly instead of looping after a stop signal.
    pub(super) fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        for link in self.peers.values() {
            *link.stream.lock().await = None;
        }
        Ok(())
    }

    /// Send an RPC and wait for the response, honoring `hard_ttl` and dropping the underlying
    /// stream on any I/O or timeout failure so the reconnect task can re-establish it.
    async fn send_rpc<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        target: u64,
        request: &Req,
        action: RPCTypes,
        hard_ttl: Duration,
    ) -> Result<Resp, RPCError<TypeConfig>> {
        let request_bytes =
            serde_json::to_vec(request).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let max_frame = self.config.max_frame_bytes;
        // Sender-side guard: an over-cap frame can never be delivered. openraft 0.10 dropped the
        // `PayloadTooLarge` chunk-hint error, so surface this as a transport error and let openraft
        // back off and retry with a smaller batch (AppendEntries) or via the snapshot path.
        if request_bytes.len() as u64 > max_frame as u64 {
            return Err(RPCError::Network(NetworkError::from_string(format!(
                "serialized {action:?} rpc is {} bytes, exceeds max_frame_bytes {max_frame}",
                request_bytes.len()
            ))));
        }

        let link = self.peers.get(&target).cloned().ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("unknown peer {}", target),
            )))
        })?;

        let effective_ttl = if hard_ttl < RPC_MIN_TIMEOUT {
            RPC_MIN_TIMEOUT
        } else {
            hard_ttl
        };
        let started = Instant::now();

        let mut guard = link.stream.lock().await;
        let stream = match guard.as_mut() {
            Some(s) => s,
            None => {
                return Err(RPCError::Unreachable(Unreachable::new(
                    &std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "outbound stream not yet established",
                    ),
                )));
            }
        };

        let len_bytes = (request_bytes.len() as u32).to_be_bytes();
        let io = async {
            stream.write_all(&len_bytes).await?;
            stream.write_all(&request_bytes).await?;
            read_framed_bounded(stream, max_frame).await
        };

        let result = tokio::time::timeout(effective_ttl, io).await;
        match result {
            Ok(Ok(resp_buf)) => serde_json::from_slice(&resp_buf)
                .map_err(|e| RPCError::Network(NetworkError::new(&e))),
            Ok(Err(io_err)) => {
                *guard = None;
                Err(RPCError::Network(NetworkError::new(&io_err)))
            }
            Err(_) => {
                *guard = None;
                Err(RPCError::Timeout(Timeout {
                    action,
                    id: self.config.node_id,
                    target,
                    timeout: started.elapsed(),
                }))
            }
        }
    }
}

/// True when no secret is configured locally (anything accepted) or the configured secret matches
/// the value advertised by the peer in its handshake.
fn secrets_match(local: Option<&str>, peer: Option<&str>) -> bool {
    match local {
        None => true,
        Some(s) => peer == Some(s),
    }
}

async fn try_connect_with_handshake(
    address: &str,
    cfg: &Config,
    epoch: Option<u128>,
) -> anyhow::Result<TcpStream> {
    let addr: std::net::SocketAddr = address.parse()?;
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("raft connect {address}"))?;
    write_handshake(
        &mut stream,
        cfg.node_id,
        cfg.cluster_secret.as_deref(),
        epoch,
    )
    .await
    .context("raft handshake write")?;
    Ok(stream)
}

async fn serve_raft_stream(
    raft: KafRaft,
    state_ref: Arc<RwLock<KafStorageState>>,
    mut stream: TcpStream,
    peer_id: u64,
    peer_epoch: Option<u128>,
    max_frame_bytes: u32,
) -> anyhow::Result<()> {
    loop {
        let buf = match read_framed_bounded(&mut stream, max_frame_bytes).await {
            Ok(b) => b,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(e.into());
            }
        };

        // Re-read the local incarnation per frame: a node that forms/joins a cluster mid-connection
        // must start fencing immediately, without waiting for the link to be re-established.
        let local_epoch = state_ref.read().await.cluster_epoch;
        let body = match dispatch_incoming(&raft, local_epoch, peer_epoch, &buf).await {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!("raft inbound rpc from {}: {}", peer_id, e);
                break;
            }
        };
        if body.len() as u64 > max_frame_bytes as u64 {
            tracing::warn!(
                "raft inbound rpc from {}: response {} bytes exceeds max_frame_bytes {}",
                peer_id,
                body.len(),
                max_frame_bytes
            );
            break;
        }

        stream.write_all(&(body.len() as u32).to_be_bytes()).await?;
        stream.write_all(&body).await?;
    }
    Ok(())
}

async fn dispatch_incoming(
    raft: &KafRaft,
    local_epoch: Option<u128>,
    peer_epoch: Option<u128>,
    buf: &[u8],
) -> anyhow::Result<Vec<u8>> {
    // Tried first: the cluster-status probe. Its required `probe_from` field cannot appear in a
    // Raft frame, and Raft frames carry fields this struct lacks, so the two never collide. The
    // probe is answered regardless of incarnation, so a stale survivor can still *discover* that a
    // majority moved to a new incarnation (and then reset itself).
    if let Ok(req) = serde_json::from_slice::<ClusterStatusRequest>(buf) {
        let resp = answer_cluster_status(raft, local_epoch, req).await?;
        return Ok(serde_json::to_vec(&resp)?);
    }
    // Everything below is a Raft RPC. Fence peers from a different cluster lineage *before* handing
    // the frame to OpenRaft, so a stale survivor's higher-term vote/append can never overwrite a
    // majority that reformed without it.
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
    // openraft 0.10 replaces the chunked install-snapshot RPC with a single whole-snapshot
    // transfer; the receiver hands the reconstructed snapshot to `install_full_snapshot`.
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

/// Build this node's answer to a cluster-status probe from its local Raft state. No `.await` is
/// held across the metrics borrow.
async fn answer_cluster_status(
    raft: &KafRaft,
    local_epoch: Option<u128>,
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
    })
}

/// One-shot cluster-status probe to `address` over a short-lived connection, authenticated with
/// the same handshake as Raft RPCs. Used only during startup cluster formation; deliberately
/// independent of the long-lived OpenRaft peer links so it cannot interfere with replication.
pub(super) async fn probe_peer_status(
    address: &str,
    node_id: u64,
    secret: Option<&str>,
    epoch: Option<u128>,
    max_frame_bytes: u32,
    budget: Duration,
) -> anyhow::Result<ClusterStatusResponse> {
    let io = async {
        let addr: std::net::SocketAddr = address.parse()?;
        let mut stream = TcpStream::connect(addr).await?;
        write_handshake(&mut stream, node_id, secret, epoch).await?;
        let req = ClusterStatusRequest {
            probe_from: node_id,
        };
        let body = serde_json::to_vec(&req)?;
        stream.write_all(&(body.len() as u32).to_be_bytes()).await?;
        stream.write_all(&body).await?;
        let resp_buf = read_framed_bounded(&mut stream, max_frame_bytes).await?;
        let resp: ClusterStatusResponse = serde_json::from_slice(&resp_buf)?;
        Ok::<_, anyhow::Error>(resp)
    };
    tokio::time::timeout(budget, io)
        .await
        .map_err(|_| anyhow::anyhow!("probe to {address} timed out"))?
}

pub struct RaftConnection {
    network: Arc<RaftNetworkImpl>,
    target: u64,
}

/// Wire envelope for a whole-snapshot transfer (openraft 0.10's `full_snapshot` replaces the 0.9
/// chunked `install_snapshot` RPC). Carries the sender's vote, the snapshot metadata and the raw
/// snapshot bytes; the receiver reconstructs `Snapshot { meta, snapshot: Cursor::new(data) }` and
/// hands it to `Raft::install_full_snapshot`.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct SnapshotTransfer {
    vote: VoteOf<TypeConfig>,
    meta: SnapshotMetaOf<TypeConfig>,
    data: Vec<u8>,
}

impl RaftNetworkFactory<TypeConfig> for RaftNetworkImpl {
    type Network = RaftConnection;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        RaftConnection {
            network: Arc::new(self.clone()),
            target,
        }
    }
}

impl RaftNetworkV2<TypeConfig> for RaftConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.network
            .send_rpc(
                self.target,
                &rpc,
                RPCTypes::AppendEntries,
                option.hard_ttl(),
            )
            .await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let transfer = SnapshotTransfer {
            vote,
            meta: snapshot.meta,
            data: snapshot.snapshot.into_inner(),
        };
        // Send the whole snapshot in one framed message. `send_rpc`'s frame-size guard still applies;
        // an over-cap snapshot surfaces as a transport error openraft retries. Map the `RPCError`
        // into a `StreamingError` (a `From` impl exists for every transport variant).
        self.network
            .send_rpc::<_, SnapshotResponse<TypeConfig>>(
                self.target,
                &transfer,
                RPCTypes::InstallSnapshot,
                option.hard_ttl(),
            )
            .await
            .map_err(StreamingError::from)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.network
            .send_rpc(self.target, &rpc, RPCTypes::Vote, option.hard_ttl())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        epochs_compatible, read_framed_bounded, read_handshake, secrets_match, write_handshake,
    };

    #[test]
    fn secrets_match_no_local_accepts_anything() {
        assert!(secrets_match(None, None));
        assert!(secrets_match(None, Some("anything")));
    }

    #[test]
    fn secrets_match_requires_exact_match_when_set() {
        assert!(secrets_match(Some("alpha"), Some("alpha")));
        assert!(!secrets_match(Some("alpha"), Some("beta")));
        assert!(!secrets_match(Some("alpha"), None));
    }

    #[test]
    fn epochs_compatible_fences_only_two_concrete_different_incarnations() {
        // Different concrete incarnations are fenced.
        assert!(!epochs_compatible(Some(1), Some(2)));
        // Same incarnation is allowed.
        assert!(epochs_compatible(Some(1), Some(1)));
        // A blank side (either) is always allowed, so diskless reboots still rejoin.
        assert!(epochs_compatible(None, Some(2)));
        assert!(epochs_compatible(Some(1), None));
        assert!(epochs_compatible(None, None));
    }

    #[tokio::test]
    async fn handshake_roundtrips_id_secret_and_epoch() {
        for (secret, epoch) in [
            (None, None),
            (Some("s3cr3t"), None),
            (None, Some(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10_u128)),
            (Some("s3cr3t"), Some(u128::MAX)),
        ] {
            let mut buf: Vec<u8> = Vec::new();
            write_handshake(&mut buf, 42, secret, epoch).await.unwrap();
            let mut cursor = std::io::Cursor::new(buf);
            let (id, got_secret, got_epoch) = read_handshake(&mut cursor).await.unwrap();
            assert_eq!(id, 42);
            assert_eq!(got_secret.as_deref(), secret);
            assert_eq!(got_epoch, epoch);
        }
    }

    fn framed(payload: &[u8]) -> Vec<u8> {
        let mut buf = (payload.len() as u32).to_be_bytes().to_vec();
        buf.extend_from_slice(payload);
        buf
    }

    #[tokio::test]
    async fn read_framed_bounded_roundtrips_exact_payload() {
        let frame = framed(b"hello frame");
        let mut reader: &[u8] = &frame;
        let got = read_framed_bounded(&mut reader, 1024).await.unwrap();
        assert_eq!(got, b"hello frame");
    }

    #[tokio::test]
    async fn read_framed_bounded_rejects_oversize_length_before_allocating() {
        // Only the 4-byte length prefix is present; the claimed size dwarfs the cap, so the
        // read must fail on the prefix alone without attempting to allocate/read the body.
        let header = 1_000_000_u32.to_be_bytes();
        let mut reader: &[u8] = &header;
        let err = read_framed_bounded(&mut reader, 64).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_framed_bounded_errors_on_truncated_body() {
        let mut buf = 5_u32.to_be_bytes().to_vec();
        buf.extend_from_slice(b"ab"); // promises 5 bytes, delivers 2
        let mut reader: &[u8] = &buf;
        let err = read_framed_bounded(&mut reader, 1024).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
