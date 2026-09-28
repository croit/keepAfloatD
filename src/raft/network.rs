//! TCP transport between Raft peers (length-prefixed JSON), reworked for production safety.
//!
//! Wire format
//! -----------
//! 1. Handshake (sender to receiver): 8-byte BE [`Config::node_id`]; then 4-byte BE secret length
//!    `s` followed by `s` bytes of [`Config::cluster_secret`] (zero-length means "no secret");
//!    then a 1-byte semantics/incarnation flag: `0` legacy without incarnation, `1` legacy with
//!    incarnation, `2` V2 without incarnation, or `3` V2 with incarnation. Flags `1`/`3` are
//!    followed by the 16-byte BE committed incarnation. Incarnations fence a different cluster
//!    lineage (see [`wire::epochs_compatible`]); the V2 bit fences legacy Raft frames only after
//!    the cluster commits V2 activation.
//! 2. Frames (both directions): 4-byte BE `u32` length followed by JSON body. Receivers refuse
//!    frames larger than their cap before allocation: [`Config::max_frame_bytes`] for Raft frames,
//!    a fixed 64 KiB cap for status/preflight frames and a fixed 4 KiB cap for submit envelopes.
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
//! background reconnect task re-establishes the stream. Each
//! [`RaftNetworkV2`](openraft::network::v2::RaftNetworkV2) call honours
//! [`openraft::network::RPCOption::hard_ttl`] via [`tokio::time::timeout`]; this is what turns a
//! half-open connection into a prompt `Timeout` error instead of an unbounded `read_exact`.
//! Cancellation during an in-flight exchange also drops the stream so its unread response cannot
//! be consumed by a later RPC. During a rolling upgrade, status preflight advertises that behavior.
//! A cancellation-unsafe legacy reader reuses only responses dispatched and accepted by one TCP
//! write inside the timeout safety margin; a later response is dropped and the connection closes.

mod client;
mod inbound;
mod server;
mod status;
mod wire;

#[cfg(test)]
mod deadline_tests;

pub(super) use status::probe_peer_status;

use super::tasks::{CleanExit, SupervisedTask, spawn_supervised_task, stop_supervised_tasks};
use super::types::{FailoverSemantics, TypeConfig};
use super::{KafRaft, KafStorageState};
use crate::config::{ClusterConfigFingerprint, Config};
use anyhow::Context;
use openraft::alias::{SnapshotMetaOf, VoteOf};
use openraft::error::{NetworkError, RPCError, Timeout, Unreachable};
use openraft::network::RPCTypes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, mpsc};
use wire::{
    InFlightRpcStream, connect_with_handshake, connect_with_legacy_handshake,
    connection_requires_config_identity, connection_requires_upgrade, read_framed_bounded,
    write_framed,
};

/// Per-peer outbound channel. The `address` is fixed at construction time; only the inner
/// `TcpStream` is replaced on (re)connect. Locking is exclusive but per-peer.
struct PeerLink {
    address: String,
    stream: Mutex<Option<TcpStream>>,
    advertises_v2: AtomicBool,
    advertises_config_identity: AtomicBool,
    config_identity_learned: AtomicBool,
}

/// Wire envelope shared by the outbound OpenRaft adapter and inbound dispatcher.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct SnapshotTransfer {
    vote: VoteOf<TypeConfig>,
    meta: SnapshotMetaOf<TypeConfig>,
    data: Vec<u8>,
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

/// An aborted async network task should join immediately. Keep an outer bound so shutdown cannot
/// hang even if a future violates Tokio's cancellation contract (#25).
const NETWORK_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

fn is_safety_fence_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("cluster_epoch mismatch")
            || message.contains("cluster configuration identity mismatch")
    })
}

/// Reconnecting a peer already authenticated as legacy must not depend on that legacy Raft
/// actor answering another status RPC during an election (#26). Once identity enforcement is
/// active, every reconnect must preflight again so a legacy or changed peer is fenced.
fn should_reuse_known_legacy_identity(
    config_identity_enforced: bool,
    identity_learned: bool,
    peer_advertises_identity: bool,
) -> bool {
    !config_identity_enforced && identity_learned && !peer_advertises_identity
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
    config_fingerprint: ClusterConfigFingerprint,
    shutdown: Arc<AtomicBool>,
    /// Set by the first `start`; a second start would push duplicate accept and reconnect tasks.
    started: Arc<AtomicBool>,
    tasks: Arc<Mutex<Vec<SupervisedTask>>>,
}

/// Claim the single allowed `start`; fails if it was already claimed.
fn claim_start(started: &AtomicBool) -> anyhow::Result<()> {
    anyhow::ensure!(
        !started.swap(true, Ordering::SeqCst),
        "raft network already started"
    );
    Ok(())
}

impl RaftNetworkImpl {
    pub fn new(
        config: Arc<Config>,
        state_ref: Arc<RwLock<KafStorageState>>,
    ) -> anyhow::Result<Self> {
        let config_fingerprint = config.cluster_config_fingerprint()?;
        let mut peers: HashMap<u64, Arc<PeerLink>> = HashMap::new();
        for p in config.other_peers() {
            peers.insert(
                p.id,
                Arc::new(PeerLink {
                    address: p.raft_address.clone(),
                    stream: Mutex::new(None),
                    advertises_v2: AtomicBool::new(false),
                    advertises_config_identity: AtomicBool::new(false),
                    config_identity_learned: AtomicBool::new(false),
                }),
            );
        }
        Ok(Self {
            config,
            peers: Arc::new(peers),
            state_ref,
            config_fingerprint,
            shutdown: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            tasks: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Bind `raft_listen`, accept peer handshakes, spawn per-peer reconnect loops, and serve
    /// inbound Raft RPCs on accepted-stream tasks.
    pub async fn start(&self, raft: KafRaft) -> anyhow::Result<mpsc::UnboundedReceiver<String>> {
        claim_start(&self.started)?;
        let addr: std::net::SocketAddr = self
            .config
            .raft_listen
            .parse()
            .with_context(|| format!("parse raft_listen {}", self.config.raft_listen))?;
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("Raft listening on {}", addr);
        let (failure_tx, failure_rx) = mpsc::unbounded_channel();

        let accept_task = server::spawn_raft_accept_task(
            listener,
            server::RaftAcceptContext {
                config: self.config.clone(),
                raft: raft.clone(),
                state_ref: self.state_ref.clone(),
                config_fingerprint: self.config_fingerprint,
                shutdown: self.shutdown.clone(),
            },
            failure_tx.clone(),
        );
        self.tasks.lock().await.push(accept_task);

        // Per-peer outbound reconnect loop.
        for (peer_id, link) in self.peers.iter() {
            let peer_id = *peer_id;
            let link = link.clone();
            let shutdown = self.shutdown.clone();
            let cfg = self.config.clone();
            let state_ref = self.state_ref.clone();
            let config_fingerprint = self.config_fingerprint;
            let failure_tx = failure_tx.clone();
            let shutdown_for_task = shutdown.clone();
            let task = spawn_supervised_task(
                "Raft reconnect task",
                shutdown_for_task,
                failure_tx,
                CleanExit::Unexpected,
                async move {
                    let mut attempts: u32 = 0;
                    let mut safety_fence_visible = false;
                    loop {
                        if shutdown.load(Ordering::SeqCst) {
                            return Ok(());
                        }
                        let (local_semantics, config_identity_enforced) = {
                            let state = state_ref.read().await;
                            (state.failover_semantics, state.config_identity_enforced)
                        };
                        let needs_reconnect = {
                            let mut stream = link.stream.lock().await;
                            if stream.is_some()
                                && (connection_requires_upgrade(
                                    local_semantics,
                                    link.advertises_v2.load(Ordering::SeqCst),
                                ) || connection_requires_config_identity(
                                    config_identity_enforced,
                                    link.advertises_config_identity.load(Ordering::SeqCst),
                                ))
                            {
                                *stream = None;
                            }
                            stream.is_none()
                        };
                        if !needs_reconnect {
                            tokio::time::sleep(RECONNECT_PROBE_INTERVAL).await;
                            attempts = 0;
                            continue;
                        }
                        // Advertise the incarnation held at connect time. The inbound side re-reads its
                        // own incarnation per frame, so that direction is always current; a stale
                        // survivor reconnects only after healing, by which point it carries its real
                        // (old) incarnation and is fenced by the peer.
                        let (epoch, advertises_v2, config_identity_enforced) = {
                            let state = state_ref.read().await;
                            (
                                state.cluster_epoch,
                                state.failover_semantics == FailoverSemantics::V2,
                                state.config_identity_enforced,
                            )
                        };
                        let reuse_known_legacy = should_reuse_known_legacy_identity(
                            config_identity_enforced,
                            link.config_identity_learned.load(Ordering::SeqCst),
                            link.advertises_config_identity.load(Ordering::SeqCst),
                        );
                        let connection = if reuse_known_legacy {
                            connect_with_legacy_handshake(&link.address, &cfg, epoch, advertises_v2)
                                .await
                                .map(|stream| (stream, false))
                        } else {
                            connect_with_handshake(
                                &link.address,
                                &cfg,
                                epoch,
                                advertises_v2,
                                config_fingerprint,
                                config_identity_enforced,
                            )
                            .await
                        };
                        match connection {
                            Ok((stream, peer_advertises_config_identity)) => {
                                let (current_semantics, current_identity_enforced) = {
                                    let state = state_ref.read().await;
                                    (state.failover_semantics, state.config_identity_enforced)
                                };
                                if connection_requires_upgrade(current_semantics, advertises_v2)
                                    || connection_requires_config_identity(
                                        current_identity_enforced,
                                        peer_advertises_config_identity,
                                    )
                                {
                                    continue;
                                }
                                tracing::info!(
                                    "connected raft peer {} at {} (after {} attempts)",
                                    peer_id,
                                    link.address,
                                    attempts.saturating_add(1)
                                );
                                link.advertises_v2.store(advertises_v2, Ordering::SeqCst);
                                link.advertises_config_identity
                                    .store(peer_advertises_config_identity, Ordering::SeqCst);
                                if !reuse_known_legacy {
                                    link.config_identity_learned.store(true, Ordering::SeqCst);
                                }
                                *link.stream.lock().await = Some(stream);
                                attempts = 0;
                                safety_fence_visible = false;
                            }
                            Err(e) => {
                                attempts = attempts.wrapping_add(1);
                                if is_safety_fence_error(&e) {
                                    if !safety_fence_visible {
                                        tracing::warn!(
                                            "raft outbound to peer {} fenced (attempt {}): {}",
                                            peer_id,
                                            attempts,
                                            e
                                        );
                                        safety_fence_visible = true;
                                    }
                                } else {
                                    safety_fence_visible = false;
                                    if attempts == 1 || attempts.is_multiple_of(20) {
                                        tracing::debug!(
                                            "raft outbound to peer {} (attempt {}): {}",
                                            peer_id,
                                            attempts,
                                            e
                                        );
                                    }
                                }
                                tokio::time::sleep(RECONNECT_RETRY_INTERVAL).await;
                            }
                        }
                    }
                },
            );
            self.tasks.lock().await.push(task);
        }

        drop(failure_tx);
        Ok(failure_rx)
    }

    /// Whether [`Self::shutdown`] has been requested. Lets background tasks (e.g. cluster
    /// auto-formation) exit promptly instead of looping after a stop signal.
    pub(super) fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    pub(super) fn config_fingerprint(&self) -> ClusterConfigFingerprint {
        self.config_fingerprint
    }

    /// Drop streams opened with the legacy handshake so reconnects immediately advertise V2.
    pub(super) async fn drop_legacy_outbound(&self) {
        for link in self.peers.values() {
            if !link.advertises_v2.load(Ordering::SeqCst) {
                *link.stream.lock().await = None;
            }
        }
    }

    /// Drop rolling-upgrade links whose preflight response omitted config identity.
    pub(super) async fn drop_legacy_config_outbound(&self) {
        for link in self.peers.values() {
            if !link.advertises_config_identity.load(Ordering::SeqCst) {
                *link.stream.lock().await = None;
            }
        }
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        let tasks = std::mem::take(&mut *self.tasks.lock().await);
        let task_result = stop_supervised_tasks(tasks, NETWORK_TASK_SHUTDOWN_TIMEOUT).await;
        for link in self.peers.values() {
            *link.stream.lock().await = None;
        }
        if let Err(error) = task_result {
            anyhow::bail!("network shutdown failed: {error}")
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

        let (local_semantics, config_identity_enforced) = {
            let state = self.state_ref.read().await;
            (state.failover_semantics, state.config_identity_enforced)
        };
        let mut guard = link.stream.lock().await;
        if connection_requires_upgrade(local_semantics, link.advertises_v2.load(Ordering::SeqCst))
            || connection_requires_config_identity(
                config_identity_enforced,
                link.advertises_config_identity.load(Ordering::SeqCst),
            )
        {
            *guard = None;
        }
        if guard.is_none() {
            return Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "outbound stream not yet established",
                ),
            )));
        }
        let mut in_flight = InFlightRpcStream::new(guard);
        let stream = in_flight.stream_mut().ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "outbound stream disappeared before rpc",
            )))
        })?;

        let io = async {
            write_framed(stream, &request_bytes).await?;
            read_framed_bounded(stream, max_frame).await
        };

        let result = tokio::time::timeout(effective_ttl, io).await;
        match result {
            Ok(Ok(resp_buf)) => match serde_json::from_slice(&resp_buf) {
                Ok(response) => {
                    in_flight.retain();
                    Ok(response)
                }
                Err(e) => Err(RPCError::Network(NetworkError::new(&e))),
            },
            Ok(Err(io_err)) => Err(RPCError::Network(NetworkError::new(&io_err))),
            Err(_) => Err(RPCError::Timeout(Timeout {
                action,
                id: self.config.node_id,
                target,
                timeout: started.elapsed(),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::client::RaftConnection;
    use super::server::{is_known_raft_source, raft_source_matches_peer};
    use super::{
        RPCError, RPCTypes, RaftNetworkImpl, is_safety_fence_error, read_framed_bounded,
        should_reuse_known_legacy_identity,
    };
    use crate::config::{
        Config, DEFAULT_SUBMIT_TIMEOUT_MS, HealthConfig, PeerConfig, RaftTuneConfig,
    };
    use crate::raft::store::new_store;
    use crate::raft::tasks::{CleanExit, spawn_supervised_task};
    use crate::raft::types::TypeConfig;
    use openraft::SnapshotMeta;
    use openraft::alias::{StoredMembershipOf, VoteOf};
    use openraft::network::RPCOption;
    use openraft::network::v2::RaftNetworkV2;
    use serde::ser::Error as _;
    use serde::{Serialize, Serializer};
    use serde_json::{Value, json};
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{mpsc, oneshot};

    #[test]
    fn safety_fence_errors_are_operator_visible() {
        assert!(is_safety_fence_error(&anyhow::anyhow!(
            "cluster_epoch mismatch during config preflight"
        )));
        assert!(is_safety_fence_error(&anyhow::anyhow!(
            "cluster configuration identity mismatch"
        )));
        assert!(!is_safety_fence_error(&anyhow::anyhow!(
            "connection refused"
        )));
    }

    #[test]
    fn network_start_can_be_claimed_only_once() {
        let started = AtomicBool::new(false);
        super::claim_start(&started).unwrap();
        let err = super::claim_start(&started).unwrap_err().to_string();
        assert!(err.contains("already started"), "{err}");
    }

    #[test]
    fn known_legacy_identity_reconnect_skips_only_before_activation() {
        assert!(should_reuse_known_legacy_identity(false, true, false));
        assert!(!should_reuse_known_legacy_identity(true, true, false));
        assert!(!should_reuse_known_legacy_identity(false, false, false));
        assert!(!should_reuse_known_legacy_identity(false, true, true));
    }

    #[test]
    fn raft_admission_rejects_unknown_and_impersonated_sources() {
        let network = test_network(1_024, &[1, 2]);
        let loopback = "127.0.0.1".parse().unwrap();
        let unknown = "192.0.2.10".parse().unwrap();

        assert!(is_known_raft_source(&network.config, loopback));
        assert!(!is_known_raft_source(&network.config, unknown));
        assert!(raft_source_matches_peer(&network.config, 2, loopback));
        assert!(!raft_source_matches_peer(&network.config, 2, unknown));
        assert!(!raft_source_matches_peer(&network.config, 99, loopback));
    }

    #[tokio::test]
    async fn network_task_supervisor_reports_exit_and_panic() {
        for future in [
            tokio::spawn(async { Ok::<_, anyhow::Error>(()) }),
            tokio::spawn(async {
                panic!("network test panic");
                #[allow(unreachable_code)]
                Ok::<_, anyhow::Error>(())
            }),
        ] {
            let shutdown = Arc::new(AtomicBool::new(false));
            let (failure_tx, mut failure_rx) = mpsc::unbounded_channel();
            let supervised = spawn_supervised_task(
                "test network task",
                shutdown,
                failure_tx,
                CleanExit::Unexpected,
                async move {
                    future
                        .await
                        .map_err(|error| anyhow::anyhow!("inner task: {error}"))?
                },
            );

            assert!(supervised.handle.await.unwrap().is_err());
            assert!(
                failure_rx
                    .recv()
                    .await
                    .unwrap()
                    .contains("test network task")
            );
        }
    }

    pub(super) fn test_network(max_frame_bytes: u32, peer_ids: &[u64]) -> RaftNetworkImpl {
        let peers: Vec<PeerConfig> = peer_ids
            .iter()
            .map(|id| PeerConfig {
                id: *id,
                raft_address: format!("127.0.0.1:{}", 17_000 + id),
                client_submit_address: format!("127.0.0.1:{}", 18_000 + id),
            })
            .collect();
        let config = Arc::new(Config {
            node_id: 1,
            raft_listen: peers[0].raft_address.clone(),
            client_submit_listen: peers[0].client_submit_address.clone(),
            peers,
            vips: Vec::new(),
            health: HealthConfig {
                command: vec!["/bin/true".into()],
                interval_ms: 1_000,
                timeout_ms: 500,
                stale_secs: Some(3),
            },
            raft: RaftTuneConfig::default(),
            cluster_secret: Some("network-test-secret".into()),
            max_frame_bytes,
            submit_timeout_ms: DEFAULT_SUBMIT_TIMEOUT_MS,
            address_protocol: crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL,
            dry_run: true,
            notify: None,
            failover_delay_secs: 0,
            failback: true,
            failback_delay_secs: 0,
        });
        let (_, _, state_ref) = new_store(Arc::new(Vec::new()), 3, true, 0);
        RaftNetworkImpl::new(config, state_ref).unwrap()
    }

    async fn attach_loopback_stream(network: &RaftNetworkImpl, target: u64) -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client, accepted) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(TcpStream::connect(address), listener.accept())
        })
        .await
        .expect("loopback connection must not stall");
        *network.peers.get(&target).unwrap().stream.lock().await = Some(client.unwrap());
        accepted.unwrap().0
    }

    struct RejectSerialize;

    impl Serialize for RejectSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(S::Error::custom("intentional serialization failure"))
        }
    }

    #[tokio::test]
    async fn send_rpc_rejects_serialization_oversize_unknown_and_disconnected_inputs() {
        let network = test_network(1_024, &[1, 2]);

        let serialization = network
            .send_rpc::<_, Value>(2, &RejectSerialize, RPCTypes::Vote, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(serialization, RPCError::Network(_)));

        let unknown = network
            .send_rpc::<_, Value>(99, &json!(0), RPCTypes::Vote, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(unknown, RPCError::Unreachable(_)));

        let disconnected = network
            .send_rpc::<_, Value>(2, &json!(0), RPCTypes::Vote, Duration::ZERO)
            .await
            .unwrap_err();
        assert!(matches!(disconnected, RPCError::Unreachable(_)));

        let tiny_frame_network = test_network(4, &[1, 2]);
        let oversize = tiny_frame_network
            .send_rpc::<_, Value>(
                2,
                &json!({"larger": "than-four-bytes"}),
                RPCTypes::AppendEntries,
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(matches!(oversize, RPCError::Network(_)));
        assert!(oversize.to_string().contains("exceeds max_frame_bytes 4"));
    }

    #[tokio::test]
    async fn send_rpc_roundtrips_a_valid_frame_with_the_minimum_ttl_floor() {
        let network = test_network(1_024, &[1, 2]);
        let mut server = attach_loopback_stream(&network, 2).await;

        // Lifetime: serves exactly one request and is joined before this test returns.
        let server_task = tokio::spawn(async move {
            let request = read_framed_bounded(&mut server, 1_024).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&request).unwrap(),
                json!({"request": 1})
            );
            let response = serde_json::to_vec(&json!({"response": 2})).unwrap();
            server
                .write_all(&(response.len() as u32).to_be_bytes())
                .await
                .unwrap();
            server.write_all(&response).await.unwrap();
        });

        let response: Value = network
            .send_rpc(2, &json!({"request": 1}), RPCTypes::Vote, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(response, json!({"response": 2}));
        server_task.await.unwrap();
        assert!(network.peers.get(&2).unwrap().stream.lock().await.is_some());
    }

    #[tokio::test]
    async fn send_rpc_surfaces_a_malformed_json_response_as_a_network_error() {
        let network = test_network(1_024, &[1, 2]);
        let mut server = attach_loopback_stream(&network, 2).await;

        // Lifetime: serves exactly one malformed response and is joined before this test returns.
        let server_task = tokio::spawn(async move {
            read_framed_bounded(&mut server, 1_024).await.unwrap();
            let response = b"not-json";
            server
                .write_all(&(response.len() as u32).to_be_bytes())
                .await
                .unwrap();
            server.write_all(response).await.unwrap();
        });

        let error = network
            .send_rpc::<_, Value>(2, &json!(0), RPCTypes::Vote, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(error, RPCError::Network(_)));
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn full_snapshot_maps_an_oversize_transfer_to_a_streaming_error() {
        let network = test_network(16, &[1, 2]);
        let mut connection = RaftConnection {
            network: Arc::new(network),
            target: 2,
        };
        let snapshot = openraft::Snapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembershipOf::<TypeConfig>::default(),
                snapshot_id: "oversize-network-test".into(),
            },
            snapshot: Cursor::new(vec![0; 128]),
        };

        let error = connection
            .full_snapshot(
                VoteOf::<TypeConfig>::new(1, 1),
                snapshot,
                std::future::pending(),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeds max_frame_bytes 16"));
    }

    #[tokio::test]
    async fn send_rpc_drops_stream_after_framing_error_and_timeout() {
        let framing_network = test_network(32, &[1, 2]);
        let mut framing_server = attach_loopback_stream(&framing_network, 2).await;
        // Lifetime: sends one deliberately oversized response prefix and is then joined.
        let framing_task = tokio::spawn(async move {
            read_framed_bounded(&mut framing_server, 32).await.unwrap();
            framing_server
                .write_all(&33_u32.to_be_bytes())
                .await
                .unwrap();
        });
        let framing_error = framing_network
            .send_rpc::<_, Value>(
                2,
                &json!(0),
                RPCTypes::AppendEntries,
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(matches!(framing_error, RPCError::Network(_)));
        framing_task.await.unwrap();
        assert!(
            framing_network
                .peers
                .get(&2)
                .unwrap()
                .stream
                .lock()
                .await
                .is_none()
        );

        let timeout_network = test_network(1_024, &[1, 2]);
        let mut timeout_server = attach_loopback_stream(&timeout_network, 2).await;
        // Lifetime: waits cancellation-safely after reading one request and is explicitly aborted.
        let timeout_task = tokio::spawn(async move {
            read_framed_bounded(&mut timeout_server, 1_024)
                .await
                .unwrap();
            std::future::pending::<()>().await;
        });
        let timeout_error = timeout_network
            .send_rpc::<_, Value>(2, &json!(0), RPCTypes::Vote, Duration::ZERO)
            .await
            .unwrap_err();
        assert!(matches!(timeout_error, RPCError::Timeout(_)));
        assert!(
            timeout_network
                .peers
                .get(&2)
                .unwrap()
                .stream
                .lock()
                .await
                .is_none()
        );
        timeout_task.abort();
        let _ = timeout_task.await;
    }

    #[tokio::test]
    async fn cancelling_an_inflight_rpc_drops_its_desynchronized_stream() {
        let network = test_network(1_024, &[1, 2]);
        let mut server = attach_loopback_stream(&network, 2).await;
        let (request_seen_tx, request_seen_rx) = oneshot::channel();

        // Lifetime: holds the response until the client RPC is cancelled, then is aborted below.
        let server_task = tokio::spawn(async move {
            read_framed_bounded(&mut server, 1_024).await.unwrap();
            request_seen_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        let rpc_network = network.clone();
        let rpc_task = tokio::spawn(async move {
            rpc_network
                .send_rpc::<_, Value>(
                    2,
                    &json!({"term": 1}),
                    RPCTypes::Vote,
                    Duration::from_secs(30),
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), request_seen_rx)
            .await
            .expect("server must receive the request")
            .expect("request signal sender must remain alive");
        rpc_task.abort();
        let _ = rpc_task.await;

        assert!(
            network.peers.get(&2).unwrap().stream.lock().await.is_none(),
            "a cancelled RPC must not leave its unread response on a reusable stream"
        );
        server_task.abort();
        let _ = server_task.await;
    }
}
