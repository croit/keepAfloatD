//! TCP transport between Raft peers (length-prefixed JSON), reworked for production safety.
//!
//! Wire format
//! -----------
//! 1. Mutual HMAC-SHA256 authentication through [`crate::auth`]. Fresh nonces and both
//!    endpoint IDs, roles, version, listener, epoch and capabilities are transcript-bound.
//!    No shared secret is transmitted. See docs/authentication.md for the byte layout.
//! 2. Frames (both directions): 4-byte BE `u32` length followed by JSON body. Receivers refuse
//!    frames larger than their cap before allocation: [`Config::max_frame_bytes`] for Raft frames,
//!    a fixed 64 KiB cap for status/preflight frames and a fixed 4 KiB cap for submit envelopes.
//!    Raft/status bodies carry one operation tag; replies must match the requested operation.
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
//! An RPC retries a disconnected cached stream once on a fresh, authenticated connection. The
//! same deadline covers lock acquisition, reconnect and both exchanges. Other errors drop the
//! stream and leave recovery to the background task, which also detects idle peer closes. Each
//! [`RaftNetworkV2`](openraft::network::v2::RaftNetworkV2) call honours
//! [`openraft::network::RPCOption::hard_ttl`] via [`tokio::time::timeout`]; this is what turns a
//! half-open connection into a prompt `Timeout` error instead of an unbounded `read_exact`.
//! Cancellation during an in-flight exchange also drops the stream so its unread response cannot
//! be consumed by a later RPC. Status preflight advertises that behavior.
//! A cancellation-unsafe reader reuses only responses dispatched and accepted by one TCP
//! write inside the timeout safety margin; a later response is dropped and the connection closes.

mod admission_rpc;
pub mod authorization;
mod client;
mod inbound;
mod reconnect;
mod request;
mod server;
mod status;
mod wire;

#[cfg(test)]
mod authorization_tests;

#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
mod deadline_tests;

#[cfg(test)]
mod reconnect_tests;

#[cfg(test)]
mod pre_vote_tests;

#[cfg(test)]
mod sender_tests;

#[cfg(test)]
mod transport_tests;

pub(super) use status::probe_peer_status;

use super::tasks::{CleanExit, SupervisedTask, spawn_supervised_task, stop_supervised_tasks};
use super::types::{FailoverSemantics, TypeConfig};
use super::{KafRaft, KafStorageState};
use crate::config::{ClusterConfigFingerprint, Config};
use anyhow::Context;
use authorization::{AdmissionController, ReplicaId};
use openraft::alias::{SnapshotMetaOf, VoteOf};
use openraft::error::{NetworkError, RPCError, Timeout, Unreachable};
use openraft::network::RPCTypes;
use reconnect::ReconnectDecision;
#[cfg(test)]
use reconnect::idle_stream_is_usable;
use request::{Operation, decode_payload, encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
#[cfg(test)]
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock, mpsc};
use wire::{
    InFlightRpcStream, connect_with_handshake, connection_requires_config_identity,
    connection_requires_upgrade, read_framed_bounded, write_framed,
};

/// Per-peer outbound channel. The `address` is fixed at construction time; only the inner
/// `TcpStream` is replaced on (re)connect. Locking is exclusive but per-peer.
struct PeerLink {
    address: String,
    stream: Mutex<Option<TcpStream>>,
    advertises_v2: AtomicBool,
    advertises_config_identity: AtomicBool,
    remote_replica: std::sync::RwLock<Option<ReplicaId>>,
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

fn is_disconnect(error: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    matches!(
        error.kind(),
        ErrorKind::UnexpectedEof
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
            | ErrorKind::NotConnected
            | ErrorKind::WriteZero
    )
}

async fn connect_peer_stream(
    link: &PeerLink,
    cfg: &Config,
    state_ref: &RwLock<KafStorageState>,
    fingerprint: ClusterConfigFingerprint,
    local_replica: ReplicaId,
) -> anyhow::Result<TcpStream> {
    // Advertise the committed incarnation at connect time; the peer fences stale survivors.
    let (epoch, advertises_v2, identity_enforced) = {
        let state = state_ref.read().await;
        (
            state.cluster_epoch,
            state.failover_semantics == FailoverSemantics::V2,
            state.config_identity_enforced,
        )
    };
    let (stream, peer_identity, replica) = connect_with_handshake(
        &link.address,
        cfg,
        epoch,
        advertises_v2,
        fingerprint,
        identity_enforced,
        local_replica,
    )
    .await?;
    let current = state_ref.read().await;
    anyhow::ensure!(
        !connection_requires_upgrade(current.failover_semantics, advertises_v2)
            && !connection_requires_config_identity(
                current.config_identity_enforced,
                peer_identity
            ),
        "raft connection policy changed during handshake"
    );
    link.advertises_v2.store(advertises_v2, Ordering::SeqCst);
    link.advertises_config_identity
        .store(peer_identity, Ordering::SeqCst);
    *link
        .remote_replica
        .write()
        .map_err(|_| anyhow::anyhow!("peer identity lock poisoned"))? = Some(replica);
    Ok(stream)
}

#[derive(Clone)]
pub struct RaftNetworkImpl {
    admission: Arc<dyn AdmissionController>,
    config: Arc<Config>,
    /// Immutable map keyed by peer id (excludes self). Per-peer links are interior-mutable via
    /// their own `Mutex<Option<TcpStream>>`; the map shape never changes after construction.
    peers: Arc<HashMap<u64, Arc<PeerLink>>>,
    /// Shared state machine, read to learn this node's committed cluster incarnation for the
    /// handshake (outbound) and for fencing inbound Raft RPCs.
    state_ref: Arc<RwLock<KafStorageState>>,
    config_fingerprint: ClusterConfigFingerprint,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<tokio::sync::Notify>,
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
        admission: Arc<dyn AdmissionController>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            admission.local_replica().physical_id == config.node_id,
            "local replica does not match configured physical member"
        );
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
                    remote_replica: std::sync::RwLock::new(None),
                }),
            );
        }
        Ok(Self {
            admission,
            config,
            peers: Arc::new(peers),
            state_ref,
            config_fingerprint,
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
            started: Arc::new(AtomicBool::new(false)),
            tasks: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Bind `raft_listen`, accept peer handshakes, spawn per-peer reconnect loops, and serve
    /// inbound Raft RPCs on accepted-stream tasks.
    #[cfg(test)]
    pub async fn start(&self, raft: KafRaft) -> anyhow::Result<mpsc::UnboundedReceiver<String>> {
        self.start_with_listener(raft, crate::listener::ListenerSource::Configured)
            .await
    }

    pub(crate) async fn start_with_listener(
        &self,
        raft: KafRaft,
        source: crate::listener::ListenerSource,
    ) -> anyhow::Result<mpsc::UnboundedReceiver<String>> {
        claim_start(&self.started)?;
        let addr: std::net::SocketAddr = self
            .config
            .raft_listen
            .parse()
            .with_context(|| format!("parse raft_listen {}", self.config.raft_listen))?;
        let listener = source.bind(addr).await?;
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
                admission: self.admission.clone(),
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
            let local_replica = self.admission.local_replica();
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
                        let mut cached = link.stream.lock().await;
                        let (local_semantics, config_identity_enforced) = {
                            let state = state_ref.read().await;
                            (state.failover_semantics, state.config_identity_enforced)
                        };
                        match link.reconnect_decision(
                            cached.as_ref(),
                            local_semantics,
                            config_identity_enforced,
                        ) {
                            ReconnectDecision::Keep => {
                                drop(cached);
                                tokio::time::sleep(RECONNECT_PROBE_INTERVAL).await;
                                attempts = 0;
                                continue;
                            }
                            ReconnectDecision::Replace => {
                                tracing::debug!(
                                    peer_id,
                                    "reconnecting closed or outdated raft stream"
                                );
                                *cached = None;
                            }
                            ReconnectDecision::Connect => {}
                        }
                        let connection = connect_peer_stream(
                            &link,
                            &cfg,
                            &state_ref,
                            config_fingerprint,
                            local_replica,
                        )
                        .await;
                        match connection {
                            Ok(stream) => {
                                tracing::info!(
                                    "connected raft peer {} at {} (after {} attempts)",
                                    peer_id,
                                    link.address,
                                    attempts.saturating_add(1)
                                );
                                *cached = Some(stream);
                                attempts = 0;
                                safety_fence_visible = false;
                                drop(cached);
                                tokio::time::sleep(RECONNECT_RETRY_INTERVAL).await;
                            }
                            Err(e) => {
                                drop(cached);
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

    pub(super) async fn wait_until_shutdown(&self) {
        let notified = self.shutdown_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_shutting_down() {
            notified.await;
        }
    }

    pub(super) fn config_fingerprint(&self) -> ClusterConfigFingerprint {
        self.config_fingerprint
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
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
        target: ReplicaId,
        request: &Req,
        action: RPCTypes,
        hard_ttl: Duration,
    ) -> Result<Resp, RPCError<TypeConfig>> {
        let operation = Operation::try_from(action)
            .map_err(|message| RPCError::Network(NetworkError::from_string(message.to_owned())))?;
        let request_bytes =
            encode(operation, request).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
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

        let link = self
            .peers
            .get(&target.physical_id)
            .cloned()
            .ok_or_else(|| {
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

        let io = async {
            let mut guard = link.stream.lock().await;
            let state = self.state_ref.read().await;
            if connection_requires_upgrade(
                state.failover_semantics,
                link.advertises_v2.load(Ordering::SeqCst),
            ) || connection_requires_config_identity(
                state.config_identity_enforced,
                link.advertises_config_identity.load(Ordering::SeqCst),
            ) {
                *guard = None;
            }
            drop(state);
            let mut in_flight = InFlightRpcStream::new(guard);
            let mut retried = false;
            loop {
                let authorization = self
                    .admission
                    .authorize_raft_async(target)
                    .await
                    .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
                let validate = || -> anyhow::Result<()> {
                    let remote = *link
                        .remote_replica
                        .read()
                        .map_err(|_| anyhow::anyhow!("peer identity lock poisoned"))?;
                    anyhow::ensure!(remote == Some(target), "Raft destination boot changed");
                    authorization.check(self.admission.local_replica(), target)?;
                    authorization.validate_request(
                        self.admission.local_replica(),
                        &request::decode(&request_bytes)?,
                    )
                };
                validate().map_err(|error| {
                    RPCError::Network(NetworkError::from_string(error.to_string()))
                })?;
                let stream = in_flight.stream_mut().ok_or_else(|| {
                    RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "outbound stream not yet established",
                    )))
                })?;
                let exchange = async {
                    write_framed(stream, &request_bytes).await?;
                    read_framed_bounded(stream, max_frame).await
                };
                let deadline = authorization
                    .session
                    .check()
                    .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
                let exchange = tokio::time::timeout_at(deadline, exchange)
                    .await
                    .unwrap_or_else(|_| {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Raft admission expired during exchange",
                        ))
                    });
                match exchange {
                    Ok(body) => {
                        let authorization = self
                            .admission
                            .authorize_raft_async(target)
                            .await
                            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
                        authorization
                            .check(self.admission.local_replica(), target)
                            .and_then(|()| authorization.validate_response(&body))
                            .map_err(|error| {
                                RPCError::Network(NetworkError::from_string(error.to_string()))
                            })?;
                        let response = decode_payload(&body, operation).map_err(|e| {
                            RPCError::Network(NetworkError::from_string(e.to_string()))
                        })?;
                        in_flight.retain();
                        return Ok(response);
                    }
                    Err(error) if !retried && is_disconnect(&error) => {
                        retried = true;
                        tracing::debug!(%target, %error, "retrying raft rpc on a fresh connection");
                        in_flight.replace(None);
                        let stream = connect_peer_stream(
                            &link,
                            &self.config,
                            &self.state_ref,
                            self.config_fingerprint,
                            self.admission.local_replica(),
                        )
                        .await
                        .map_err(|e| RPCError::Network(NetworkError::from_string(e.to_string())))?;
                        in_flight.replace(Some(stream));
                    }
                    Err(error) => return Err(RPCError::Network(NetworkError::new(&error))),
                }
            }
        };
        tokio::time::timeout(effective_ttl, io)
            .await
            .unwrap_or_else(|_| {
                Err(RPCError::Timeout(Timeout {
                    action,
                    id: self.admission.local_replica(),
                    target,
                    timeout: started.elapsed(),
                }))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::client::RaftConnection;
    use super::request::{Operation, decode_payload, encode};
    use super::server::{is_known_raft_source, raft_source_matches_peer};
    use super::{RPCError, RPCTypes, RaftNetworkImpl, is_safety_fence_error, read_framed_bounded};
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
            cluster_secret_file: None,
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
        RaftNetworkImpl::new(
            config.clone(),
            state_ref,
            crate::raft::network::testing::controller(&config),
        )
        .unwrap()
    }

    pub(super) async fn attach_loopback_stream(
        network: &RaftNetworkImpl,
        target: u64,
    ) -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client, accepted) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(TcpStream::connect(address), listener.accept())
        })
        .await
        .expect("loopback connection must not stall");
        *network.peers.get(&target).unwrap().stream.lock().await = Some(client.unwrap());
        *network.peers[&target].remote_replica.write().unwrap() =
            Some(crate::raft::types::test_replica(target));
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
            .send_rpc::<_, Value>(
                crate::raft::types::test_replica(2),
                &RejectSerialize,
                RPCTypes::Vote,
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(matches!(serialization, RPCError::Network(_)));

        let unsupported = network
            .send_rpc::<_, Value>(
                crate::raft::types::test_replica(2),
                &super::testing::vote_request(),
                RPCTypes::TransferLeader,
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(unsupported.to_string().contains("no wire operation"));

        let unknown = network
            .send_rpc::<_, Value>(
                crate::raft::types::test_replica(99),
                &super::testing::vote_request(),
                RPCTypes::Vote,
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
        assert!(matches!(unknown, RPCError::Unreachable(_)));

        *network.peers[&2].remote_replica.write().unwrap() =
            Some(crate::raft::types::test_replica(2));
        let disconnected = network
            .send_rpc::<_, Value>(
                crate::raft::types::test_replica(2),
                &super::testing::vote_request(),
                RPCTypes::Vote,
                Duration::ZERO,
            )
            .await
            .unwrap_err();
        assert!(matches!(disconnected, RPCError::Unreachable(_)));

        let tiny_frame_network = test_network(4, &[1, 2]);
        let oversize = tiny_frame_network
            .send_rpc::<_, Value>(
                crate::raft::types::test_replica(2),
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
                decode_payload::<Value>(&request, Operation::Vote).unwrap(),
                serde_json::to_value(super::testing::vote_request()).unwrap()
            );
            let response = encode(Operation::Vote, &super::testing::vote_response(true)).unwrap();
            server
                .write_all(&(response.len() as u32).to_be_bytes())
                .await
                .unwrap();
            server.write_all(&response).await.unwrap();
        });

        let response: Value = network
            .send_rpc(
                crate::raft::types::test_replica(2),
                &super::testing::vote_request(),
                RPCTypes::Vote,
                Duration::ZERO,
            )
            .await
            .unwrap();
        assert_eq!(
            response,
            serde_json::to_value(super::testing::vote_response(true)).unwrap()
        );
        server_task.await.unwrap();
        assert!(network.peers.get(&2).unwrap().stream.lock().await.is_some());
    }

    #[tokio::test]
    async fn tagged_wire_wrong_response_operation_discards_stream() {
        let network = test_network(1_024, &[1, 2]);
        let mut server = attach_loopback_stream(&network, 2).await;
        // Lifetime: one bounded response exchange, joined before the test returns.
        let server_task = tokio::spawn(async move {
            read_framed_bounded(&mut server, 1_024).await.unwrap();
            super::write_framed(&mut server, br#"{"append_entries":true}"#)
                .await
                .unwrap();
        });
        let result = network
            .send_rpc::<_, Value>(
                crate::raft::types::test_replica(2),
                &super::testing::vote_request(),
                RPCTypes::Vote,
                Duration::from_secs(1),
            )
            .await;
        server_task.await.unwrap();
        assert!(result.is_err(), "wrong-operation response was accepted");
        assert!(network.peers[&2].stream.lock().await.is_none());
    }

    #[tokio::test]
    async fn tagged_wire_invalid_responses_never_retain_a_stream() {
        use openraft::raft::VoteResponse;
        let payload = r#"{"vote":{"leader_id":{"term":7,"node_id":2},"committed":false},"vote_granted":true,"last_log_id":null}"#;
        for bytes in [
            payload.to_owned(),
            format!("{{\"other\":{payload}}}"),
            format!("{{\"pre_vote\":{payload}}}"),
            format!("{{\"vote\":{payload},\"vote\":{payload}}}"),
            format!("{{\"vote\":{payload},\"pre_vote\":{payload}}}"),
            "{\"vote\":false}".to_owned(),
            "{\"vote\":{}}".to_owned(),
            "null".to_owned(),
        ] {
            let network = test_network(1_024, &[1, 2]);
            let mut server = attach_loopback_stream(&network, 2).await;
            let reply = bytes.clone();
            // Lifetime: one bounded response exchange, joined before the next case.
            let server_task = tokio::spawn(async move {
                read_framed_bounded(&mut server, 1_024).await.unwrap();
                super::write_framed(&mut server, reply.as_bytes())
                    .await
                    .unwrap();
            });
            let result = network
                .send_rpc::<_, VoteResponse<TypeConfig>>(
                    crate::raft::types::test_replica(2),
                    &super::testing::vote_request(),
                    RPCTypes::Vote,
                    Duration::from_secs(1),
                )
                .await;
            server_task.await.unwrap();
            assert!(result.is_err(), "accepted invalid response {bytes}");
            assert!(network.peers[&2].stream.lock().await.is_none());
        }
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
            .send_rpc::<_, Value>(
                crate::raft::types::test_replica(2),
                &super::testing::vote_request(),
                RPCTypes::Vote,
                Duration::from_secs(1),
            )
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
            target: crate::raft::types::test_replica(2),
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
                VoteOf::<TypeConfig>::new(1, crate::raft::types::test_replica(1)),
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
        let framing_network = test_network(1_024, &[1, 2]);
        let mut framing_server = attach_loopback_stream(&framing_network, 2).await;
        // Lifetime: sends one deliberately oversized response prefix and is then joined.
        let framing_task = tokio::spawn(async move {
            read_framed_bounded(&mut framing_server, 1_024)
                .await
                .unwrap();
            framing_server
                .write_all(&1_025_u32.to_be_bytes())
                .await
                .unwrap();
        });
        let framing_error = framing_network
            .send_rpc::<_, Value>(
                crate::raft::types::test_replica(2),
                &super::testing::vote_request(),
                RPCTypes::Vote,
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
            .send_rpc::<_, Value>(
                crate::raft::types::test_replica(2),
                &super::testing::vote_request(),
                RPCTypes::Vote,
                Duration::ZERO,
            )
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
                    crate::raft::types::test_replica(2),
                    &super::testing::vote_request(),
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
