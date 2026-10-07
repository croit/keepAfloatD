//! Forward `client_write` to the Raft leader via a small TCP JSON channel on `client_submit_*`.
//!
//! Wire format
//! -----------
//! 4-byte BE length, then a signed release envelope. The leader replies with a signed
//! [`SubmitResponse`]. Submit frames have a fixed 4 KiB cap, independent of
//! the larger configurable Raft/snapshot frame limit.
//!
//! Authentication
//! --------------
//! A loaded config must carry a `cluster_secret` (enforced in [`Config`] validation), shared by
//! every node. A bounded mutual HMAC handshake proves key possession before any envelope.
//! Records bind both runtime boots and the fresh channel transcript. Payloads remain plaintext.
//!
//! Timeout
//! -------
//! Every submit attempt is bounded by `cfg.submit_timeout_ms`, including local leader
//! `raft.client_write(...)` calls and follower->leader forwarding. This keeps an isolated leader
//! from delaying release notification indefinitely after it loses quorum.

use crate::config::{Config, canonical_socket_addr};
use crate::connection_admission::{
    ConnectionAdmission, UnauthenticatedConnection, connect_from_advertised,
};
use crate::listener::{AcceptBackoff, ConnectionListener};
use crate::raft::{KafRaft, KafRequest};
use crate::warning_limit::{WarningLimiter, warn_limited};
use anyhow::Context;
use openraft::error::{ClientWriteError, RaftError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(test)]
use tokio::net::TcpListener;
use tokio::net::TcpStream;

#[cfg(test)]
mod admission_tests;
mod proof;
#[cfg(test)]
mod release_tests;

#[cfg(test)]
pub(crate) mod test_wire {
    use super::*;
    use crate::raft::TypeConfig;
    use crate::raft::admission::{Genesis, ReplicaId, SignedAdmission};
    use openraft::alias::LogIdOf;

    pub(crate) fn release(
        cfg: &Config,
        replica: ReplicaId,
        binding: [u8; 32],
        genesis: Genesis,
        request: KafRequest,
    ) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(&SignedAdmission::sign(
            cfg.cluster_secret.as_deref(),
            proof::RELEASE_REQUEST_ROLE,
            replica,
            replica,
            binding,
            proof::ReleaseEnvelope { genesis, request },
        )?)?)
    }

    pub(crate) fn acknowledgement(
        cfg: &Config,
        replica: ReplicaId,
        binding: [u8; 32],
        bytes: &[u8],
    ) -> anyhow::Result<LogIdOf<TypeConfig>> {
        let response: SignedAdmission<SubmitResponse> = serde_json::from_slice(bytes)?;
        response.verify(
            cfg.cluster_secret.as_deref(),
            proof::RELEASE_RESPONSE_ROLE,
            replica,
            replica,
            binding,
        )?;
        response
            .payload
            .accepted()?
            .log_id
            .context("release acknowledgement lacks a committed log ID")
    }

    pub(crate) fn acknowledge_request(
        cfg: &Config,
        replica: ReplicaId,
        binding: [u8; 32],
        bytes: &[u8],
        log_id: LogIdOf<TypeConfig>,
    ) -> anyhow::Result<Vec<u8>> {
        let request: SignedAdmission<proof::ReleaseEnvelope> = serde_json::from_slice(bytes)?;
        request.verify(
            cfg.cluster_secret.as_deref(),
            proof::RELEASE_REQUEST_ROLE,
            replica,
            replica,
            binding,
        )?;
        anyhow::ensure!(
            matches!(request.payload.request, KafRequest::VipReleased { .. }),
            "expected release request"
        );
        Ok(serde_json::to_vec(&SignedAdmission::sign(
            cfg.cluster_secret.as_deref(),
            proof::RELEASE_RESPONSE_ROLE,
            replica,
            replica,
            binding,
            SubmitResponse::from_applied(log_id, crate::raft::types::KafResponse::Ok),
        )?)?)
    }
}

/// Submit messages contain only one small control request or response. Keep their allocation cap
/// independent of the much larger Raft snapshot limit.
const SUBMIT_FRAME_MAX_BYTES: u32 = 4 * 1024;

/// Bounds slow readers after a response is ready.
const SUBMIT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) const SUBMIT_UNAUTHENTICATED_CONNECTION_LIMIT: usize = 32;
const SUBMIT_AUTHENTICATED_CONNECTION_LIMIT: usize = 64;

/// Request envelope sent only after mutual authentication.
#[derive(Debug, Serialize, Deserialize)]
struct SubmitEnvelope {
    request: KafRequest,
}

/// Result of one submit attempt as returned by the leader.
#[derive(Debug, Serialize, Deserialize)]
struct SubmitResponse {
    ok: bool,
    #[serde(default)]
    message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    log_id: Option<openraft::alias::LogIdOf<crate::raft::TypeConfig>>,
}

impl SubmitResponse {
    fn from_applied(
        log_id: openraft::alias::LogIdOf<crate::raft::TypeConfig>,
        response: crate::raft::types::KafResponse,
    ) -> Self {
        match response {
            crate::raft::types::KafResponse::Ok => Self {
                ok: true,
                message: String::new(),
                log_id: Some(log_id),
            },
            crate::raft::types::KafResponse::Rejected(message) => Self {
                ok: false,
                message,
                log_id: None,
            },
        }
    }
    fn accepted(self) -> anyhow::Result<Self> {
        anyhow::ensure!(self.ok, "leader rejected: {}", self.message);
        Ok(self)
    }
}

/// Submit a client request through Raft (local `client_write` if leader, otherwise forward to
/// the leader).
///
/// Only releases from the current admitted voter boot are accepted. Health publication uses
/// the runtime driver's nonce-bound progress protocol.
pub async fn submit_request(
    cfg: &Arc<Config>,
    raft: &KafRaft,
    req: KafRequest,
) -> anyhow::Result<()> {
    let started = tokio::time::Instant::now();
    let budget = Duration::from_millis(cfg.submit_timeout_ms);
    anyhow::ensure!(
        matches!(req, KafRequest::VipReleased { .. }),
        "ordinary submission accepts only admitted VIP releases"
    );
    anyhow::ensure!(
        req.node_id() == Some(cfg.node_id),
        "release request belongs to another physical member"
    );
    let authority = proof::release_authority(cfg, raft).await?;
    let deadline = authority.session.check()?.min(started + budget);
    tokio::time::timeout_at(deadline, async {
        authority.check(*raft.node_id()).await?;
        let response = match raft.client_write(req.clone()).await {
            Ok(response) => SubmitResponse::from_applied(response.log_id, response.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                let leader = match forward.leader_id {
                    Some(leader) => leader,
                    None => raft
                        .current_leader()
                        .await
                        .context("release submit has no leader")?,
                };
                proof::forward_release(cfg, &authority, leader, req).await?
            }
            Err(error) => anyhow::bail!("release raft write: {error:?}"),
        };
        authority.check(*raft.node_id()).await?;
        response.accepted()?;
        Ok(())
    })
    .await
    .context("release submission exceeded its admission or submit deadline")?
}
async fn read_framed_bounded<R>(stream: &mut R, max_frame_bytes: u32) -> anyhow::Result<Vec<u8>>
where
    R: AsyncReadExt + Unpin,
{
    let (bytes, ()) = crate::frame::read(stream, max_frame_bytes, |_| Ok(()))
        .await
        .map_err(|error| match error {
            crate::frame::ReadError::Io(error) => anyhow::Error::from(error),
            crate::frame::ReadError::TooLarge {
                length,
                max_frame_bytes,
            } => anyhow::anyhow!(
                "submit frame {} bytes exceeds max_frame_bytes {}",
                length,
                max_frame_bytes
            ),
        })?;
    Ok(bytes)
}

async fn write_submit_frame_with_timeout<W>(
    stream: &mut W,
    body: &[u8],
    timeout: Duration,
) -> anyhow::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    anyhow::ensure!(
        body.len() as u64 <= SUBMIT_FRAME_MAX_BYTES as u64,
        "submit response {} bytes exceeds submit frame limit {}",
        body.len(),
        SUBMIT_FRAME_MAX_BYTES
    );
    let len = u32::try_from(body.len()).context("submit response length exceeds u32")?;
    tokio::time::timeout(timeout, async {
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(body).await
    })
    .await
    .context("submit response write timed out")??;
    Ok(())
}

async fn read_submit_frame_with_timeout<R>(
    stream: &mut R,
    timeout: Duration,
) -> anyhow::Result<Vec<u8>>
where
    R: AsyncReadExt + Unpin,
{
    tokio::time::timeout(timeout, read_framed_bounded(stream, SUBMIT_FRAME_MAX_BYTES))
        .await
        .context("submit read timed out")?
}

/// Listen for follower-forwarded writes, authenticate them and apply on the leader.
pub async fn run_submit_server(
    cfg: Arc<Config>,
    raft: KafRaft,
    source: crate::listener::ListenerSource,
) -> anyhow::Result<()> {
    let addr: std::net::SocketAddr = cfg
        .client_submit_listen
        .parse()
        .with_context(|| format!("parse client_submit_listen {}", cfg.client_submit_listen))?;
    let listener = source.bind(addr).await?;
    tracing::info!("client_submit listening on {}", addr);
    serve_submit_listener(listener, cfg, raft).await
}

async fn serve_submit_listener(
    listener: impl ConnectionListener,
    cfg: Arc<Config>,
    raft: KafRaft,
) -> anyhow::Result<()> {
    let mut connections = tokio::task::JoinSet::new();
    let mut accept_backoff = AcceptBackoff::default();
    let warnings = WarningLimiter::default();
    let admission = ConnectionAdmission::new(
        SUBMIT_UNAUTHENTICATED_CONNECTION_LIMIT,
        SUBMIT_AUTHENTICATED_CONNECTION_LIMIT,
    );
    let source_admission = crate::admission::ConnectionAdmission::new(
        cfg.peers
            .iter()
            .filter_map(|peer| peer.client_submit_address.parse().ok()),
    );

    loop {
        tokio::select! {
            accepted = accept_backoff.accept(&listener) => {
                let (mut sock, from) = match accepted {
                    Ok(connection) => connection,
                    Err(error) => {
                        accept_backoff.failed("submit", error);
                        continue;
                    }
                };
                if !is_known_submit_source(&cfg, from.ip()) {
                    warn_limited!(warnings, "submit from unknown source {} rejected before admission", from);
                    continue;
                }
                let Some(unauthenticated) = admission.try_begin() else {
                    warn_limited!(warnings, "submit from {} rejected: unauthenticated connection limit reached", from);
                    continue;
                };
                let Some(source_unauthenticated) = source_admission.try_begin(from.ip()) else {
                    warn_limited!(warnings, "submit from {} rejected: source unauthenticated connection limit reached", from);
                    continue;
                };
                let raft = raft.clone();
                let cfg = cfg.clone();
                let warnings = warnings.clone();
                // Lifetime: owned by `connections`; server cancellation aborts every slow client.
                connections.spawn(async move {
                    if let Err(e) = handle_one_submit(
                        &mut sock,
                        from,
                        &raft,
                        &cfg,
                        unauthenticated,
                        source_unauthenticated,
                    ).await {
                        warn_limited!(warnings, "submit from {} failed: {}", from, e);
                    }
                });
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined {
                    return Err(anyhow::anyhow!("submit connection task failed: {error}"));
                }
            }
        }
    }
}

/// Upper bound on how long an accepted submit connection may take to deliver its framed request.
/// Without this an unauthenticated peer can open many connections, send a max-length prefix and
/// then stall, pinning a task and up to `max_frame_bytes` each indefinitely (slowloris / memory
/// exhaustion). Kept independent of `submit_timeout_ms` (which bounds the raft write, not the read).
const SUBMIT_READ_TIMEOUT: Duration = Duration::from_secs(5);

async fn handle_one_submit(
    sock: &mut TcpStream,
    from: std::net::SocketAddr,
    raft: &KafRaft,
    cfg: &Config,
    unauthenticated: UnauthenticatedConnection,
    source_unauthenticated: UnauthenticatedConnection,
) -> anyhow::Result<()> {
    let local = *raft.node_id();
    let authenticated = crate::auth::server_bound(
        sock,
        crate::auth::Peer::for_replica(local, None, true),
        cfg.cluster_secret.as_deref(),
        crate::auth::Listener::Submit,
    )
    .await?;
    let peer = authenticated
        .peer
        .replica()
        .context("submit peer has no exact boot identity")?;
    let buf = read_submit_frame_with_timeout(sock, SUBMIT_READ_TIMEOUT).await?;
    let signed: crate::raft::admission::SignedAdmission<proof::ReleaseEnvelope> =
        serde_json::from_slice(&buf)?;
    signed.verify(
        cfg.cluster_secret.as_deref(),
        proof::RELEASE_REQUEST_ROLE,
        peer,
        local,
        authenticated.binding,
    )?;
    let req = validate_and_extract(
        cfg,
        from.ip(),
        SubmitEnvelope {
            request: signed.payload.request,
        },
    )
    .map_err(anyhow::Error::msg)?;
    anyhow::ensure!(
        req.node_id() == Some(peer.physical_id),
        "submit node_id differs from authenticated peer"
    );
    let _authenticated = unauthenticated
        .try_authenticate()
        .map_err(|_| anyhow::anyhow!("authenticated submit connection limit reached"))?;
    let _source_authenticated = source_unauthenticated
        .try_authenticate()
        .map_err(|_| anyhow::anyhow!("source authenticated submit connection limit reached"))?;
    let started = tokio::time::Instant::now();
    let authority = proof::release_authority(cfg, raft).await?;
    anyhow::ensure!(
        signed.payload.genesis == authority.session.context().genesis,
        "submit genesis differs from active admission"
    );
    let deadline = authority
        .session
        .check()?
        .min(started + Duration::from_millis(cfg.submit_timeout_ms));
    tokio::time::timeout_at(deadline, async {
        authority.check(peer).await?;
        let response = match raft.client_write(req).await {
            Ok(response) => SubmitResponse::from_applied(response.log_id, response.data),
            Err(error) => SubmitResponse {
                ok: false,
                message: format!("{error:?}"),
                log_id: None,
            },
        };
        authority.check(peer).await?;
        let signed = crate::raft::admission::SignedAdmission::sign(
            cfg.cluster_secret.as_deref(),
            proof::RELEASE_RESPONSE_ROLE,
            local,
            peer,
            authenticated.binding,
            response,
        )?;
        let body = serde_json::to_vec(&signed)?;
        write_submit_frame_with_timeout(sock, &body, SUBMIT_WRITE_TIMEOUT).await
    })
    .await
    .context("submit exceeded its admission or write deadline")?
}
fn is_known_submit_source(cfg: &Config, source: std::net::IpAddr) -> bool {
    let source = canonical_socket_addr(std::net::SocketAddr::new(source, 0)).ip();
    cfg.peers.iter().any(|peer| {
        peer.client_submit_address
            .parse::<std::net::SocketAddr>()
            .is_ok_and(|address| canonical_socket_addr(address).ip() == source)
    })
}

/// Validate envelope (node_id membership + sender binding) and extract the inner request,
/// or return a human-readable rejection message.
///
/// `from_ip` is the connection's source address. A node may submit only for **itself**: the source
/// IP must match the advertised address of the claimed `node_id`. The caller independently
/// verifies the signed channel record and the exact admitted voter boot, including same-IP peers.
fn validate_and_extract(
    cfg: &Config,
    from_ip: std::net::IpAddr,
    env: SubmitEnvelope,
) -> Result<KafRequest, String> {
    let Some(node_id) = env.request.node_id() else {
        return Err("cluster-scoped request not accepted over client submit".into());
    };
    if !matches!(env.request, KafRequest::VipReleased { .. }) {
        return Err("ordinary submission accepts only admitted VIP releases".into());
    }
    let Some(peer) = cfg.peers.iter().find(|p| p.id == node_id) else {
        return Err(format!("node_id {} not in peers", node_id));
    };
    let expected_ip = peer
        .client_submit_address
        .parse::<std::net::SocketAddr>()
        .map(canonical_socket_addr)
        .map(|address| address.ip())
        .map_err(|e| {
            format!(
                "peer {} has an unparseable client_submit_address: {e}",
                node_id
            )
        })?;
    let from_ip = canonical_socket_addr(std::net::SocketAddr::new(from_ip, 0)).ip();
    if from_ip != expected_ip {
        return Err(format!(
            "submit for node_id {node_id} came from {from_ip}, but its advertised address is {expected_ip}"
        ));
    }
    Ok(env.request)
}

#[cfg(test)]
mod tests {
    use super::{
        SUBMIT_FRAME_MAX_BYTES, SubmitEnvelope, read_framed_bounded,
        read_submit_frame_with_timeout, validate_and_extract, write_submit_frame_with_timeout,
    };
    use std::net::{IpAddr, Ipv4Addr};

    const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
    use crate::config::Config;
    use crate::raft::KafRequest;
    use crate::raft::types::test_replica;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn unadmitted_local_release_cannot_be_acknowledged() {
        use std::sync::Arc;
        use std::time::Duration;
        let mut config = cfg_with(Some("unadmitted-release-test-secret"));
        config.peers.truncate(1);
        let cfg = Arc::new(config);
        let (log, machine, state) = crate::raft::store::new_store(Arc::new(vec![]), 3, true, 0);
        let network = crate::raft::RaftNetworkImpl::new(
            cfg.clone(),
            state,
            crate::raft::network::testing::controller(&cfg),
        )
        .unwrap();
        let replica = test_replica(1);
        let raft = crate::raft::KafRaft::new(
            replica,
            Arc::new(openraft::Config::default()),
            network,
            log,
            machine,
        )
        .await
        .unwrap();
        raft.initialize(std::collections::BTreeMap::from([(
            replica,
            openraft::BasicNode::new("127.0.0.1:1"),
        )]))
        .await
        .unwrap();
        raft.wait(Some(Duration::from_secs(1)))
            .current_leader(replica, "local unit leader")
            .await
            .unwrap();
        let result = super::submit_request(
            &cfg,
            &raft,
            KafRequest::VipReleased {
                node_id: 1,
                vip: "192.0.2.1".parse().unwrap(),
                generation: 7,
            },
        )
        .await;
        raft.shutdown().await.unwrap();
        assert!(
            result.is_err(),
            "unadmitted release was acknowledged: {result:?}"
        );
    }

    async fn without_clock_advance<F: std::future::Future>(future: F) -> F::Output {
        let watchdog = std::time::Instant::now();
        tokio::pin!(future);
        // Keep paused time fixed while loopback I/O waits for the OS.
        loop {
            assert!(
                watchdog.elapsed() < std::time::Duration::from_secs(5),
                "paused-clock fixture exceeded its wall-clock watchdog"
            );
            tokio::select! {
                biased;
                result = &mut future => return result,
                _ = tokio::task::yield_now() => {}
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ordinary_release_forwards_to_admitted_leader_and_bounds_errors() {
        without_clock_advance(ordinary_release_fixture()).await;
    }

    async fn ordinary_release_fixture() {
        use std::sync::Arc;
        use std::time::Duration;
        let listener = TcpListener::bind("127.248.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let mut cfg = cfg_with(Some("ordinary-release-secret"));
        cfg.client_submit_listen = "127.248.0.2:1".into();
        cfg.peers[0].client_submit_address = cfg.client_submit_listen.clone();
        cfg.peers[1].client_submit_address = address;
        cfg.submit_timeout_ms = 50;
        let cfg = Arc::new(cfg);
        let (log, machine, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
        let controller = crate::raft::network::testing::controller(&cfg);
        let authorization = controller.authorize_raft(test_replica(1)).unwrap();
        let genesis = authorization.session.context().genesis.clone();
        {
            let mut state = state.write().await;
            state.genesis = Some(genesis.clone());
            state.last_membership = authorization.committed_membership;
            state.bind_admission(authorization.session).unwrap();
        }
        let network = crate::raft::RaftNetworkImpl::new(cfg.clone(), state, controller).unwrap();
        let raft = crate::raft::KafRaft::new(
            test_replica(1),
            Arc::new(openraft::Config::default()),
            network,
            log,
            machine,
        )
        .await
        .unwrap();
        raft.append_entries(openraft::raft::AppendEntriesRequest {
            vote: openraft::Vote::new_committed(1, test_replica(2)),
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: None,
        })
        .await
        .unwrap();
        let request = KafRequest::VipReleased {
            node_id: 1,
            vip: "192.0.2.1".parse().unwrap(),
            generation: 7,
        };
        let expected = request.clone();
        let (blocked_request, received_request) = tokio::sync::oneshot::channel();
        // The server lives only for these requests; dropping the final socket ends its blocked reply.
        let server = tokio::spawn(async move {
            use crate::raft::admission::SignedAdmission;
            let mut blocked_request = Some(blocked_request);
            for response in [Some(true), Some(false), None] {
                let (mut socket, source) = listener.accept().await.unwrap();
                let authenticated = crate::auth::server_bound(
                    &mut socket,
                    crate::auth::Peer::for_replica(test_replica(2), None, true),
                    Some("ordinary-release-secret"),
                    crate::auth::Listener::Submit,
                )
                .await
                .unwrap();
                assert_eq!(source.ip(), "127.248.0.2".parse::<IpAddr>().unwrap());
                let body = read_framed_bounded(&mut socket, SUBMIT_FRAME_MAX_BYTES)
                    .await
                    .unwrap();
                let envelope: SignedAdmission<super::proof::ReleaseEnvelope> =
                    serde_json::from_slice(&body).unwrap();
                envelope
                    .verify(
                        Some("ordinary-release-secret"),
                        super::proof::RELEASE_REQUEST_ROLE,
                        test_replica(1),
                        test_replica(2),
                        authenticated.binding,
                    )
                    .unwrap();
                assert_eq!(envelope.payload.request, expected);
                assert_eq!(envelope.payload.genesis, genesis);
                if let Some(ok) = response {
                    let response = SignedAdmission::sign(
                        Some("ordinary-release-secret"),
                        super::proof::RELEASE_RESPONSE_ROLE,
                        test_replica(2),
                        test_replica(1),
                        authenticated.binding,
                        super::SubmitResponse {
                            ok,
                            message: "admitted rejection".into(),
                            log_id: ok.then(|| {
                                openraft::testing::log_id::<crate::raft::TypeConfig>(
                                    1,
                                    test_replica(2),
                                    7,
                                )
                            }),
                        },
                    )
                    .unwrap();
                    let response = serde_json::to_vec(&response).unwrap();
                    write_submit_frame_with_timeout(&mut socket, &response, Duration::from_secs(1))
                        .await
                        .unwrap();
                } else {
                    blocked_request.take().unwrap().send(()).unwrap();
                    let mut discarded = Vec::new();
                    tokio::io::AsyncReadExt::read_to_end(&mut socket, &mut discarded)
                        .await
                        .unwrap();
                }
            }
        });
        let started = tokio::time::Instant::now();
        super::submit_request(&cfg, &raft, request.clone())
            .await
            .unwrap();
        let rejection = super::submit_request(&cfg, &raft, request.clone())
            .await
            .unwrap_err();
        assert!(
            rejection.to_string().contains("admitted rejection"),
            "{rejection}"
        );
        assert_eq!(started.elapsed(), Duration::ZERO);
        let pending = super::submit_request(&cfg, &raft, request);
        tokio::pin!(pending);
        tokio::select! {
            result = &mut pending => panic!("release completed before the server barrier: {result:?}"),
            received = received_request => received.unwrap(),
        }
        assert_eq!(started.elapsed(), Duration::ZERO);
        tokio::time::advance(Duration::from_millis(49)).await;
        assert!(futures::poll!(&mut pending).is_pending());
        tokio::time::advance(Duration::from_millis(1)).await;
        let timeout = pending.await.unwrap_err();
        assert!(timeout.to_string().contains("deadline"), "{timeout}");
        assert_eq!(started.elapsed(), Duration::from_millis(50));
        server.await.unwrap();
        raft.shutdown().await.unwrap();
    }

    #[test]
    fn retired_health_proof_wire_request_is_rejected() {
        let wire = br#"{"request":{"HealthUpdateWithProof":{"node_id":1,"healthy":true}}}"#;
        #[derive(serde::Deserialize)]
        struct LegacyEnvelope {
            request: KafRequest,
        }
        assert!(serde_json::from_slice::<LegacyEnvelope>(wire).is_err());
        let legacy = serde_json::from_str::<LegacyEnvelope>(
            r#"{"request":{"HealthUpdate":{"node_id":1,"healthy":false}}}"#,
        )
        .unwrap();
        assert_eq!(
            legacy.request,
            KafRequest::HealthUpdate {
                node_id: 1,
                healthy: false
            }
        );
        assert!(serde_json::from_slice::<SubmitEnvelope>(wire).is_err());
    }

    #[test]
    fn successful_proof_response_preserves_the_committed_log_id() {
        let id = openraft::testing::log_id::<crate::raft::TypeConfig>(3, test_replica(1), 42);
        let wire = serde_json::json!({"ok":true,"message":"","log_id":id});
        let response: super::SubmitResponse = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(response).unwrap()["log_id"],
            wire["log_id"]
        );
    }

    #[test]
    fn rejected_state_machine_response_cannot_be_acknowledged_as_success() {
        let log = openraft::testing::log_id::<crate::raft::TypeConfig>(1, test_replica(1), 9);
        let response = super::SubmitResponse::from_applied(
            log,
            crate::raft::types::KafResponse::Rejected("admission expired".into()),
        );
        assert!(!response.ok);
        assert_eq!(response.log_id, None);
        assert!(
            response
                .accepted()
                .unwrap_err()
                .to_string()
                .contains("admission expired")
        );
        let accepted =
            super::SubmitResponse::from_applied(log, crate::raft::types::KafResponse::Ok);
        assert_eq!(accepted.accepted().unwrap().log_id, Some(log));
    }

    #[tokio::test]
    async fn submit_rejects_frames_above_main_four_kib_limit() {
        let mut encoded = &4097_u32.to_be_bytes()[..];
        let error = read_framed_bounded(&mut encoded, SUBMIT_FRAME_MAX_BYTES)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("exceeds max_frame_bytes"),
            "{error}"
        );
    }

    pub(super) fn cfg_with(secret: Option<&str>) -> Config {
        let yaml = format!(
            r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
  - id: 2
    raft_address: "127.0.0.1:3"
    client_submit_address: "127.0.0.1:4"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
{secret_line}
"#,
            secret_line = match secret {
                Some(s) => format!("cluster_secret: \"{s}\""),
                None => String::new(),
            }
        );
        let mut c: Config = serde_yaml::from_str(&yaml).unwrap();
        // Reuse the public load path indirectly: construct + manual normalize via a fresh roundtrip.
        // The validator runs in `Config::load_path`; we replicate the bare minimum here by calling
        // `serde_yaml`-roundtrip-friendly setup. Since we only test the secret/membership branches,
        // nothing else matters for these tests.
        c.cluster_secret = secret.map(str::to_owned);
        c
    }

    fn env(node_id: u64) -> SubmitEnvelope {
        SubmitEnvelope {
            request: KafRequest::VipReleased {
                node_id,
                vip: "192.0.2.1".parse().unwrap(),
                generation: 7,
            },
        }
    }

    #[test]
    fn unknown_node_id_rejected() {
        let c = cfg_with(None);
        assert!(validate_and_extract(&c, LOOPBACK, env(99)).is_err());
    }

    #[test]
    fn submit_from_wrong_source_ip_is_rejected() {
        // A node may submit only for itself: even with the right secret and a known node_id, a
        // source IP that does not match the claimed node's advertised address is rejected, so a
        // different-IP peer cannot claim another node's release source address.
        let c = cfg_with(Some("alpha"));
        let wrong: IpAddr = "10.0.0.99".parse().unwrap();
        assert!(validate_and_extract(&c, wrong, env(1)).is_err());
        // From node 1's own (loopback) address the same request is accepted.
        assert!(validate_and_extract(&c, LOOPBACK, env(1)).is_ok());
    }

    #[test]
    fn submit_source_matching_canonicalizes_ipv4_mapped_addresses() {
        let mut cfg = cfg_with(Some("secret"));
        cfg.peers[0].client_submit_address = "[::ffff:127.0.0.1]:2".into();

        assert!(super::is_known_submit_source(&cfg, LOOPBACK));
        let env = SubmitEnvelope {
            request: KafRequest::VipReleased {
                node_id: 1,
                vip: "192.0.2.1".parse().unwrap(),
                generation: 7,
            },
        };
        assert!(validate_and_extract(&cfg, LOOPBACK, env).is_ok());
    }

    #[test]
    fn release_request_roundtrips_through_validation() {
        let c = cfg_with(Some("alpha"));
        let env = SubmitEnvelope {
            request: KafRequest::VipReleased {
                node_id: 2,
                vip: "10.0.0.10".parse().unwrap(),
                generation: 7,
            },
        };
        assert!(validate_and_extract(&c, LOOPBACK, env).is_ok());
    }

    #[tokio::test]
    async fn read_framed_bounded_rejects_oversize_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            // Claim a frame far larger than the cap; the reader must refuse on the prefix alone.
            s.write_all(&1_000_000_u32.to_be_bytes()).await.unwrap();
            s.flush().await.unwrap();
        });
        let (mut server, _) = listener.accept().await.unwrap();
        let err = read_framed_bounded(&mut server, 64).await.unwrap_err();
        assert!(err.to_string().contains("max_frame_bytes"));
        client.await.unwrap();
    }

    #[tokio::test]
    async fn submit_frame_reader_preserves_partial_eof_and_oversize_errors() {
        let frame = [0, 0, 0, 3, b'a', b'b', b'c'];
        for end in 0..frame.len() {
            let error = read_framed_bounded(&mut &frame[..end], 3)
                .await
                .unwrap_err();
            assert_eq!(
                error.downcast_ref::<std::io::Error>().unwrap().kind(),
                std::io::ErrorKind::UnexpectedEof,
                "prefix {end}",
            );
        }
        let mut oversized = &[0, 0, 0, 4, b'a'][..];
        let error = read_framed_bounded(&mut oversized, 3).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "submit frame 4 bytes exceeds max_frame_bytes 3"
        );
        assert!(error.downcast_ref::<std::io::Error>().is_none());
        assert_eq!(oversized, b"a");
    }

    #[tokio::test]
    async fn submit_frame_reader_accepts_zero_and_exact_cap_without_reading_ahead() {
        for payload in [b"".as_slice(), b"abc".as_slice()] {
            let mut bytes = (payload.len() as u32).to_be_bytes().to_vec();
            bytes.extend_from_slice(payload);
            bytes.extend_from_slice(b"next");
            let mut reader = bytes.as_slice();
            assert_eq!(read_framed_bounded(&mut reader, 3).await.unwrap(), payload);
            assert_eq!(reader, b"next");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn submit_frame_reader_uses_one_deadline_for_prefix_and_body() {
        use std::time::Duration;
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer.write_all(&[0]).await.unwrap();
        let read = read_submit_frame_with_timeout(&mut reader, Duration::from_secs(5));
        tokio::pin!(read);
        assert!(futures::poll!(&mut read).is_pending());
        tokio::time::advance(Duration::from_secs(3)).await;
        writer.write_all(&[0, 0, 3, b'a']).await.unwrap();
        assert!(futures::poll!(&mut read).is_pending());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(read.await.unwrap_err().to_string(), "submit read timed out");
    }

    #[tokio::test]
    async fn read_framed_bounded_roundtrips_a_valid_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            let body = b"payload";
            s.write_all(&(body.len() as u32).to_be_bytes())
                .await
                .unwrap();
            s.write_all(body).await.unwrap();
            s.flush().await.unwrap();
        });
        let (mut server, _) = listener.accept().await.unwrap();
        let got = read_framed_bounded(&mut server, 1024).await.unwrap();
        assert_eq!(got, b"payload");
        client.await.unwrap();
    }

    #[tokio::test]
    async fn submit_frame_cap_is_independent_of_the_raft_snapshot_cap() {
        let (mut client, mut server) = tokio::io::duplex(16);
        let claimed = SUBMIT_FRAME_MAX_BYTES + 1;
        let writer = tokio::spawn(async move {
            client.write_all(&claimed.to_be_bytes()).await.unwrap();
        });

        let err = read_framed_bounded(&mut server, SUBMIT_FRAME_MAX_BYTES)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("max_frame_bytes"), "{err}");
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn submit_response_write_is_bounded() {
        let (mut blocked_writer, _reader) = tokio::io::duplex(1);
        let err = write_submit_frame_with_timeout(
            &mut blocked_writer,
            b"response body larger than the duplex capacity",
            std::time::Duration::from_millis(20),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("timed out"), "{err}");
    }

    #[tokio::test]
    async fn stalled_submit_prefix_and_body_are_bounded() {
        let (_silent_client, mut prefix_reader) = tokio::io::duplex(16);
        let prefix_error = read_submit_frame_with_timeout(
            &mut prefix_reader,
            std::time::Duration::from_millis(20),
        )
        .await
        .unwrap_err();
        assert!(prefix_error.to_string().contains("timed out"));

        let (mut partial_client, mut body_reader) = tokio::io::duplex(16);
        partial_client
            .write_all(&1_u32.to_be_bytes())
            .await
            .unwrap();
        let body_error =
            read_submit_frame_with_timeout(&mut body_reader, std::time::Duration::from_millis(20))
                .await
                .unwrap_err();
        assert!(body_error.to_string().contains("timed out"));
    }

    #[test]
    fn cluster_scoped_request_rejected_over_submit_even_with_valid_secret() {
        // A cluster incarnation is committed only by the leader's epoch minter, never forwarded by
        // a client. Even with the correct secret it must be refused on the submit channel so a peer
        // cannot inject a foreign incarnation.
        let c = cfg_with(Some("alpha"));
        let env = SubmitEnvelope {
            request: KafRequest::ClusterFormed {
                cluster_id: 9,
                failover_semantics: crate::raft::FailoverSemantics::Legacy,
                config_identity_enforced: false,
            },
        };
        let err = validate_and_extract(&c, LOOPBACK, env).unwrap_err();
        assert!(err.contains("cluster-scoped"), "unexpected error: {err}");
    }
}
