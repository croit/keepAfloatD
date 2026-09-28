//! Forward `client_write` to the Raft leader via a small TCP JSON channel on `client_submit_*`.
//!
//! Wire format
//! -----------
//! 4-byte BE length, then a JSON [`SubmitEnvelope`]. The leader replies with a 4-byte BE length
//! followed by a JSON [`SubmitResponse`]. Submit frames have a fixed 4 KiB cap, independent of
//! the larger configurable Raft/snapshot frame limit.
//!
//! Authentication
//! --------------
//! A loaded config must carry a `cluster_secret` (enforced in [`Config`] validation), shared by
//! every node. Each envelope carries that secret, and the leader accepts a submit only when the
//! envelope's `secret` matches its own. This keeps a stranger on the same broadcast domain from
//! spoofing `node_id` health reports as long as the secret stays out of their reach; use a
//! dedicated, unguessable token and keep `/etc/keepafloatd/config.yaml` readable only by the
//! service account.
//!
//! Timeout
//! -------
//! Every submit attempt is bounded by `cfg.submit_timeout_ms`, including local leader
//! `raft.client_write(...)` calls and follower->leader forwarding. This keeps an isolated leader
//! from freezing its health-publication loop forever after it loses quorum.

use crate::config::{Config, canonical_socket_addr};
use crate::connection_admission::{
    ConnectionAdmission, UnauthenticatedConnection, connect_from_advertised,
};
use crate::raft::{KafRaft, KafRequest};
use anyhow::Context;
use openraft::error::{ClientWriteError, RaftError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod proof;
pub(crate) use proof::submit_health;

/// Submit messages contain only one small control request or response. Keep their allocation cap
/// independent of the much larger Raft snapshot limit.
const SUBMIT_FRAME_MAX_BYTES: u32 = 4 * 1024;

/// Bounds slow readers after a response is ready.
const SUBMIT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) const SUBMIT_UNAUTHENTICATED_CONNECTION_LIMIT: usize = 32;
const SUBMIT_AUTHENTICATED_CONNECTION_LIMIT: usize = 64;

/// Outer envelope on the submit channel: carries the request plus a shared-secret token used by
/// the leader to authenticate the sender.
#[derive(Debug, Serialize, Deserialize)]
struct SubmitEnvelope {
    /// Cluster shared secret. `None` means "not provided"; the leader rejects this when its own
    /// `cluster_secret` is set.
    #[serde(default)]
    secret: Option<String>,
    #[serde(deserialize_with = "proof::decode_request")]
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

/// Submit a client request through Raft (local `client_write` if leader, otherwise forward to
/// the leader).
///
/// Every request variant carries a `node_id` that must be listed in [`Config::peers`]. Followers
/// validate before invoking Raft; if not leader they forward over TCP to the leader, which
/// re-validates `node_id` membership and the cluster secret before committing.
pub async fn submit_request(
    cfg: &Arc<Config>,
    raft: &KafRaft,
    req: KafRequest,
) -> anyhow::Result<()> {
    let node_id = req
        .node_id()
        .context("cluster-scoped request cannot be submitted via submit_request")?;
    anyhow::ensure!(
        cfg.peers.iter().any(|p| p.id == node_id),
        "request node_id {} not in peers",
        node_id
    );
    let timeout = Duration::from_millis(cfg.submit_timeout_ms);
    match tokio::time::timeout(timeout, raft.client_write(req.clone()))
        .await
        .with_context(|| {
            format!(
                "local raft client_write timed out after {}ms",
                cfg.submit_timeout_ms
            )
        })? {
        Ok(_) => Ok(()),
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ftl))) => {
            let leader = match ftl.leader_id {
                Some(id) => id,
                None => raft
                    .current_leader()
                    .await
                    .context("forward to leader but no leader id")?,
            };
            let addr = cfg
                .get_peer(leader)
                .map(|p| p.client_submit_address.clone())
                .context("leader not in peers")?;
            let timeout = Duration::from_millis(cfg.submit_timeout_ms);
            let envelope = SubmitEnvelope {
                secret: cfg.cluster_secret.clone(),
                request: req,
            };
            tokio::time::timeout(
                timeout,
                forward_client_submit(&cfg.client_submit_listen, &addr, &envelope),
            )
            .await
            .with_context(|| format!("forward to leader {leader} ({addr}) timed out"))?
            .map(|_| ())
        }
        Err(e) => Err(anyhow::anyhow!("raft client_write: {:?}", e)),
    }
}

async fn forward_client_submit<T: Serialize>(
    local_address: &str,
    addr: &str,
    envelope: &T,
) -> anyhow::Result<SubmitResponse> {
    let mut stream = connect_from_advertised(local_address, addr)
        .await
        .with_context(|| format!("connect client_submit {}", addr))?;
    let body = serde_json::to_vec(envelope)?;
    anyhow::ensure!(
        body.len() as u64 <= SUBMIT_FRAME_MAX_BYTES as u64,
        "submit request {} bytes exceeds submit frame limit {}",
        body.len(),
        SUBMIT_FRAME_MAX_BYTES
    );
    let len = body.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&body).await?;

    let resp_buf = read_framed_bounded(&mut stream, SUBMIT_FRAME_MAX_BYTES).await?;
    let r: SubmitResponse = serde_json::from_slice(&resp_buf)?;
    anyhow::ensure!(r.ok, "leader rejected: {}", r.message);
    Ok(r)
}

async fn read_framed_bounded<R>(stream: &mut R, max_frame_bytes: u32) -> anyhow::Result<Vec<u8>>
where
    R: AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let n = u32::from_be_bytes(len_buf);
    anyhow::ensure!(
        n <= max_frame_bytes,
        "submit frame {} bytes exceeds max_frame_bytes {}",
        n,
        max_frame_bytes
    );
    let mut buf = vec![0u8; n as usize];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
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
pub async fn run_submit_server(cfg: Arc<Config>, raft: KafRaft) -> anyhow::Result<()> {
    let addr: std::net::SocketAddr = cfg
        .client_submit_listen
        .parse()
        .with_context(|| format!("parse client_submit_listen {}", cfg.client_submit_listen))?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("client_submit listening on {}", addr);
    let mut connections = tokio::task::JoinSet::new();
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
            accepted = listener.accept() => {
                let (mut sock, from) = accepted.context("accept submit connection")?;
                if !is_known_submit_source(&cfg, from.ip()) {
                    tracing::warn!("submit from unknown source {} rejected before admission", from);
                    continue;
                }
                let Some(unauthenticated) = admission.try_begin() else {
                    tracing::warn!("submit from {} rejected: unauthenticated connection limit reached", from);
                    continue;
                };
                let Some(source_permit) = source_admission.try_acquire(from.ip()) else {
                    tracing::warn!("submit from {} rejected: source connection limit reached", from);
                    continue;
                };
                let raft = raft.clone();
                let cfg = cfg.clone();
                // Lifetime: owned by `connections`; server cancellation aborts every slow client.
                connections.spawn(async move {
                    let _source_permit = source_permit;
                    if let Err(e) = handle_one_submit(
                        &mut sock,
                        from,
                        &raft,
                        &cfg,
                        unauthenticated,
                    ).await {
                        tracing::warn!("submit from {} failed: {}", from, e);
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
) -> anyhow::Result<()> {
    let buf = read_submit_frame_with_timeout(sock, SUBMIT_READ_TIMEOUT).await?;
    let env: SubmitEnvelope = serde_json::from_slice(&buf)?;
    let req = validate_and_extract(cfg, from.ip(), env).map_err(anyhow::Error::msg)?;
    let _authenticated = unauthenticated
        .try_authenticate()
        .map_err(|_| anyhow::anyhow!("authenticated submit connection limit reached"))?;

    let resp = match tokio::time::timeout(
        Duration::from_millis(cfg.submit_timeout_ms),
        raft.client_write(req),
    )
    .await
    {
        Ok(Ok(response)) => SubmitResponse {
            ok: true,
            message: String::new(),
            log_id: Some(response.log_id),
        },
        Ok(Err(e)) => SubmitResponse {
            ok: false,
            message: format!("{:?}", e),
            log_id: None,
        },
        Err(_) => SubmitResponse {
            ok: false,
            message: format!(
                "local raft client_write timed out after {}ms",
                cfg.submit_timeout_ms
            ),
            log_id: None,
        },
    };

    let body = serde_json::to_vec(&resp)?;
    write_submit_frame_with_timeout(sock, &body, SUBMIT_WRITE_TIMEOUT).await
}

fn is_known_submit_source(cfg: &Config, source: std::net::IpAddr) -> bool {
    let source = canonical_socket_addr(std::net::SocketAddr::new(source, 0)).ip();
    cfg.peers.iter().any(|peer| {
        peer.client_submit_address
            .parse::<std::net::SocketAddr>()
            .is_ok_and(|address| canonical_socket_addr(address).ip() == source)
    })
}

/// Validate envelope (secret + node_id membership + sender binding) and extract the inner request,
/// or return a human-readable rejection message.
///
/// `from_ip` is the connection's source address. A node may submit only for **itself**: the source
/// IP must match the advertised address of the claimed `node_id`. The shared secret proves the
/// sender is *in* the cluster; this binding distinguishes nodes that advertise distinct IPs.
/// Same-host development clusters whose peers share one IP rely on the shared secret alone.
fn validate_and_extract(
    cfg: &Config,
    from_ip: std::net::IpAddr,
    env: SubmitEnvelope,
) -> Result<KafRequest, String> {
    if let Some(local) = cfg.cluster_secret.as_deref() {
        match env.secret.as_deref() {
            Some(s) if crate::secret::secrets_equal(s.as_bytes(), local.as_bytes()) => {}
            _ => return Err("cluster_secret mismatch".into()),
        }
    }
    let Some(node_id) = env.request.node_id() else {
        return Err("cluster-scoped request not accepted over client submit".into());
    };
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
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn ordinary_release_forwards_to_legacy_leader_and_bounds_errors() {
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
        let network = crate::raft::RaftNetworkImpl::new(cfg.clone(), state).unwrap();
        let raft = crate::raft::KafRaft::new(
            1,
            Arc::new(openraft::Config::default()),
            network,
            log,
            machine,
        )
        .await
        .unwrap();
        raft.append_entries(openraft::raft::AppendEntriesRequest {
            vote: openraft::Vote::new_committed(1, 2),
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
        // The server lives only for these requests; dropping the final socket ends its blocked reply.
        let server = tokio::spawn(async move {
            #[derive(serde::Deserialize)]
            struct LegacyEnvelope {
                secret: Option<String>,
                request: KafRequest,
            }
            for response in [Some(true), Some(false), None] {
                let (mut socket, source) = listener.accept().await.unwrap();
                assert_eq!(source.ip(), "127.248.0.2".parse::<IpAddr>().unwrap());
                let body = read_framed_bounded(&mut socket, SUBMIT_FRAME_MAX_BYTES)
                    .await
                    .unwrap();
                let envelope: LegacyEnvelope = serde_json::from_slice(&body).unwrap();
                assert_eq!(envelope.secret.as_deref(), Some("ordinary-release-secret"));
                assert_eq!(envelope.request, expected);
                if let Some(ok) = response {
                    let response = serde_json::to_vec(
                        &serde_json::json!({"ok":ok,"message":"legacy rejection"}),
                    )
                    .unwrap();
                    write_submit_frame_with_timeout(&mut socket, &response, Duration::from_secs(1))
                        .await
                        .unwrap();
                } else {
                    let mut discarded = Vec::new();
                    tokio::io::AsyncReadExt::read_to_end(&mut socket, &mut discarded)
                        .await
                        .unwrap();
                }
            }
        });
        super::submit_request(&cfg, &raft, request.clone())
            .await
            .unwrap();
        let rejection = super::submit_request(&cfg, &raft, request.clone())
            .await
            .unwrap_err();
        assert!(
            rejection.to_string().contains("legacy rejection"),
            "{rejection}"
        );
        let timeout = super::submit_request(&cfg, &raft, request)
            .await
            .unwrap_err();
        assert!(
            timeout.to_string().contains("forward to leader 2"),
            "{timeout}"
        );
        assert!(timeout.to_string().contains("timed out"), "{timeout}");
        server.await.unwrap();
        raft.shutdown().await.unwrap();
    }

    #[test]
    fn proof_health_request_is_submit_only_and_legacy_readers_reject_it() {
        let wire =
            br#"{"secret":null,"request":{"HealthUpdateWithProof":{"node_id":1,"healthy":true}}}"#;
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
        let envelope: SubmitEnvelope = serde_json::from_slice(wire)
            .expect("new server must decode the submit-only proof request");
        assert_eq!(
            validate_and_extract(&cfg_with(None), LOOPBACK, envelope).unwrap(),
            KafRequest::HealthUpdate {
                node_id: 1,
                healthy: true
            }
        );
    }

    #[test]
    fn successful_proof_response_preserves_the_committed_log_id() {
        let id = openraft::testing::log_id::<crate::raft::TypeConfig>(3, 1, 42);
        let wire = serde_json::json!({"ok":true,"message":"","log_id":id});
        let response: super::SubmitResponse = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(response).unwrap()["log_id"],
            wire["log_id"]
        );
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

    fn cfg_with(secret: Option<&str>) -> Config {
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

    fn env(secret: Option<&str>, node_id: u64) -> SubmitEnvelope {
        SubmitEnvelope {
            secret: secret.map(str::to_owned),
            request: KafRequest::HealthUpdate {
                node_id,
                healthy: true,
            },
        }
    }

    #[test]
    fn no_local_secret_accepts_anything() {
        let c = cfg_with(None);
        assert!(validate_and_extract(&c, LOOPBACK, env(None, 1)).is_ok());
        assert!(validate_and_extract(&c, LOOPBACK, env(Some("x"), 1)).is_ok());
    }

    #[test]
    fn local_secret_requires_match() {
        let c = cfg_with(Some("alpha"));
        assert!(validate_and_extract(&c, LOOPBACK, env(Some("alpha"), 1)).is_ok());
        assert!(validate_and_extract(&c, LOOPBACK, env(Some("beta"), 1)).is_err());
        assert!(validate_and_extract(&c, LOOPBACK, env(None, 1)).is_err());
    }

    #[test]
    fn unknown_node_id_rejected() {
        let c = cfg_with(None);
        assert!(validate_and_extract(&c, LOOPBACK, env(None, 99)).is_err());
    }

    #[test]
    fn submit_from_wrong_source_ip_is_rejected() {
        // A node may submit only for itself: even with the right secret and a known node_id, a
        // source IP that does not match the claimed node's advertised address is rejected, so a
        // different-IP peer cannot forge health for another node. Same-IP development peers share
        // an identity boundary and rely on the cluster secret instead.
        let c = cfg_with(Some("alpha"));
        let wrong: IpAddr = "10.0.0.99".parse().unwrap();
        assert!(validate_and_extract(&c, wrong, env(Some("alpha"), 1)).is_err());
        // From node 1's own (loopback) address the same request is accepted.
        assert!(validate_and_extract(&c, LOOPBACK, env(Some("alpha"), 1)).is_ok());
    }

    #[test]
    fn submit_source_matching_canonicalizes_ipv4_mapped_addresses() {
        let mut cfg = cfg_with(Some("secret"));
        cfg.peers[0].client_submit_address = "[::ffff:127.0.0.1]:2".into();

        assert!(super::is_known_submit_source(&cfg, LOOPBACK));
        let env = SubmitEnvelope {
            secret: Some("secret".into()),
            request: KafRequest::HealthUpdate {
                node_id: 1,
                healthy: true,
            },
        };
        assert!(validate_and_extract(&cfg, LOOPBACK, env).is_ok());
    }

    #[test]
    fn release_request_roundtrips_through_validation() {
        let c = cfg_with(Some("alpha"));
        let env = SubmitEnvelope {
            secret: Some("alpha".into()),
            request: KafRequest::VipReleased {
                node_id: 2,
                vip: "10.0.0.10".parse().unwrap(),
                generation: 7,
            },
        };
        assert!(validate_and_extract(&c, LOOPBACK, env).is_ok());
    }

    #[test]
    fn non_ascii_secret_requires_exact_match() {
        let c = cfg_with(Some("pä$$-✓-wörd"));
        assert!(validate_and_extract(&c, LOOPBACK, env(Some("pä$$-✓-wörd"), 1)).is_ok());
        assert!(validate_and_extract(&c, LOOPBACK, env(Some("pa$$-x-word"), 1)).is_err());
        assert!(validate_and_extract(&c, LOOPBACK, env(None, 1)).is_err());
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
            secret: Some("alpha".into()),
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
