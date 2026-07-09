//! Forward `client_write` to the Raft leader via a small TCP JSON channel on `client_submit_*`.
//!
//! Wire format
//! -----------
//! 4-byte BE length, then a JSON [`SubmitEnvelope`]. The leader replies with a 4-byte BE length
//! followed by a JSON [`SubmitResponse`]. The receiver refuses any frame larger than
//! [`Config::max_frame_bytes`] before allocating.
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

use crate::config::Config;
use crate::raft::{KafRaft, KafRequest};
use anyhow::Context;
use openraft::error::{ClientWriteError, RaftError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Outer envelope on the submit channel: carries the request plus a shared-secret token used by
/// the leader to authenticate the sender.
#[derive(Debug, Serialize, Deserialize)]
struct SubmitEnvelope {
    /// Cluster shared secret. `None` means "not provided"; the leader rejects this when its own
    /// `cluster_secret` is set.
    #[serde(default)]
    secret: Option<String>,
    request: KafRequest,
}

/// Result of one submit attempt as returned by the leader.
#[derive(Debug, Serialize, Deserialize)]
struct SubmitResponse {
    ok: bool,
    #[serde(default)]
    message: String,
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
                forward_client_submit(&addr, &envelope, cfg.max_frame_bytes),
            )
            .await
            .with_context(|| format!("forward to leader {leader} ({addr}) timed out"))?
        }
        Err(e) => Err(anyhow::anyhow!("raft client_write: {:?}", e)),
    }
}

async fn forward_client_submit(
    addr: &str,
    envelope: &SubmitEnvelope,
    max_frame_bytes: u32,
) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect client_submit {}", addr))?;
    let body = serde_json::to_vec(envelope)?;
    anyhow::ensure!(
        body.len() as u64 <= max_frame_bytes as u64,
        "submit request {} bytes exceeds max_frame_bytes {}",
        body.len(),
        max_frame_bytes
    );
    let len = body.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&body).await?;

    let resp_buf = read_framed_bounded(&mut stream, max_frame_bytes).await?;
    let r: SubmitResponse = serde_json::from_slice(&resp_buf)?;
    anyhow::ensure!(r.ok, "leader rejected: {}", r.message);
    Ok(())
}

async fn read_framed_bounded(
    stream: &mut TcpStream,
    max_frame_bytes: u32,
) -> anyhow::Result<Vec<u8>> {
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

/// Listen for follower-forwarded writes, authenticate them and apply on the leader.
pub async fn run_submit_server(cfg: Arc<Config>, raft: KafRaft) -> anyhow::Result<()> {
    let addr: std::net::SocketAddr = cfg
        .client_submit_listen
        .parse()
        .with_context(|| format!("parse client_submit_listen {}", cfg.client_submit_listen))?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("client_submit listening on {}", addr);

    loop {
        let (mut sock, from) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::error!("submit accept: {}", e);
                continue;
            }
        };
        let raft = raft.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_one_submit(&mut sock, from, &raft, &cfg).await {
                tracing::warn!("submit from {} failed: {}", from, e);
            }
        });
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
) -> anyhow::Result<()> {
    let buf = tokio::time::timeout(
        SUBMIT_READ_TIMEOUT,
        read_framed_bounded(sock, cfg.max_frame_bytes),
    )
    .await
    .context("submit read timed out")??;
    let env: SubmitEnvelope = serde_json::from_slice(&buf)?;

    let resp = match validate_and_extract(cfg, from.ip(), env) {
        Ok(req) => match tokio::time::timeout(
            Duration::from_millis(cfg.submit_timeout_ms),
            raft.client_write(req),
        )
        .await
        {
            Ok(Ok(_)) => SubmitResponse {
                ok: true,
                message: String::new(),
            },
            Ok(Err(e)) => SubmitResponse {
                ok: false,
                message: format!("{:?}", e),
            },
            Err(_) => SubmitResponse {
                ok: false,
                message: format!(
                    "local raft client_write timed out after {}ms",
                    cfg.submit_timeout_ms
                ),
            },
        },
        Err(message) => SubmitResponse { ok: false, message },
    };

    let body = serde_json::to_vec(&resp)?;
    if body.len() as u64 > cfg.max_frame_bytes as u64 {
        return Err(anyhow::anyhow!(
            "submit response {} bytes exceeds max_frame_bytes {}",
            body.len(),
            cfg.max_frame_bytes
        ));
    }
    let len = body.len() as u32;
    sock.write_all(&len.to_be_bytes()).await?;
    sock.write_all(&body).await?;
    Ok(())
}

/// Validate envelope (secret + node_id membership + sender binding) and extract the inner
/// request, or return a human-readable rejection message. `from_ip` is the connection's source
/// address; a node may submit only for itself, so it must match the claimed node's advertised
/// address — a secret-holding but compromised node cannot then forge state for a different node.
fn validate_and_extract(
    cfg: &Config,
    from_ip: std::net::IpAddr,
    env: SubmitEnvelope,
) -> Result<KafRequest, String> {
    if let Some(local) = cfg.cluster_secret.as_deref() {
        match env.secret.as_deref() {
            Some(s) if s == local => {}
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
        .map(|a| a.ip())
        .map_err(|e| {
            format!(
                "peer {} has an unparseable client_submit_address: {e}",
                node_id
            )
        })?;
    if from_ip != expected_ip {
        return Err(format!(
            "submit for node_id {node_id} came from {from_ip}, but its advertised address is {expected_ip}"
        ));
    }
    Ok(env.request)
}

#[cfg(test)]
mod tests {
    use super::{SubmitEnvelope, read_framed_bounded, validate_and_extract};
    use std::net::{IpAddr, Ipv4Addr};

    const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
    use crate::config::Config;
    use crate::raft::KafRequest;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

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
        // compromised peer cannot forge health for a different node.
        let c = cfg_with(Some("alpha"));
        let wrong: IpAddr = "10.0.0.99".parse().unwrap();
        assert!(validate_and_extract(&c, wrong, env(Some("alpha"), 1)).is_err());
        // From node 1's own (loopback) address the same request is accepted.
        assert!(validate_and_extract(&c, LOOPBACK, env(Some("alpha"), 1)).is_ok());
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

    #[test]
    fn cluster_scoped_request_rejected_over_submit_even_with_valid_secret() {
        // A cluster incarnation is committed only by the leader's epoch minter, never forwarded by
        // a client. Even with the correct secret it must be refused on the submit channel so a peer
        // cannot inject a foreign incarnation.
        let c = cfg_with(Some("alpha"));
        let env = SubmitEnvelope {
            secret: Some("alpha".into()),
            request: KafRequest::ClusterFormed { cluster_id: 9 },
        };
        let err = validate_and_extract(&c, LOOPBACK, env).unwrap_err();
        assert!(err.contains("cluster-scoped"), "unexpected error: {err}");
    }
}
