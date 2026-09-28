//! Bounded framing and versioned peer-handshake primitives.

use super::super::probe::{
    ClusterStatusRequest, ClusterStatusResponse, config_identity_compatible,
};
use super::super::types::FailoverSemantics;
use crate::config::{ClusterConfigFingerprint, Config};
use crate::connection_admission::{FrameByteBudget, FrameBytePermit, connect_from_advertised};
use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Cluster-status messages are tiny JSON objects. Keep their allocation cap independent of the
/// much larger Raft/snapshot frame limit because a preflight peer is not yet authenticated as a
/// valid configuration member.
pub(super) const STATUS_FRAME_MAX_BYTES: u32 = 64 * 1024;
use tokio::sync::MutexGuard;

/// Bound TCP connect, handshake write, and the config-status preflight as one operation.
const OUTBOUND_PREFLIGHT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Length cap (bytes) on the cluster_secret field. Mirrors [`Config`] validation.
const MAX_SECRET_BYTES: u32 = 256;

/// Owns a peer stream slot for one request/response exchange.
///
/// Dropping the RPC future before it consumes the matching response would otherwise leave that
/// response queued for the next RPC. Clear the slot on every incomplete/error path, including
/// async cancellation; only a fully decoded response may retain the stream.
pub(super) struct InFlightRpcStream<'a> {
    slot: MutexGuard<'a, Option<TcpStream>>,
    retain: bool,
}

impl<'a> InFlightRpcStream<'a> {
    pub(super) fn new(slot: MutexGuard<'a, Option<TcpStream>>) -> Self {
        Self {
            slot,
            retain: false,
        }
    }

    pub(super) fn stream_mut(&mut self) -> Option<&mut TcpStream> {
        self.slot.as_mut()
    }

    pub(super) fn retain(&mut self) {
        self.retain = true;
    }
}

impl Drop for InFlightRpcStream<'_> {
    fn drop(&mut self) {
        if !self.retain {
            *self.slot = None;
        }
    }
}

/// Read a length-prefixed JSON frame, refusing oversized payloads before allocating.
pub(super) async fn read_framed_bounded<R: AsyncReadExt + Unpin>(
    stream: &mut R,
    max_frame_bytes: u32,
) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let n = u32::from_be_bytes(len_buf);
    let mut buf = bounded_frame_buffer(n, max_frame_bytes)?;
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

#[derive(Debug)]
pub(super) struct BudgetedFrame {
    bytes: Vec<u8>,
    _permit: FrameBytePermit,
}

impl AsRef<[u8]> for BudgetedFrame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// One deadline covers the complete frame, including a trickled length prefix (#26).
pub(super) async fn read_framed_bounded_with_timeout_and_budget<R: AsyncReadExt + Unpin>(
    stream: &mut R,
    max_frame_bytes: u32,
    frame_timeout: std::time::Duration,
    budget: &FrameByteBudget,
) -> std::io::Result<BudgetedFrame> {
    tokio::time::timeout(frame_timeout, async {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let n = u32::from_be_bytes(len_buf);
        if n > max_frame_bytes {
            return Err(frame_size_error(n, max_frame_bytes));
        }
        let permit = budget.try_reserve(n)?;
        let mut bytes = vec![0_u8; n as usize];
        stream.read_exact(&mut bytes).await?;
        Ok(BudgetedFrame {
            bytes,
            _permit: permit,
        })
    })
    .await
    .map_err(|_| frame_read_timeout_error(frame_timeout))?
}

fn bounded_frame_buffer(n: u32, max_frame_bytes: u32) -> std::io::Result<Vec<u8>> {
    if n > max_frame_bytes {
        return Err(frame_size_error(n, max_frame_bytes));
    }
    Ok(vec![0_u8; n as usize])
}

fn frame_size_error(n: u32, max_frame_bytes: u32) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("frame size {n} exceeds max_frame_bytes {max_frame_bytes}; refusing to allocate"),
    )
}

fn frame_read_timeout_error(frame_timeout: std::time::Duration) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("frame read timed out after {}ms", frame_timeout.as_millis()),
    )
}

/// Write a complete length-prefixed frame without an await point between its prefix and body.
///
/// Older peers can retain an RPC stream when OpenRaft cancels a request. If the
/// writer yields after the prefix, such a peer can consume that prefix before cancellation and
/// misread the delayed JSON body as the next frame length. Building one contiguous buffer keeps
/// the small response frames used by Raft on a single `write_all` path during rolling upgrades.
pub(super) async fn write_framed<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    payload: &[u8],
) -> std::io::Result<()> {
    let frame = encode_frame(payload)?;
    stream.write_all(&frame).await
}

fn encode_frame(payload: &[u8]) -> std::io::Result<Vec<u8>> {
    let length = u32::try_from(payload.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "frame payload exceeds the u32 wire length",
        )
    })?;
    let capacity = payload.len().checked_add(4).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "frame length overflow")
    })?;
    let mut frame = Vec::with_capacity(capacity);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn require_complete_frame_write(written: usize, expected: usize) -> std::io::Result<()> {
    if written == expected {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::WriteZero,
        format!(
            "single framed write accepted only {written} of {expected} bytes; dropping the stream"
        ),
    ))
}

/// Attempt a complete frame in one nonblocking TCP write or fail the connection.
///
/// A legacy reader can retain its stream if OpenRaft cancels an in-flight request. Continuing a
/// short write after an await would then let that reader mistake the delayed JSON body for the next
/// frame length. This helper never continues a short write: it returns an error so the inbound task
/// closes the stream. A short write may already have sent a prefix, but the connection close keeps
/// it from being reused. `WouldBlock` is safe to retry because it writes no bytes.
pub(super) async fn write_framed_tcp(stream: &TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let frame = encode_frame(payload)?;
    loop {
        stream.writable().await?;
        match stream.try_write(&frame) {
            Ok(written) => return require_complete_frame_write(written, frame.len()),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e),
        }
    }
}

/// Write the handshake: node id, secret, then the semantics/incarnation flag.
pub(super) async fn write_handshake<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    node_id: u64,
    secret: Option<&str>,
    epoch: Option<u128>,
    advertises_v2: bool,
) -> std::io::Result<()> {
    stream.write_all(&node_id.to_be_bytes()).await?;
    let secret_bytes = secret.map(str::as_bytes).unwrap_or(&[]);
    let len = secret_bytes.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    if !secret_bytes.is_empty() {
        stream.write_all(secret_bytes).await?;
    }
    match (epoch, advertises_v2) {
        (Some(e), false) => {
            stream.write_all(&[1]).await?;
            stream.write_all(&e.to_be_bytes()).await?;
        }
        (None, false) => stream.write_all(&[0]).await?,
        (None, true) => stream.write_all(&[2]).await?,
        (Some(e), true) => {
            stream.write_all(&[3]).await?;
            stream.write_all(&e.to_be_bytes()).await?;
        }
    }
    Ok(())
}

/// Read the inbound handshake and its V2-capability flag.
pub(super) async fn read_handshake<R: AsyncReadExt + Unpin>(
    stream: &mut R,
) -> std::io::Result<(u64, Option<String>, Option<u128>, bool)> {
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
    let (epoch, advertises_v2) = match flag[0] {
        0 => (None, false),
        1 | 3 => {
            let mut eb = [0u8; 16];
            stream.read_exact(&mut eb).await?;
            (Some(u128::from_be_bytes(eb)), flag[0] == 3)
        }
        2 => (None, true),
        other => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("handshake incarnation flag {other} invalid"),
            ));
        }
    };
    Ok((peer_id, secret, epoch, advertises_v2))
}

/// Allow blank peers to join, but fence two different concrete cluster incarnations.
pub(super) fn epochs_compatible(local: Option<u128>, peer: Option<u128>) -> bool {
    match (local, peer) {
        (Some(l), Some(p)) => l == p,
        _ => true,
    }
}

pub(super) fn peer_semantics_compatible(local: FailoverSemantics, peer_supports_v2: bool) -> bool {
    local == FailoverSemantics::Legacy || peer_supports_v2
}

pub(super) fn connection_requires_upgrade(local: FailoverSemantics, advertised_v2: bool) -> bool {
    local == FailoverSemantics::V2 && !advertised_v2
}

pub(super) fn connection_requires_config_identity(
    enforcement_active: bool,
    advertised_identity: bool,
) -> bool {
    enforcement_active && !advertised_identity
}

/// True when no secret is configured locally or the peer advertised the exact configured value.
/// The comparison is constant-time in the secret contents (see [`crate::secret::secrets_equal`]).
pub(super) fn secrets_match(local: Option<&str>, peer: Option<&str>) -> bool {
    match (local, peer) {
        (None, _) => true,
        (Some(local), Some(peer)) => {
            crate::secret::secrets_equal(local.as_bytes(), peer.as_bytes())
        }
        (Some(_), None) => false,
    }
}

pub(super) async fn connect_with_handshake(
    address: &str,
    cfg: &Config,
    epoch: Option<u128>,
    advertises_v2: bool,
    config_fingerprint: ClusterConfigFingerprint,
    config_identity_enforced: bool,
) -> anyhow::Result<(TcpStream, bool)> {
    let io = async {
        let mut stream = connect_from_advertised(&cfg.raft_listen, address)
            .await
            .with_context(|| format!("raft connect {address}"))?;
        write_handshake(
            &mut stream,
            cfg.node_id,
            cfg.cluster_secret.as_deref(),
            epoch,
            advertises_v2,
        )
        .await
        .context("raft handshake write")?;

        // A framed status preflight is understood by both legacy and current receivers. Current
        // peers exchange config identity before the first Raft frame; legacy responses omit it.
        let request = serde_json::to_vec(&ClusterStatusRequest {
            probe_from: cfg.node_id,
            config_fingerprint: Some(config_fingerprint),
            supports_cancellation_safe_rpc_v1: true,
        })?;
        anyhow::ensure!(
            request.len() as u64 <= u64::from(cfg.max_frame_bytes),
            "config preflight exceeds max_frame_bytes"
        );
        write_framed(&mut stream, &request).await?;
        let response = read_framed_bounded(&mut stream, STATUS_FRAME_MAX_BYTES).await?;
        let response: ClusterStatusResponse =
            serde_json::from_slice(&response).context("decode config preflight response")?;
        anyhow::ensure!(
            !response.reports_foreign_epoch(epoch),
            "cluster_epoch mismatch during config preflight"
        );
        anyhow::ensure!(
            config_identity_compatible(
                config_fingerprint,
                response.config_fingerprint,
                config_identity_enforced || response.config_identity_enforced,
            ),
            "cluster configuration identity mismatch (local {}, peer {})",
            config_fingerprint,
            response
                .config_fingerprint
                .map_or_else(|| "legacy/missing".to_owned(), |value| value.to_string())
        );
        let advertises_config_identity =
            response.supports_config_identity_v1 && response.config_fingerprint.is_some();
        Ok((stream, advertises_config_identity))
    };
    tokio::time::timeout(OUTBOUND_PREFLIGHT_TIMEOUT, io)
        .await
        .map_err(|_| anyhow::anyhow!("raft config preflight to {address} timed out"))?
}

/// Reopen a peer that a completed status exchange already identified as legacy. This is allowed
/// only while configuration identity enforcement is inactive; the caller re-checks that state
/// after this bounded connect and before installing the stream.
pub(super) async fn connect_with_legacy_handshake(
    address: &str,
    cfg: &Config,
    epoch: Option<u128>,
    advertises_v2: bool,
) -> anyhow::Result<TcpStream> {
    let io = async {
        let mut stream = connect_from_advertised(&cfg.raft_listen, address)
            .await
            .with_context(|| format!("raft connect {address}"))?;
        write_handshake(
            &mut stream,
            cfg.node_id,
            cfg.cluster_secret.as_deref(),
            epoch,
            advertises_v2,
        )
        .await
        .context("raft handshake write")?;
        Ok::<_, anyhow::Error>(stream)
    };
    tokio::time::timeout(OUTBOUND_PREFLIGHT_TIMEOUT, io)
        .await
        .map_err(|_| anyhow::anyhow!("raft legacy reconnect to {address} timed out"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DEFAULT_SUBMIT_TIMEOUT_MS, HealthConfig, PeerConfig, RaftTuneConfig};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::AsyncWrite;
    use tokio::net::TcpListener;

    #[derive(Default)]
    struct RecordingWriter {
        writes: Vec<Vec<u8>>,
    }

    impl AsyncWrite for RecordingWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.writes.push(buf.to_vec());
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Clone, Copy)]
    enum TestPeerIdentity {
        Same,
        Different,
        Missing,
    }

    async fn run_config_preflight(
        peer_identity: TestPeerIdentity,
        enforcement_active: bool,
    ) -> anyhow::Result<bool> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config = Config {
            node_id: 1,
            raft_listen: "127.0.0.1:17001".into(),
            client_submit_listen: "127.0.0.1:18001".into(),
            peers: vec![
                PeerConfig {
                    id: 1,
                    raft_address: "127.0.0.1:17001".into(),
                    client_submit_address: "127.0.0.1:18001".into(),
                },
                PeerConfig {
                    id: 2,
                    raft_address: address.to_string(),
                    client_submit_address: "127.0.0.1:18002".into(),
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
            cluster_secret: Some("wire-test-secret".into()),
            max_frame_bytes: 64 * 1024,
            submit_timeout_ms: DEFAULT_SUBMIT_TIMEOUT_MS,
            address_protocol: crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL,
            dry_run: true,
            notify: None,
            failover_delay_secs: 0,
            failback: true,
            failback_delay_secs: 0,
        };
        let local_fingerprint = config.cluster_config_fingerprint().unwrap();
        let response_fingerprint = match peer_identity {
            TestPeerIdentity::Same => Some(local_fingerprint),
            TestPeerIdentity::Different => Some(ClusterConfigFingerprint {
                version: local_fingerprint.version,
                digest: [0xff; 32],
            }),
            TestPeerIdentity::Missing => None,
        };

        // Lifetime: answers exactly one handshake preflight and is joined before return.
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_handshake(&mut stream).await.unwrap();
            let request = read_framed_bounded(&mut stream, 64 * 1024).await.unwrap();
            let request: ClusterStatusRequest = serde_json::from_slice(&request).unwrap();
            assert_eq!(request.config_fingerprint, Some(local_fingerprint));
            let response = serde_json::to_vec(&ClusterStatusResponse {
                config_fingerprint: response_fingerprint,
                supports_config_identity_v1: response_fingerprint.is_some(),
                ..ClusterStatusResponse::default()
            })
            .unwrap();
            stream
                .write_all(&(response.len() as u32).to_be_bytes())
                .await
                .unwrap();
            stream.write_all(&response).await.unwrap();
        });

        let result = connect_with_handshake(
            &address.to_string(),
            &config,
            None,
            true,
            local_fingerprint,
            enforcement_active,
        )
        .await
        .map(|(_, advertised)| advertised);
        server.await.unwrap();
        result
    }

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
        assert!(!epochs_compatible(Some(1), Some(2)));
        assert!(epochs_compatible(Some(1), Some(1)));
        assert!(epochs_compatible(None, Some(2)));
        assert!(epochs_compatible(Some(1), None));
        assert!(epochs_compatible(None, None));
    }

    #[tokio::test]
    async fn handshake_roundtrips_id_secret_epoch_and_semantics() {
        for (secret, epoch, advertises_v2) in [
            (None, None, false),
            (Some("s3cr3t"), None, true),
            (
                None,
                Some(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10_u128),
                false,
            ),
            (Some("s3cr3t"), Some(u128::MAX), true),
        ] {
            let mut buf = Vec::new();
            write_handshake(&mut buf, 42, secret, epoch, advertises_v2)
                .await
                .unwrap();
            let (id, got_secret, got_epoch, got_v2) =
                read_handshake(&mut std::io::Cursor::new(buf))
                    .await
                    .unwrap();
            assert_eq!(id, 42);
            assert_eq!(got_secret.as_deref(), secret);
            assert_eq!(got_epoch, epoch);
            assert_eq!(got_v2, advertises_v2);
        }
    }

    #[tokio::test]
    async fn handshake_rejects_oversize_secret_invalid_utf8_and_unknown_flag() {
        let mut oversize = 42_u64.to_be_bytes().to_vec();
        oversize.extend_from_slice(&(MAX_SECRET_BYTES + 1).to_be_bytes());
        let error = read_handshake(&mut std::io::Cursor::new(oversize))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

        let mut invalid_utf8 = 42_u64.to_be_bytes().to_vec();
        invalid_utf8.extend_from_slice(&1_u32.to_be_bytes());
        invalid_utf8.push(0xff);
        invalid_utf8.push(0);
        let error = read_handshake(&mut std::io::Cursor::new(invalid_utf8))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

        let mut unknown_flag = 42_u64.to_be_bytes().to_vec();
        unknown_flag.extend_from_slice(&0_u32.to_be_bytes());
        unknown_flag.push(4);
        let error = read_handshake(&mut std::io::Cursor::new(unknown_flag))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn authenticated_frame_body_read_is_bounded_after_its_prefix() {
        let (mut client, mut server) = tokio::io::duplex(16);
        client.write_all(&8_u32.to_be_bytes()).await.unwrap();

        let error = read_framed_bounded_with_timeout_and_budget(
            &mut server,
            64,
            std::time::Duration::from_millis(20),
            &crate::connection_admission::FrameByteBudget::new(64),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn authenticated_frame_byte_budget_sheds_before_allocation_and_recovers() {
        let budget = crate::connection_admission::FrameByteBudget::new(8);
        let (mut first_client, mut first_server) = tokio::io::duplex(16);
        first_client.write_all(&8_u32.to_be_bytes()).await.unwrap();
        let first_budget = budget.clone();
        let first = tokio::spawn(async move {
            read_framed_bounded_with_timeout_and_budget(
                &mut first_server,
                64,
                std::time::Duration::from_secs(1),
                &first_budget,
            )
            .await
        });
        while budget.available_bytes() != 0 {
            tokio::task::yield_now().await;
        }

        let (mut rejected_client, mut rejected_server) = tokio::io::duplex(16);
        rejected_client
            .write_all(&1_u32.to_be_bytes())
            .await
            .unwrap();
        let error = read_framed_bounded_with_timeout_and_budget(
            &mut rejected_server,
            64,
            std::time::Duration::from_secs(1),
            &budget,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::OutOfMemory);

        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(budget.available_bytes(), 8);

        let (mut recovered_client, mut recovered_server) = tokio::io::duplex(16);
        recovered_client
            .write_all(&1_u32.to_be_bytes())
            .await
            .unwrap();
        recovered_client.write_all(b"x").await.unwrap();
        let frame = read_framed_bounded_with_timeout_and_budget(
            &mut recovered_server,
            64,
            std::time::Duration::from_secs(1),
            &budget,
        )
        .await
        .unwrap();
        assert_eq!(frame.as_ref(), b"x");
    }

    #[test]
    fn activated_v2_fences_legacy_handshakes() {
        assert!(peer_semantics_compatible(FailoverSemantics::Legacy, false));
        assert!(peer_semantics_compatible(FailoverSemantics::Legacy, true));
        assert!(!peer_semantics_compatible(FailoverSemantics::V2, false));
        assert!(peer_semantics_compatible(FailoverSemantics::V2, true));
        assert!(connection_requires_upgrade(FailoverSemantics::V2, false));
        assert!(!connection_requires_upgrade(FailoverSemantics::V2, true));
    }

    #[test]
    fn activated_config_identity_fences_legacy_connections() {
        assert!(!connection_requires_config_identity(false, false));
        assert!(!connection_requires_config_identity(false, true));
        assert!(connection_requires_config_identity(true, false));
        assert!(!connection_requires_config_identity(true, true));
    }

    #[tokio::test]
    async fn config_preflight_rejects_concrete_mismatch_before_raft_frames() {
        let error = run_config_preflight(TestPeerIdentity::Different, false)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("configuration identity mismatch")
        );
        assert!(
            run_config_preflight(TestPeerIdentity::Same, true)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn config_preflight_allows_legacy_only_before_activation() {
        assert!(
            !run_config_preflight(TestPeerIdentity::Missing, false)
                .await
                .unwrap()
        );
        let error = run_config_preflight(TestPeerIdentity::Missing, true)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("configuration identity mismatch")
        );
    }

    #[tokio::test]
    async fn known_legacy_reconnect_needs_only_the_authenticated_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cfg = Config {
            node_id: 1,
            raft_listen: "127.0.0.1:17001".into(),
            client_submit_listen: "127.0.0.1:18001".into(),
            peers: vec![
                PeerConfig {
                    id: 1,
                    raft_address: "127.0.0.1:17001".into(),
                    client_submit_address: "127.0.0.1:18001".into(),
                },
                PeerConfig {
                    id: 2,
                    raft_address: address.to_string(),
                    client_submit_address: "127.0.0.1:18002".into(),
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
            cluster_secret: Some("wire-test-secret".into()),
            max_frame_bytes: 64 * 1024,
            submit_timeout_ms: DEFAULT_SUBMIT_TIMEOUT_MS,
            address_protocol: crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL,
            dry_run: true,
            notify: None,
            failover_delay_secs: 0,
            failback: true,
            failback_delay_secs: 0,
        };

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let handshake = read_handshake(&mut stream).await.unwrap();
            assert_eq!(handshake.0, 1);
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        tokio::time::timeout(
            Duration::from_millis(250),
            connect_with_legacy_handshake(&address.to_string(), &cfg, Some(7), false),
        )
        .await
        .expect("known legacy reconnect must not wait for a status response")
        .unwrap();
        server.abort();
        let _ = server.await;
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
        assert_eq!(
            read_framed_bounded(&mut reader, 1024).await.unwrap(),
            b"hello frame"
        );
    }

    #[tokio::test]
    async fn write_framed_emits_one_contiguous_frame() {
        let mut writer = RecordingWriter::default();
        write_framed(&mut writer, b"\"Success\"").await.unwrap();

        assert_eq!(writer.writes, vec![framed(b"\"Success\"")]);
    }

    #[test]
    fn single_tcp_frame_write_accepts_only_the_complete_frame() {
        require_complete_frame_write(13, 13).unwrap();

        let err = require_complete_frame_write(4, 13).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WriteZero);
    }

    #[tokio::test]
    async fn single_tcp_writer_roundtrips_a_complete_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = tokio::spawn(async move { TcpStream::connect(address).await.unwrap() });
        let (mut server, _) = listener.accept().await.unwrap();
        let client = client.await.unwrap();

        write_framed_tcp(&client, b"\"Success\"").await.unwrap();

        assert_eq!(
            read_framed_bounded(&mut server, 1024).await.unwrap(),
            b"\"Success\""
        );
    }

    #[tokio::test]
    async fn read_framed_bounded_rejects_oversize_length_before_allocating() {
        let header = 1_000_000_u32.to_be_bytes();
        let mut reader: &[u8] = &header;
        let err = read_framed_bounded(&mut reader, 64).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn status_response_cap_rejects_a_raft_sized_length_prefix() {
        let advertised = (STATUS_FRAME_MAX_BYTES + 1).to_be_bytes();
        let err = read_framed_bounded(&mut &advertised[..], STATUS_FRAME_MAX_BYTES)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_framed_bounded_errors_on_truncated_body() {
        let mut buf = 5_u32.to_be_bytes().to_vec();
        buf.extend_from_slice(b"ab");
        let err = read_framed_bounded(&mut &buf[..], 1024).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
