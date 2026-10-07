//! Inbound peer stream identity gates and OpenRaft RPC dispatch.

use super::authorization::{AdmissionController, ReplicaId, ReplicationConflict};
use super::request::{self, Operation, RaftRequest, encode};
use super::status;
use super::wire::{
    epochs_compatible, peer_semantics_compatible, read_framed_bounded_with_timeout_and_budget,
    write_framed_tcp_checked,
};
use crate::config::ClusterConfigFingerprint;
use crate::connection_admission::FrameByteBudget;
use crate::raft::KafRaft;
use crate::raft::probe::config_identity_compatible;
use crate::raft::store::KafStorageState;
use crate::warning_limit::{WarningLimiter, warn_limited};
use anyhow::Context;
use openraft::Snapshot;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::Instant;

pub(super) struct InboundPeer {
    pub(super) channel_binding: [u8; 32],
    pub(super) replica: Option<ReplicaId>,
    pub(super) id: u64,
    pub(super) epoch: Option<u128>,
    pub(super) supports_v2: bool,
}

pub(super) struct InboundStreamPolicy {
    pub(super) admission: Arc<dyn AdmissionController>,
    pub(super) secret: Option<String>,
    pub(super) local_config_fingerprint: ClusterConfigFingerprint,
    pub(super) max_frame_bytes: u32,
    pub(super) legacy_response_budget: Duration,
    pub(super) frame_byte_budget: FrameByteBudget,
    pub(super) idle_timeout: Duration,
    pub(super) warnings: WarningLimiter,
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
    frame: &RaftRequest,
    peer_fingerprint: &mut Option<ClusterConfigFingerprint>,
    local_fingerprint: ClusterConfigFingerprint,
    enforcement_active: bool,
) -> ConfigFrameGate {
    let status_request = frame.status();
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
            let request = request::decode(buf.as_ref())?;
            if let RaftRequest::AdmissionControl(request) = request {
                let replica = peer
                    .replica
                    .context("management peer omitted its boot identity")?;
                anyhow::ensure!(
                    replica.physical_id == peer.id,
                    "authenticated physical identity mismatch"
                );
                let response = super::authorization::management::dispatch(
                    policy.admission.as_ref(),
                    replica,
                    peer.channel_binding,
                    policy.secret.as_deref(),
                    request,
                    RAFT_PROCESSING_TIMEOUT.saturating_sub(dispatch_started.elapsed()),
                )
                .await?;
                let gate = ConfigFrameGate {
                    is_status: true,
                    compatible: true,
                    peer_supports_cancellation_safe_rpc: None,
                };
                return Ok(Some((
                    gate,
                    encode(Operation::AdmissionControl, &response)?,
                )));
            }
            if let RaftRequest::Admission(request) = request {
                let replica = peer
                    .replica
                    .context("admission peer omitted its boot identity")?;
                let response = super::admission_rpc::dispatch(
                    policy.admission.as_ref(),
                    replica,
                    peer.channel_binding,
                    policy.secret.as_deref(),
                    request,
                )
                .await?;
                let gate = ConfigFrameGate {
                    is_status: true,
                    compatible: true,
                    peer_supports_cancellation_safe_rpc: None,
                };
                return Ok(Some((gate, encode(Operation::Admission, &response)?)));
            }
            let config_gate = classify_config_frame(
                &request,
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
                warn_limited!(
                    policy.warnings,
                    "raft inbound rpc from {}: legacy peer rejected after V2 activation",
                    peer.id
                );
                return Ok(None);
            }
            if !config_gate.may_dispatch() {
                warn_limited!(
                    policy.warnings,
                    "raft inbound rpc from {}: cluster configuration identity mismatch; dropping before dispatch",
                    peer.id
                );
                return Ok(None);
            }
            let body = dispatch_request(
                &raft,
                &peer,
                local_epoch,
                policy.local_config_fingerprint,
                config_identity_enforced,
                request,
                policy.admission.as_ref(),
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
                warn_limited!(policy.warnings, "raft inbound rpc from {}: {}", peer.id, e);
                break;
            }
        };
        if body.len() as u64 > policy.max_frame_bytes as u64 {
            warn_limited!(
                policy.warnings,
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
            write_framed_tcp_checked(&stream, &body, || async {
                if !config_gate.is_status {
                    let replica = peer.replica.ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "Raft peer omitted its boot identity",
                        )
                    })?;
                    let authorization = policy
                        .admission
                        .authorize_raft_async(replica)
                        .await
                        .map_err(std::io::Error::other)?;
                    authorization
                        .check(policy.admission.local_replica(), replica)
                        .map_err(std::io::Error::other)?;
                }
                Ok(())
            }),
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
            warn_limited!(
                policy.warnings,
                "raft status from {}: cluster configuration identity mismatch; preflight answered then stream closed",
                peer.id
            );
            break;
        }
    }
    Ok(())
}

fn validate_sender(peer_id: u64, sender_id: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        sender_id == peer_id,
        "payload sender {sender_id} does not match authenticated peer {peer_id}"
    );
    Ok(())
}

async fn dispatch_request(
    raft: &KafRaft,
    peer: &InboundPeer,
    local_epoch: Option<u128>,
    local_config_fingerprint: ClusterConfigFingerprint,
    config_identity_enforced: bool,
    request: RaftRequest,
    admission: &dyn AdmissionController,
) -> anyhow::Result<Vec<u8>> {
    let peer_id = peer.id;
    let peer_epoch = peer.epoch;
    // Status stays reachable across an identity/epoch mismatch so guards can diagnose and fence.
    if request.status().is_none() && !epochs_compatible(local_epoch, peer_epoch) {
        anyhow::bail!(
            "cluster_epoch mismatch (local {:?}, peer {:?}); dropping raft rpc",
            local_epoch,
            peer_epoch
        );
    }
    let mut replication_conflict = false;
    let authorization = if request.status().is_none() {
        let peer_replica = peer
            .replica
            .context("Raft peer omitted its boot identity")?;
        anyhow::ensure!(
            peer_replica.physical_id == peer_id,
            "authenticated physical identity mismatch"
        );
        let authorization = admission.authorize_raft_async(peer_replica).await?;
        authorization.check(admission.local_replica(), peer_replica)?;
        if let Err(error) = authorization.validate_request(peer_replica, &request) {
            if !matches!(&request, RaftRequest::AppendEntries(_))
                || !error.is::<ReplicationConflict>()
            {
                return Err(error);
            }
            replication_conflict = true;
        }
        Some(authorization)
    } else {
        None
    };
    if let Some(authorization) = &authorization {
        authorization.session.check()?;
    }
    let response: anyhow::Result<Vec<u8>> = match request {
        RaftRequest::Admission(_) | RaftRequest::AdmissionControl(_) => {
            anyhow::bail!("admission records require authenticated controller dispatch")
        }
        RaftRequest::Status(req) => {
            validate_sender(peer_id, req.probe_from)?;
            let resp = status::answer_cluster_status(
                raft,
                local_epoch,
                local_config_fingerprint,
                config_identity_enforced,
                req,
            )
            .await?;
            Ok(encode(Operation::Status, &resp)?)
        }
        RaftRequest::PreVote(req) => {
            let response = raft
                .pre_vote(req)
                .await
                .map_err(|error| anyhow::anyhow!("pre_vote: {error:?}"))?;
            Ok(encode(Operation::PreVote, &response)?)
        }
        RaftRequest::AppendEntries(req) => {
            let resp = if replication_conflict {
                tracing::debug!(
                    peer_id,
                    "authorized replication requires prefix backtracking"
                );
                openraft::raft::AppendEntriesResponse::<crate::raft::TypeConfig>::Conflict
            } else {
                raft.append_entries(req)
                    .await
                    .map_err(|e| anyhow::anyhow!("append_entries: {:?}", e))?
            };
            Ok(encode(Operation::AppendEntries, &resp)?)
        }
        RaftRequest::Snapshot(req) => {
            let snapshot = Snapshot {
                meta: req.meta,
                snapshot: Cursor::new(req.data),
            };
            let resp = raft
                .install_full_snapshot(req.vote, snapshot)
                .await
                .map_err(|e| anyhow::anyhow!("install_full_snapshot: {:?}", e))?;
            Ok(encode(Operation::InstallSnapshot, &resp)?)
        }
        RaftRequest::Vote(req) => {
            let resp = raft
                .vote(req)
                .await
                .map_err(|e| anyhow::anyhow!("vote: {:?}", e))?;
            Ok(encode(Operation::Vote, &resp)?)
        }
    };
    let response = response?;
    if let Some(authorization) = &authorization {
        authorization.session.check()?;
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::super::SnapshotTransfer;
    use super::super::request::decode_payload;
    use super::*;
    use crate::config::{
        Config, DEFAULT_MAX_FRAME_BYTES, DEFAULT_SUBMIT_TIMEOUT_MS, HealthConfig, PeerConfig,
        RaftTuneConfig,
    };
    use crate::raft::network::wire::{read_framed_bounded, write_framed, write_replica_handshake};
    use crate::raft::probe::ClusterStatusRequest;
    use crate::raft::types::TypeConfig;
    use openraft::alias::VoteOf;
    use openraft::raft::{AppendEntriesRequest, VoteRequest};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn inbound_peer(id: u64, epoch: Option<u128>) -> InboundPeer {
        InboundPeer {
            id,
            epoch,
            replica: Some(crate::raft::types::test_replica(id)),
            channel_binding: [0; 32],
            supports_v2: true,
        }
    }

    async fn dispatch_incoming(
        raft: &KafRaft,
        peer_id: u64,
        local_epoch: Option<u128>,
        peer_epoch: Option<u128>,
        fingerprint: ClusterConfigFingerprint,
        enforced: bool,
        bytes: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        let authority =
            super::super::testing::with_context(crate::raft::admission::AdmissionContext {
                local_replica: crate::raft::types::test_replica(1),
                genesis: crate::raft::admission::Genesis {
                    config: fingerprint,
                    epoch: local_epoch.unwrap_or(1),
                    voters: [1, 2, 3].map(crate::raft::types::test_replica).into(),
                },
            });
        dispatch_request(
            raft,
            &inbound_peer(peer_id, peer_epoch),
            local_epoch,
            fingerprint,
            enforced,
            request::decode(bytes)?,
            authority.as_ref(),
        )
        .await
    }

    fn classify_config_frame(
        bytes: &[u8],
        peer: &mut Option<ClusterConfigFingerprint>,
        local: ClusterConfigFingerprint,
        enforced: bool,
    ) -> ConfigFrameGate {
        super::classify_config_frame(&request::decode(bytes).unwrap(), peer, local, enforced)
    }

    fn vote_frame() -> Vec<u8> {
        encode(
            Operation::Vote,
            &VoteRequest::<TypeConfig>::new(
                VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
                None,
            ),
        )
        .unwrap()
    }

    struct ConflictController(
        std::sync::Mutex<Option<super::super::authorization::RaftAuthorization>>,
    );

    impl AdmissionController for ConflictController {
        fn local_replica(&self) -> ReplicaId {
            crate::raft::types::test_replica(1)
        }

        fn authorize_raft(
            &self,
            _: ReplicaId,
        ) -> Result<
            super::super::authorization::RaftAuthorization,
            crate::raft::admission::AdmissionDenied,
        > {
            Ok(self.0.lock().unwrap().take().unwrap())
        }

        fn dispatch(
            &self,
            _: ReplicaId,
            _: [u8; 32],
            _: super::super::authorization::AdmissionRpc,
        ) -> futures::future::BoxFuture<'_, anyhow::Result<super::super::authorization::AdmissionRpc>>
        {
            Box::pin(async { anyhow::bail!("no admission RPC in dispatch fixture") })
        }
    }

    async fn conflict_fixture() -> (KafRaft, ConflictController, ClusterConfigFingerprint) {
        let network = super::super::tests::test_network(65_536, &[1, 2]);
        let cfg = network.config.clone();
        let peer = crate::raft::types::test_replica(2);
        let fixture = super::super::testing::controller(&cfg);
        let (_, _, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
        let mut state = state.write().await;
        state.admission = Some(fixture.authorize_raft(peer).unwrap().session);
        let authorization =
            super::super::authorization::RaftAuthorization::from_state(&cfg, &state, peer, None)
                .unwrap();
        let (log, machine, _) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
        let raft = KafRaft::new(
            crate::raft::types::test_replica(1),
            Arc::new(openraft::Config {
                enable_tick: false,
                ..Default::default()
            }),
            network,
            log,
            machine,
        )
        .await
        .unwrap();
        // A stopped Raft makes accidental dispatch observable without starting a listener.
        raft.shutdown().await.unwrap();
        (
            raft,
            ConflictController(std::sync::Mutex::new(Some(authorization))),
            cfg.cluster_config_fingerprint().unwrap(),
        )
    }

    fn missing_prefix_request(sender: ReplicaId, history: ReplicaId) -> RaftRequest {
        RaftRequest::AppendEntries(AppendEntriesRequest::<TypeConfig> {
            vote: VoteOf::<TypeConfig>::new(7, sender),
            prev_log_id: Some(openraft::testing::log_id::<TypeConfig>(7, history, 10)),
            entries: vec![openraft::Entry {
                log_id: openraft::testing::log_id::<TypeConfig>(7, history, 11),
                payload: openraft::EntryPayload::Blank,
            }],
            leader_commit: None,
        })
    }

    #[tokio::test]
    async fn replication_prefix_conflict_returns_append_response_without_raft_dispatch() {
        let (raft, controller, fingerprint) = conflict_fixture().await;
        let peer = crate::raft::types::test_replica(2);
        let response = dispatch_request(
            &raft,
            &inbound_peer(2, Some(1)),
            Some(1),
            fingerprint,
            false,
            missing_prefix_request(peer, peer),
            &controller,
        )
        .await
        .unwrap();
        assert!(matches!(
            decode_payload::<openraft::raft::AppendEntriesResponse<TypeConfig>>(
                &response,
                Operation::AppendEntries,
            )
            .unwrap(),
            openraft::raft::AppendEntriesResponse::Conflict
        ));
    }

    struct WaitingController {
        inner: ConflictController,
        gate: tokio::sync::RwLock<()>,
    }

    impl AdmissionController for WaitingController {
        fn local_replica(&self) -> ReplicaId {
            self.inner.local_replica()
        }

        fn authorize_raft(
            &self,
            _: ReplicaId,
        ) -> Result<
            super::super::authorization::RaftAuthorization,
            crate::raft::admission::AdmissionDenied,
        > {
            Err(crate::raft::admission::AdmissionDenied(
                "synchronous authorization used",
            ))
        }

        fn authorize_raft_async(
            &self,
            peer: ReplicaId,
        ) -> futures::future::BoxFuture<
            '_,
            Result<
                super::super::authorization::RaftAuthorization,
                crate::raft::admission::AdmissionDenied,
            >,
        > {
            Box::pin(async move {
                let _read = self.gate.read().await;
                self.inner.authorize_raft(peer)
            })
        }

        fn dispatch(
            &self,
            peer: ReplicaId,
            binding: [u8; 32],
            request: super::super::authorization::AdmissionRpc,
        ) -> futures::future::BoxFuture<'_, anyhow::Result<super::super::authorization::AdmissionRpc>>
        {
            self.inner.dispatch(peer, binding, request)
        }
    }

    #[tokio::test]
    async fn inbound_authorization_waits_without_dispatching_or_rejecting() {
        let (raft, inner, fingerprint) = conflict_fixture().await;
        let controller = WaitingController {
            inner,
            gate: tokio::sync::RwLock::new(()),
        };
        let writer = controller.gate.write().await;
        let peer = crate::raft::types::test_replica(2);
        let inbound = inbound_peer(2, Some(1));
        let response = dispatch_request(
            &raft,
            &inbound,
            Some(1),
            fingerprint,
            false,
            missing_prefix_request(peer, peer),
            &controller,
        );
        tokio::pin!(response);
        assert!(futures::poll!(&mut response).is_pending());
        drop(writer);
        let response = response.await.unwrap();
        assert!(matches!(
            decode_payload::<openraft::raft::AppendEntriesResponse<TypeConfig>>(
                &response,
                Operation::AppendEntries,
            )
            .unwrap(),
            openraft::raft::AppendEntriesResponse::Conflict
        ));
    }

    #[tokio::test]
    async fn replication_prefix_conflict_does_not_mask_authorization_errors() {
        let peer = crate::raft::types::test_replica(2);
        for (sender, history, expected) in [
            (
                crate::raft::types::test_replica(1),
                peer,
                "authenticated boot",
            ),
            (
                peer,
                crate::raft::types::test_replica(99),
                "unauthorized replica",
            ),
        ] {
            let (raft, controller, fingerprint) = conflict_fixture().await;
            let error = dispatch_request(
                &raft,
                &inbound_peer(2, Some(1)),
                Some(1),
                fingerprint,
                false,
                missing_prefix_request(sender, history),
                &controller,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains(expected), "{error:#}");
            assert!(!error.is::<super::super::authorization::ReplicationConflict>());
        }
    }

    #[tokio::test]
    async fn replication_prefix_conflict_rechecks_permission_before_reply() {
        use crate::raft::admission::{
            AdmissionContext, AdmissionDenied, AdmissionFence, AdmissionSession,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct ExpiringFence(AdmissionContext, AtomicUsize);

        impl AdmissionFence for ExpiringFence {
            fn local_replica(&self) -> ReplicaId {
                self.0.local_replica
            }
            fn check(&self, context: &AdmissionContext) -> Result<Instant, AdmissionDenied> {
                assert_eq!(context, &self.0);
                // Construction, peer check and payload validation succeed; replying must not.
                if self.1.fetch_add(1, Ordering::SeqCst) >= 3 {
                    return Err(AdmissionDenied("fixture permission revoked before reply"));
                }
                Ok(Instant::now() + Duration::from_secs(60))
            }
        }

        let (raft, controller, fingerprint) = conflict_fixture().await;
        {
            let mut grant = controller.0.lock().unwrap();
            let grant = grant.as_mut().unwrap();
            let context = grant.session.context().clone();
            grant.session = AdmissionSession::new(
                context.clone(),
                Arc::new(ExpiringFence(context, AtomicUsize::new(0))),
            )
            .unwrap();
        }
        let peer = crate::raft::types::test_replica(2);
        let error = dispatch_request(
            &raft,
            &inbound_peer(2, Some(1)),
            Some(1),
            fingerprint,
            false,
            missing_prefix_request(peer, peer),
            &controller,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("permission revoked before reply"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn request_decoding_dispatches_only_the_explicit_operation() {
        let (log, machine, _) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
        let raft = KafRaft::new(
            crate::raft::types::test_replica(1),
            Arc::new(openraft::Config {
                enable_tick: false,
                ..Default::default()
            }),
            super::super::tests::test_network(65_536, &[1, 2]),
            log,
            machine,
        )
        .await
        .unwrap();
        raft.shutdown().await.unwrap();
        let vote = VoteRequest::<TypeConfig>::new(
            VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
            None,
        );
        let append = encode(
            Operation::AppendEntries,
            &AppendEntriesRequest::<TypeConfig> {
                vote: VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            },
        )
        .unwrap();
        let genesis = crate::raft::admission::Genesis {
            config: fingerprint(1),
            epoch: 1,
            voters: [1, 2, 3].map(crate::raft::types::test_replica).into(),
        };
        let membership = openraft::alias::StoredMembershipOf::<TypeConfig>::new(
            None,
            openraft::Membership::new_with_defaults(vec![genesis.voters.clone()], []),
        );
        let snapshot = encode(
            Operation::InstallSnapshot,
            &SnapshotTransfer {
                vote: VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
                meta: openraft::SnapshotMeta {
                    last_log_id: None,
                    last_membership: membership.clone(),
                    snapshot_id: "wire-characterization".into(),
                },
                data: serde_json::to_vec(&serde_json::json!({ "genesis": genesis, "cluster_epoch": 1, "last_applied": null, "last_membership": membership, "applied_progress": {} })).unwrap(),
            },
        )
        .unwrap();
        for (request, method) in [
            (encode(Operation::Vote, &vote).unwrap(), "vote:"),
            (append, "append_entries:"),
            (snapshot, "install_full_snapshot:"),
            (encode(Operation::PreVote, &vote).unwrap(), "pre_vote:"),
        ] {
            let error =
                dispatch_incoming(&raft, 2, Some(1), Some(1), fingerprint(1), false, &request)
                    .await
                    .unwrap_err();
            assert!(
                error.to_string().starts_with(method),
                "{request:?}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn explicit_status_remains_reachable_across_epoch_fence() {
        let (log, machine, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
        let raft = KafRaft::new(
            crate::raft::types::test_replica(1),
            Arc::new(openraft::Config {
                enable_tick: false,
                ..Default::default()
            }),
            super::super::tests::test_network(65_536, &[1, 2]),
            log,
            machine,
        )
        .await
        .unwrap();
        let response = dispatch_incoming(
            &raft,
            2,
            Some(1),
            Some(2),
            fingerprint(1),
            false,
            br#"{"status":{"probe_from":2}}"#,
        )
        .await
        .unwrap();
        decode_payload::<crate::raft::probe::ClusterStatusResponse>(&response, Operation::Status)
            .unwrap();
        let error = dispatch_incoming(
            &raft,
            2,
            Some(1),
            Some(2),
            fingerprint(1),
            false,
            &vote_frame(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().starts_with("cluster_epoch mismatch"));
        assert!(state.read().await.vote.is_none());
        raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pre_vote_dispatch_is_read_only_and_rejects_ambiguous_envelopes() {
        let (log, machine, state) = crate::raft::store::new_store(Arc::new(Vec::new()), 3, true, 0);
        let network = super::super::tests::test_network(65_536, &[1, 2, 3]);
        let raft = KafRaft::new(
            crate::raft::types::test_replica(1),
            Arc::new(openraft::Config {
                enable_tick: false,
                ..Default::default()
            }),
            network,
            log,
            machine,
        )
        .await
        .unwrap();
        let vote = VoteRequest::<TypeConfig>::new(
            VoteOf::<TypeConfig>::new(7, crate::raft::types::test_replica(2)),
            None,
        );
        let body = serde_json::to_vec(&serde_json::json!({"pre_vote": vote})).unwrap();
        let result =
            dispatch_incoming(&raft, 2, Some(1), Some(1), fingerprint(1), false, &body).await;
        let unchanged = {
            let state = state.read().await;
            state.vote.is_none() && state.log.is_empty() && state.last_applied_log.is_none()
        };
        for marker in [
            serde_json::to_value(&vote).unwrap(),
            serde_json::Value::Null,
            serde_json::json!(false),
            serde_json::json!([1, 2, 3]),
        ] {
            let mut ambiguous = serde_json::to_value(&vote).unwrap();
            ambiguous["pre_vote"] = marker;
            let rejected = dispatch_incoming(
                &raft,
                2,
                Some(1),
                Some(1),
                fingerprint(1),
                false,
                &serde_json::to_vec(&ambiguous).unwrap(),
            )
            .await
            .is_err();
            assert!(
                rejected,
                "hybrid envelope was processed as an ordinary vote"
            );
            let state = state.read().await;
            assert!(state.vote.is_none() && state.log.is_empty());
        }
        raft.shutdown().await.unwrap();
        let response: openraft::raft::VoteResponse<TypeConfig> = decode_payload(
            &result.expect("authenticated Pre-Vote must be dispatched"),
            Operation::PreVote,
        )
        .unwrap();
        assert!(response.vote_granted);
        assert!(unchanged, "Pre-Vote mutated vote or log state");
    }

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
        let status = encode(
            Operation::Status,
            &ClusterStatusRequest {
                probe_from: 2,
                config_fingerprint: Some(fingerprint(2)),
                supports_cancellation_safe_rpc_v1: true,
            },
        )
        .unwrap();

        let status_gate = classify_config_frame(&status, &mut peer, local, false);
        assert!(status_gate.is_status);
        assert!(status_gate.may_dispatch());
        assert!(!status_gate.compatible);
        assert_eq!(status_gate.peer_supports_cancellation_safe_rpc, Some(true));

        let legacy_status = br#"{"status":{"probe_from":2}}"#;
        let legacy_gate = classify_config_frame(legacy_status, &mut peer, local, false);
        assert_eq!(legacy_gate.peer_supports_cancellation_safe_rpc, Some(false));

        let raft_gate = classify_config_frame(&vote_frame(), &mut peer, local, false);
        assert!(!raft_gate.is_status);
        assert!(!raft_gate.may_dispatch());
        assert_eq!(raft_gate.peer_supports_cancellation_safe_rpc, None);
    }

    #[test]
    fn activation_rejects_missing_identity_before_raft_dispatch() {
        let gate = classify_config_frame(&vote_frame(), &mut None, fingerprint(1), true);

        assert!(!gate.is_status);
        assert!(!gate.may_dispatch());
    }

    #[test]
    fn matching_identity_allows_raft_dispatch() {
        let local = fingerprint(1);
        let gate = classify_config_frame(&vote_frame(), &mut Some(local), local, true);

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
        let reserved = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let raft_port = reserved.local_addr().unwrap().port();
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
            cluster_secret: Some("cluster-test-secret-01234567890123".into()),
            cluster_secret_file: None,
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
        let (raft, network, state) = super::super::testing::start_transport(
            cfg.clone(),
            crate::listener::ListenerSource::Bound(reserved),
        )
        .await;

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
                    channel_binding: [0; 32],
                    id: 2,
                    replica: Some(crate::raft::types::test_replica(2)),
                    epoch: None,
                    supports_v2: true,
                },
                InboundStreamPolicy {
                    admission: super::super::testing::controller(&cfg),
                    secret: cfg.cluster_secret.clone(),
                    local_config_fingerprint: local_fingerprint,
                    max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
                    legacy_response_budget: Duration::ZERO,
                    frame_byte_budget: FrameByteBudget::new(128 * 1024 * 1024),
                    idle_timeout: Duration::from_secs(5),
                    warnings: WarningLimiter::default(),
                },
            )
            .await
        });

        let status = encode(
            Operation::Status,
            &ClusterStatusRequest {
                probe_from: 2,
                config_fingerprint: Some(local_fingerprint),
                supports_cancellation_safe_rpc_v1: false,
            },
        )
        .unwrap();
        write_framed(&mut client, &status).await.unwrap();
        read_framed_bounded(&mut client, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap();

        let vote = VoteRequest::<TypeConfig>::new(
            VoteOf::<TypeConfig>::new(1, crate::raft::types::test_replica(2)),
            None,
        );
        write_framed(&mut client, &encode(Operation::Vote, &vote).unwrap())
            .await
            .unwrap();
        let late_response = read_framed_bounded(&mut client, DEFAULT_MAX_FRAME_BYTES).await;
        assert!(
            late_response.is_err(),
            "a V2-only handshake must not imply cancellation-safe stream ownership"
        );

        server_task.await.unwrap().unwrap();
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
        let reserved = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = reserved.local_addr().unwrap().port();
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
            cluster_secret: Some("cluster-test-secret-01234567890123".into()),
            cluster_secret_file: None,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            submit_timeout_ms: DEFAULT_SUBMIT_TIMEOUT_MS,
            address_protocol: crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL,
            dry_run: true,
            notify: None,
            failover_delay_secs: 0,
            failback: true,
            failback_delay_secs: 0,
        });
        let (raft, network, _state) = super::super::testing::start_transport(
            cfg.clone(),
            crate::listener::ListenerSource::Bound(reserved),
        )
        .await;

        let mut stream = TcpStream::connect(address).await.unwrap();
        write_replica_handshake(
            &mut stream,
            crate::raft::types::test_replica(1),
            1,
            Some("cluster-test-secret-01234567890123"),
            None,
            false,
        )
        .await
        .unwrap();
        stream.write_all(&2_u32.to_be_bytes()).await.unwrap();
        stream.write_all(b"{}").await.unwrap();
        let mut byte = [0_u8; 1];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);

        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_unsafe_peer_stream_reuses_a_fresh_raft_response() {
        let reserved = TcpListener::bind(("127.250.0.2", 0)).await.unwrap();
        let port = reserved.local_addr().unwrap().port();
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
            cluster_secret: Some("cluster-test-secret-01234567890123".into()),
            cluster_secret_file: None,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            submit_timeout_ms: DEFAULT_SUBMIT_TIMEOUT_MS,
            address_protocol: crate::config::DEFAULT_VIP_ADDRESS_PROTOCOL,
            dry_run: true,
            notify: None,
            failover_delay_secs: 0,
            failback: true,
            failback_delay_secs: 0,
        });
        let (raft, network, _state) = super::super::testing::start_transport(
            cfg.clone(),
            crate::listener::ListenerSource::Bound(reserved),
        )
        .await;

        let mut stream = crate::connection_admission::connect_from_advertised(&address, &address)
            .await
            .unwrap();
        write_replica_handshake(
            &mut stream,
            crate::raft::types::test_replica(1),
            1,
            Some("cluster-test-secret-01234567890123"),
            None,
            false,
        )
        .await
        .unwrap();
        let status = encode(Operation::Status, &serde_json::json!({"probe_from": 1})).unwrap();
        write_framed(&mut stream, &status).await.unwrap();
        read_framed_bounded(&mut stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap();

        let vote = VoteRequest::<TypeConfig>::new(
            VoteOf::<TypeConfig>::new(1, crate::raft::types::test_replica(1)),
            None,
        );
        let vote = encode(Operation::Vote, &vote).unwrap();
        write_framed(&mut stream, &vote).await.unwrap();
        read_framed_bounded(&mut stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap();

        let second = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            write_framed(&mut stream, &vote).await?;
            read_framed_bounded(&mut stream, DEFAULT_MAX_FRAME_BYTES).await
        })
        .await
        .expect("a fresh response must preserve the cancellation-unsafe stream")
        .expect("connection must serve the second fresh Raft response");
        decode_payload::<openraft::raft::VoteResponse<TypeConfig>>(&second, Operation::Vote)
            .unwrap();

        network.shutdown().await.unwrap();
        raft.shutdown().await.unwrap();
    }
}
