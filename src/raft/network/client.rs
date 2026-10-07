//! OpenRaft's outbound client traits over the shared peer transport.

use super::RaftNetworkImpl;
use super::authorization::ReplicaId;
use super::request::{Operation, decode_payload, encode};
use crate::raft::types::{KafSnapshotData, TypeConfig};
use openraft::alias::{SnapshotOf, VoteOf};
use openraft::error::{RPCError, ReplicationClosed, StreamingError};
use openraft::network::v2::RaftNetworkV2;
use openraft::network::{RPCOption, RPCTypes, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, OptionalSend};
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

pub struct RaftConnection {
    pub(super) network: Arc<RaftNetworkImpl>,
    pub(super) target: ReplicaId,
}

impl RaftNetworkFactory<TypeConfig> for RaftNetworkImpl {
    type Network = RaftConnection;

    async fn new_client(&mut self, target: ReplicaId, _node: &BasicNode) -> Self::Network {
        RaftConnection {
            network: Arc::new(self.clone()),
            target,
        }
    }
}

impl RaftNetworkV2<TypeConfig> for RaftConnection {
    type SnapshotData = KafSnapshotData;

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
        snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let transfer = super::SnapshotTransfer {
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

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let started = Instant::now();
        let budget = option.hard_ttl().max(super::RPC_MIN_TIMEOUT);
        let result = tokio::time::timeout(budget, self.pre_vote_exchange(rpc)).await;
        match result {
            Ok(result) => result.map_err(|error| {
                tracing::debug!(target = %self.target, %error, "Pre-Vote exchange failed");
                RPCError::Network(openraft::error::NetworkError::from_string(
                    error.to_string(),
                ))
            }),
            Err(_) => Err(RPCError::Timeout(openraft::error::Timeout {
                action: RPCTypes::Vote,
                id: self.network.admission.local_replica(),
                target: self.target,
                timeout: started.elapsed(),
            })),
        }
    }
}

impl RaftConnection {
    async fn pre_vote_exchange(
        &self,
        rpc: VoteRequest<TypeConfig>,
    ) -> anyhow::Result<VoteResponse<TypeConfig>> {
        use super::wire::{
            connect_with_preflight, connection_requires_upgrade, epochs_compatible,
            read_framed_bounded, write_framed,
        };
        use crate::raft::probe::config_identity_compatible;
        use anyhow::Context;

        let link = self
            .network
            .peers
            .get(&self.target.physical_id)
            .context("unknown Pre-Vote peer")?;
        let (epoch, advertises_v2, identity_enforced) = {
            let state = self.network.state_ref.read().await;
            (
                state.cluster_epoch,
                state.failover_semantics == crate::raft::FailoverSemantics::V2,
                state.config_identity_enforced,
            )
        };
        // A fresh preflight binds capability and identity checks to this RPC's connection.
        let (mut stream, response, remote_replica) = connect_with_preflight(
            &link.address,
            &self.network.config,
            epoch,
            advertises_v2,
            self.network.config_fingerprint,
            identity_enforced,
            self.network.admission.local_replica(),
        )
        .await?;
        anyhow::ensure!(
            remote_replica == self.target,
            "Pre-Vote destination boot changed"
        );
        {
            let state = self.network.state_ref.read().await;
            anyhow::ensure!(
                epochs_compatible(state.cluster_epoch, response.cluster_epoch)
                    && !connection_requires_upgrade(state.failover_semantics, advertises_v2)
                    && config_identity_compatible(
                        self.network.config_fingerprint,
                        response.config_fingerprint,
                        state.config_identity_enforced || response.config_identity_enforced
                    ),
                "Pre-Vote connection policy changed during preflight"
            );
        }
        anyhow::ensure!(
            response.supports_pre_vote,
            "protocol-three peer lacks required Pre-Vote support"
        );
        let body = encode(Operation::PreVote, &rpc)?;
        let max_frame = self.network.config.max_frame_bytes;
        anyhow::ensure!(
            body.len() as u64 <= u64::from(max_frame),
            "Pre-Vote exceeds max_frame_bytes"
        );
        let authorization = self
            .network
            .admission
            .authorize_raft_async(self.target)
            .await?;
        authorization.check(self.network.admission.local_replica(), self.target)?;
        authorization.validate_request(
            self.network.admission.local_replica(),
            &super::request::RaftRequest::PreVote(rpc),
        )?;
        let deadline = authorization.session.check()?;
        let body = tokio::time::timeout_at(deadline, async {
            write_framed(&mut stream, &body).await?;
            read_framed_bounded(&mut stream, max_frame).await
        })
        .await
        .context("Pre-Vote admission expired during exchange")??;
        let authorization = self
            .network
            .admission
            .authorize_raft_async(self.target)
            .await?;
        authorization.check(self.network.admission.local_replica(), self.target)?;
        authorization.validate_response(&body)?;
        decode_payload(&body, Operation::PreVote)
    }
}
