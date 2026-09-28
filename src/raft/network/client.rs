//! OpenRaft's outbound client traits over the shared peer transport.

use super::RaftNetworkImpl;
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

pub struct RaftConnection {
    pub(super) network: Arc<RaftNetworkImpl>,
    pub(super) target: u64,
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
}
