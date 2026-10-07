//! Decisions for retaining or replacing a cached outbound connection.

use super::PeerLink;
use super::wire::{connection_requires_config_identity, connection_requires_upgrade};
use crate::raft::types::FailoverSemantics;
use std::sync::atomic::Ordering;
use tokio::net::TcpStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReconnectDecision {
    Keep,
    Connect,
    Replace,
}

impl PeerLink {
    pub(super) fn reconnect_decision(
        &self,
        stream: Option<&TcpStream>,
        local_semantics: FailoverSemantics,
        config_identity_enforced: bool,
    ) -> ReconnectDecision {
        let Some(stream) = stream else {
            return ReconnectDecision::Connect;
        };
        if connection_requires_upgrade(local_semantics, self.advertises_v2.load(Ordering::SeqCst))
            || connection_requires_config_identity(
                config_identity_enforced,
                self.advertises_config_identity.load(Ordering::SeqCst),
            )
            || !idle_stream_is_usable(stream)
        {
            ReconnectDecision::Replace
        } else {
            ReconnectDecision::Keep
        }
    }
}

pub(super) fn idle_stream_is_usable(stream: &TcpStream) -> bool {
    // No RPC owns this stream: EOF or unsolicited bytes require a fresh channel.
    matches!(stream.try_read(&mut [0; 1]), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock)
}
