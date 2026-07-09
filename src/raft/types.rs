//! OpenRaft type configuration and replicated request/response types for keepAfloatD.
//!
//! The replicated log carries local health updates plus explicit "I have already unbound this VIP"
//! acknowledgements from the previous holder. VIP ownership is still derived deterministically in
//! the state machine, but the committed state also tracks a per-VIP handoff generation so the new
//! holder can wait for a safe release point before binding.

use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Commands replicated through Raft and applied to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KafRequest {
    /// Local health result for `node_id`.
    ///
    /// Each daemon process must publish updates only for **its own**
    /// [`crate::config::Config::node_id`]. The state machine maintains a per-node committed probe
    /// counter, incremented on every applied update, and expires stale peers by comparing that
    /// node-local counter with the most recent committed probe round seen anywhere in the cluster.
    HealthUpdate { node_id: u64, healthy: bool },
    /// Best-effort acknowledgement from the previous holder after it has already removed `vip`
    /// from the local kernel. `generation` fences delayed acks from older handoff attempts.
    VipReleased {
        node_id: u64,
        vip: IpAddr,
        generation: u64,
    },
    /// Per-formation cluster incarnation, committed exactly once by the first leader after a fresh
    /// `Raft::initialize`. It carries no `node_id` because it identifies the *cluster*, not a
    /// member. The state machine records the first committed value and ignores any later ones (the
    /// formation is idempotent), so every node converges on the same incarnation. Peers advertise
    /// this value in the transport handshake; a node that already holds a *different* incarnation
    /// rejects another's Raft RPCs, which is what stops a stale survivor from overwriting a
    /// majority that reformed without it. See `src/raft/network.rs` and `run_cluster_guard`.
    ClusterFormed { cluster_id: u128 },
}

impl std::fmt::Display for KafRequest {
    // openraft 0.10 requires the replicated-data type `D` to implement `Display` (via the `AppData`
    // bound). Used only for tracing/diagnostics; keep it compact and side-effect-free.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HealthUpdate { node_id, healthy } => {
                write!(f, "HealthUpdate(node={node_id}, healthy={healthy})")
            }
            Self::VipReleased {
                node_id,
                vip,
                generation,
            } => write!(
                f,
                "VipReleased(node={node_id}, vip={vip}, gen={generation})"
            ),
            Self::ClusterFormed { cluster_id } => write!(f, "ClusterFormed(id={cluster_id})"),
        }
    }
}

impl KafRequest {
    /// Return the member that originated this request, if the command is node-scoped.
    ///
    /// `ClusterFormed` is cluster-scoped (no originating member) and returns `None`.
    #[must_use]
    pub fn node_id(&self) -> Option<u64> {
        match self {
            Self::HealthUpdate { node_id, .. } | Self::VipReleased { node_id, .. } => {
                Some(*node_id)
            }
            Self::ClusterFormed { .. } => None,
        }
    }
}

/// Response returned after applying one log entry (minimal surface for v1).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KafResponse {
    Ok,
}

openraft::declare_raft_types!(
    /// Marker type wiring OpenRaft generics for this daemon.
    ///
    /// Only the application-specific associated types are set here; the rest
    /// (`NodeId = u64`, `Node = BasicNode`, `SnapshotData = Cursor<Vec<u8>>`,
    /// `AsyncRuntime = TokioRuntime`, the leader-id/vote/entry/responder types) take the
    /// `declare_raft_types!` defaults, which match the values keepafloatd used under openraft 0.9.
    pub TypeConfig:
        D = KafRequest,
        R = KafResponse,
);

#[cfg(test)]
mod tests {
    use super::{KafRequest, KafResponse};

    #[test]
    fn kaf_request_json_roundtrip() {
        let cases = [
            KafRequest::HealthUpdate {
                node_id: 3,
                healthy: true,
            },
            KafRequest::HealthUpdate {
                node_id: 1,
                healthy: false,
            },
            KafRequest::VipReleased {
                node_id: 9,
                vip: "10.0.0.9".parse().unwrap(),
                generation: 17,
            },
            KafRequest::ClusterFormed {
                cluster_id: 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210,
            },
        ];
        for req in cases {
            let v = serde_json::to_vec(&req).unwrap();
            let back: KafRequest = serde_json::from_slice(&v).unwrap();
            assert_eq!(req, back);
        }
    }

    #[test]
    fn node_id_is_none_for_cluster_scoped_request() {
        assert_eq!(
            KafRequest::HealthUpdate {
                node_id: 5,
                healthy: true
            }
            .node_id(),
            Some(5)
        );
        assert_eq!(KafRequest::ClusterFormed { cluster_id: 7 }.node_id(), None);
    }

    #[test]
    fn kaf_response_json_roundtrip() {
        let ok = KafResponse::Ok;
        let enc = serde_json::to_vec(&ok).unwrap();
        assert_eq!(serde_json::from_slice::<KafResponse>(&enc).unwrap(), ok);
    }
}
