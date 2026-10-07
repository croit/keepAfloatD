//! OpenRaft type configuration and replicated request/response types for keepAfloatD.
//!
//! The replicated log carries local health updates plus explicit "I have already unbound this VIP"
//! acknowledgements from the previous holder. VIP ownership is still derived deterministically in
//! the state machine, but the committed state also tracks a per-VIP handoff generation so the new
//! holder can wait for a safe release point before binding.

use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::net::IpAddr;

/// Replicated failover/failback behavior selected for one cluster incarnation.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailoverSemantics {
    /// Historical state-machine behavior retained for compatibility decoding only.
    /// It does not permit mixed-protocol voters or a rolling protocol upgrade.
    #[default]
    Legacy,
    /// Ownership-aware nopreempt and silent-recovery behavior.
    V2,
}

/// Commands replicated through Raft and applied to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KafRequest {
    /// Consumer-specific challenge whose complete contents must be applied before use.
    HealthProgress(super::admission::HealthProgress),
    /// Immutable descriptor committed only after admission's reservation quorum.
    AdmissionGenesis(super::admission::Genesis),
    AdmissionMembership(super::admission::AdmissionCommand),
    /// Local health result for `node_id`.
    ///
    /// Each daemon process must publish updates only for **its own**
    /// [`crate::config::Config::node_id`]. The state machine maintains a per-node committed probe
    /// counter, incremented on every applied update, and expires stale peers by comparing that
    /// node-local counter with the most recent committed probe round seen anywhere in the cluster.
    HealthUpdate {
        node_id: u64,
        healthy: bool,
    },
    /// Best-effort acknowledgement from the previous holder after it has already removed `vip`
    /// from the local kernel. `generation` fences delayed acks from older handoff attempts.
    VipReleased {
        node_id: u64,
        vip: IpAddr,
        generation: u64,
    },
    /// Historical set-once incarnation record retained for compatibility decoding.
    /// Current formation uses `AdmissionGenesis`; decoding this record grants no runtime authority.
    ClusterFormed {
        cluster_id: u128,
        /// Historical entries without this field retain legacy state-machine semantics.
        #[serde(default)]
        failover_semantics: FailoverSemantics,
        /// Historical entries without this field leave identity enforcement disabled.
        /// Current admitted genesis enables enforcement without a mixed-protocol mode.
        #[serde(default)]
        config_identity_enforced: bool,
    },
    /// Historical one-way semantics activation retained for compatibility decoding.
    EnableFailoverSemanticsV2,
    /// Historical one-way identity activation retained for compatibility decoding.
    EnableConfigIdentityV1,
}

impl std::fmt::Display for KafRequest {
    // openraft 0.10 requires the replicated-data type `D` to implement `Display` (via the `AppData`
    // bound). Used only for tracing/diagnostics; keep it compact and side-effect-free.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HealthProgress(progress) => {
                write!(f, "HealthProgress(replica={})", progress.replica)
            }
            Self::AdmissionGenesis(genesis) => {
                write!(f, "AdmissionGenesis(epoch={})", genesis.epoch)
            }
            Self::AdmissionMembership(command) => write!(f, "AdmissionMembership({command:?})"),
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
            Self::ClusterFormed {
                cluster_id,
                failover_semantics,
                config_identity_enforced,
            } => write!(
                f,
                "ClusterFormed(id={cluster_id}, failover={failover_semantics:?}, config_identity={config_identity_enforced})"
            ),
            Self::EnableFailoverSemanticsV2 => write!(f, "EnableFailoverSemanticsV2"),
            Self::EnableConfigIdentityV1 => write!(f, "EnableConfigIdentityV1"),
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
            Self::HealthProgress(progress) => Some(progress.node_id),
            Self::AdmissionGenesis(_) | Self::AdmissionMembership(_) => None,
            Self::HealthUpdate { node_id, .. } | Self::VipReleased { node_id, .. } => {
                Some(*node_id)
            }
            Self::ClusterFormed { .. }
            | Self::EnableFailoverSemanticsV2
            | Self::EnableConfigIdentityV1 => None,
        }
    }
}

/// Response returned after applying one log entry (minimal surface for v1).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KafResponse {
    Ok,
    Rejected(String),
}

/// In-memory snapshot payload shared by the state machine and Raft transport.
pub type KafSnapshotData = Cursor<Vec<u8>>;

openraft::declare_raft_types!(
    /// Marker type wiring OpenRaft generics for this daemon.
    ///
    /// Physical configuration IDs never serve as volatile consensus identities.
    pub TypeConfig:
        NodeId = crate::raft::admission::ReplicaId,
        D = KafRequest,
        R = KafResponse,
);

#[cfg(test)]
pub(crate) fn test_replica(physical_id: u64) -> crate::raft::admission::ReplicaId {
    crate::raft::admission::ReplicaId {
        physical_id,
        boot_nonce: [42; 32],
    }
}

#[cfg(test)]
mod tests {
    use super::{FailoverSemantics, KafRequest, KafResponse};
    use serde::Deserialize;

    #[test]
    fn openraft_node_id_is_the_exact_boot_replica() {
        assert_eq!(
            std::any::TypeId::of::<<super::TypeConfig as openraft::RaftTypeConfig>::NodeId>(),
            std::any::TypeId::of::<crate::raft::admission::ReplicaId>()
        );
    }

    #[test]
    fn admission_membership_commands_are_cluster_scoped_and_roundtrip() {
        use crate::raft::admission::{AdmissionCommand, Genesis, JoinPlan};
        let consumer = super::test_replica(2);
        let previous = std::collections::BTreeSet::from([super::test_replica(1)]);
        let commands = [
            AdmissionCommand::PrepareJoin(JoinPlan {
                genesis: Genesis {
                    config: crate::config::ClusterConfigFingerprint {
                        version: 1,
                        digest: [8; 32],
                    },
                    epoch: 9,
                    voters: previous.clone(),
                },
                consumer,
                request_nonce: [4; 32],
                previous_voters: previous,
                next_voters: std::collections::BTreeSet::from([super::test_replica(1), consumer]),
            }),
            AdmissionCommand::LearnerApplied {
                consumer,
                request_nonce: [4; 32],
                prepared: openraft::testing::log_id::<super::TypeConfig>(
                    1,
                    super::test_replica(1),
                    2,
                ),
            },
        ];
        for command in commands {
            let request = KafRequest::AdmissionMembership(command);
            assert_eq!(request.node_id(), None);
            assert!(request.to_string().starts_with("AdmissionMembership("));
            assert_eq!(
                serde_json::from_slice::<KafRequest>(&serde_json::to_vec(&request).unwrap())
                    .unwrap(),
                request
            );
        }
    }

    #[derive(Deserialize)]
    enum LegacyKafRequest {
        ClusterFormed { cluster_id: u128 },
    }

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
                failover_semantics: FailoverSemantics::V2,
                config_identity_enforced: true,
            },
            KafRequest::EnableFailoverSemanticsV2,
            KafRequest::EnableConfigIdentityV1,
        ];
        for req in cases {
            let v = serde_json::to_vec(&req).unwrap();
            let back: KafRequest = serde_json::from_slice(&v).unwrap();
            assert_eq!(req, back);
        }
    }

    #[test]
    fn kaf_request_display_covers_every_wire_command() {
        let cases = [
            (
                KafRequest::HealthUpdate {
                    node_id: 3,
                    healthy: false,
                },
                "HealthUpdate(node=3, healthy=false)".to_owned(),
            ),
            (
                KafRequest::VipReleased {
                    node_id: 4,
                    vip: "10.0.0.4".parse().unwrap(),
                    generation: 9,
                },
                "VipReleased(node=4, vip=10.0.0.4, gen=9)".to_owned(),
            ),
            (
                KafRequest::ClusterFormed {
                    cluster_id: 7,
                    failover_semantics: FailoverSemantics::V2,
                    config_identity_enforced: true,
                },
                "ClusterFormed(id=7, failover=V2, config_identity=true)".to_owned(),
            ),
            (
                KafRequest::EnableFailoverSemanticsV2,
                "EnableFailoverSemanticsV2".to_owned(),
            ),
            (
                KafRequest::EnableConfigIdentityV1,
                "EnableConfigIdentityV1".to_owned(),
            ),
        ];
        for (request, expected) in cases {
            assert_eq!(request.to_string(), expected);
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
        assert_eq!(
            KafRequest::ClusterFormed {
                cluster_id: 7,
                failover_semantics: FailoverSemantics::Legacy,
                config_identity_enforced: false,
            }
            .node_id(),
            None
        );
        assert_eq!(KafRequest::EnableFailoverSemanticsV2.node_id(), None);
        assert_eq!(KafRequest::EnableConfigIdentityV1.node_id(), None);
    }

    #[test]
    fn legacy_cluster_formed_json_defaults_to_legacy_semantics() {
        let legacy = r#"{"ClusterFormed":{"cluster_id":7}}"#;
        assert_eq!(
            serde_json::from_str::<KafRequest>(legacy).unwrap(),
            KafRequest::ClusterFormed {
                cluster_id: 7,
                failover_semantics: FailoverSemantics::Legacy,
                config_identity_enforced: false,
            }
        );
    }

    #[test]
    fn fresh_formation_field_is_old_reader_compatible_but_activation_is_not() {
        let formed = serde_json::to_vec(&KafRequest::ClusterFormed {
            cluster_id: 7,
            failover_semantics: FailoverSemantics::V2,
            config_identity_enforced: true,
        })
        .unwrap();
        let LegacyKafRequest::ClusterFormed { cluster_id } =
            serde_json::from_slice::<LegacyKafRequest>(&formed).unwrap();
        assert_eq!(cluster_id, 7);

        let activation = serde_json::to_vec(&KafRequest::EnableFailoverSemanticsV2).unwrap();
        assert!(serde_json::from_slice::<LegacyKafRequest>(&activation).is_err());
        let identity_activation = serde_json::to_vec(&KafRequest::EnableConfigIdentityV1).unwrap();
        assert!(serde_json::from_slice::<LegacyKafRequest>(&identity_activation).is_err());
    }

    #[test]
    fn kaf_response_json_roundtrip() {
        let ok = KafResponse::Ok;
        let enc = serde_json::to_vec(&ok).unwrap();
        assert_eq!(serde_json::from_slice::<KafResponse>(&enc).unwrap(), ok);
    }
}
