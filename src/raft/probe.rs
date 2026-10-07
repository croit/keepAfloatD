//! Read-only discovery for the admission controller, available during quarantine.
//!
//! Status can identify a reachable boot and report its observed history. Neither reachability
//! nor a reported leader authorizes initialization, voting, learner admission or renewal.
//! Those decisions require the admission controller and a checked process-local session.
//!
//! The probe travels over the same TCP transport and handshake (node-id + `cluster_secret` +
//! peer-id gate) as Raft RPCs, so it is authenticated identically. The request/response structs
//! are payloads inside the explicit `status` operation envelope. Protocol version and operation
//! validation happen before the dispatcher handles a status or OpenRaft RPC payload.

use crate::config::ClusterConfigFingerprint;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Asks a peer to report whether it is already part of a formed cluster.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterStatusRequest {
    /// `node_id` of the asking node (diagnostic / symmetry with the handshake).
    pub probe_from: u64,
    /// Canonical identity of the asking node's cluster-wide settings. Missing means a legacy peer.
    #[serde(default)]
    pub config_fingerprint: Option<ClusterConfigFingerprint>,
    /// Whether the asking binary drops an RPC stream when its in-flight request is cancelled.
    /// Missing means a legacy reader that may retain a complete unread response.
    #[serde(default)]
    pub supports_cancellation_safe_rpc_v1: bool,
}

/// A peer's answer to [`ClusterStatusRequest`], computed from its local Raft state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterStatusResponse {
    /// Authenticated responder boot, filled from the handshake, never trusted from status JSON.
    #[serde(skip)]
    pub replica: Option<super::admission::ReplicaId>,
    /// Whether this peer handles the distinct, read-only Pre-Vote envelope.
    #[serde(default)]
    pub supports_pre_vote: bool,
    /// True once `Raft::initialize` (or replicated membership) has taken effect locally.
    pub initialized: bool,
    /// Current leader as seen by the responder, if any.
    pub current_leader: Option<super::admission::ReplicaId>,
    /// Number of voters in the responder's committed membership (0 when uninitialized).
    pub member_count: usize,
    /// The responder's committed cluster incarnation, if it has one yet (`None` until the first
    /// leader commits `ClusterFormed`). A node holding a *different* non-`None` incarnation is in a
    /// separate cluster lineage; `run_cluster_guard` uses this to recognize a stale survivor.
    /// `#[serde(default)]` keeps the probe decodable from peers that predate the field.
    #[serde(default)]
    pub cluster_epoch: Option<u128>,
    /// Whether this binary can apply and enforce failover semantics V2. Missing means legacy.
    #[serde(default)]
    pub supports_failover_semantics_v2: bool,
    /// Canonical identity of the responder's cluster-wide settings. Missing means a legacy peer.
    #[serde(default)]
    pub config_fingerprint: Option<ClusterConfigFingerprint>,
    /// Whether this binary can enforce configuration identity on Raft streams.
    #[serde(default)]
    pub supports_config_identity_v1: bool,
    /// Whether the replicated cluster state now requires every peer to advertise an identity.
    #[serde(default)]
    pub config_identity_enforced: bool,
}

/// Concrete mismatches are always rejected. A missing identity is accepted only during a rolling
/// upgrade, before the cluster commits identity enforcement.
#[must_use]
pub fn config_identity_compatible(
    local: ClusterConfigFingerprint,
    peer: Option<ClusterConfigFingerprint>,
    enforcement_active: bool,
) -> bool {
    match peer {
        Some(peer) => local == peer,
        None => !enforcement_active,
    }
}

/// Size of the largest exact non-local identity group. Distinct foreign fingerprints never add
/// together: only one coherent alternative configuration can prove this node is the minority.
#[must_use]
pub fn largest_foreign_identity_group(
    local: ClusterConfigFingerprint,
    identities: impl IntoIterator<Item = Option<ClusterConfigFingerprint>>,
) -> usize {
    let mut groups = BTreeMap::<ClusterConfigFingerprint, usize>::new();
    for identity in identities
        .into_iter()
        .flatten()
        .filter(|value| *value != local)
    {
        *groups.entry(identity).or_default() += 1;
    }
    groups.values().copied().max().unwrap_or(0)
}

/// Size of the largest exact non-local cluster-incarnation group. A reset is justified only by
/// one coherent replacement cluster; unrelated foreign epochs must never add into a quorum (#31).
#[must_use]
pub fn largest_foreign_epoch_group(
    local: Option<u128>,
    epochs: impl IntoIterator<Item = Option<u128>>,
) -> usize {
    let Some(local) = local else {
        return 0;
    };
    let mut groups = BTreeMap::<u128, usize>::new();
    for epoch in epochs.into_iter().flatten().filter(|value| *value != local) {
        *groups.entry(epoch).or_default() += 1;
    }
    groups.values().copied().max().unwrap_or(0)
}

/// Consecutive-round hold-down for a quorum observation. A below-majority round resets progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsecutiveMajority {
    strikes: u32,
    required: u32,
}

impl ConsecutiveMajority {
    #[must_use]
    pub const fn new(required: u32) -> Self {
        Self {
            strikes: 0,
            required,
        }
    }

    /// Record one round and return true once the configured hold-down has been satisfied.
    pub fn observe(&mut self, largest_group: usize, majority: usize) -> bool {
        if largest_group >= majority {
            self.strikes = self.strikes.saturating_add(1);
        } else {
            self.strikes = 0;
        }
        self.strikes >= self.required
    }

    #[must_use]
    pub const fn strikes(&self) -> u32 {
        self.strikes
    }
}

impl ClusterStatusResponse {
    /// True when the responder reports a *concrete* incarnation that differs from `mine`. A `None`
    /// responder (blank / pre-first-commit) is never "foreign": it may still be mid-formation and
    /// will be absorbed by replication. Comparing two `None`s is likewise not foreign.
    #[must_use]
    pub fn reports_foreign_epoch(&self, mine: Option<u128>) -> bool {
        match (mine, self.cluster_epoch) {
            (Some(m), Some(theirs)) => m != theirs,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfigFingerprint;

    fn fingerprint(byte: u8) -> ClusterConfigFingerprint {
        ClusterConfigFingerprint {
            version: 1,
            digest: [byte; 32],
        }
    }

    #[derive(Deserialize)]
    struct LegacyStatusRequest {
        probe_from: u64,
    }

    #[test]
    fn concrete_config_mismatch_is_never_compatible() {
        assert!(config_identity_compatible(
            fingerprint(1),
            Some(fingerprint(1)),
            false
        ));
        assert!(!config_identity_compatible(
            fingerprint(1),
            Some(fingerprint(2)),
            false
        ));
        assert!(!config_identity_compatible(
            fingerprint(1),
            Some(ClusterConfigFingerprint {
                version: 2,
                digest: [1; 32],
            }),
            false
        ));
    }

    #[test]
    fn missing_legacy_identity_is_allowed_only_before_activation() {
        assert!(config_identity_compatible(fingerprint(1), None, false));
        assert!(!config_identity_compatible(fingerprint(1), None, true));
    }

    #[test]
    fn status_request_roundtrip() {
        let req = ClusterStatusRequest {
            probe_from: 7,
            config_fingerprint: Some(fingerprint(1)),
            supports_cancellation_safe_rpc_v1: true,
        };
        let v = serde_json::to_vec(&req).unwrap();
        assert_eq!(
            serde_json::from_slice::<ClusterStatusRequest>(&v).unwrap(),
            req
        );
        assert_eq!(
            serde_json::from_slice::<LegacyStatusRequest>(&v)
                .unwrap()
                .probe_from,
            7
        );
        let legacy: ClusterStatusRequest = serde_json::from_str(r#"{"probe_from":7}"#).unwrap();
        assert_eq!(legacy.config_fingerprint, None);
        assert!(!legacy.supports_cancellation_safe_rpc_v1);
    }

    #[test]
    fn status_response_roundtrip_preserves_protocol_fields() {
        let resp = ClusterStatusResponse {
            replica: None,
            supports_pre_vote: true,
            initialized: true,
            current_leader: Some(crate::raft::types::test_replica(2)),
            member_count: 3,
            cluster_epoch: Some(42),
            supports_failover_semantics_v2: true,
            config_fingerprint: Some(fingerprint(1)),
            supports_config_identity_v1: true,
            config_identity_enforced: true,
        };
        let v = serde_json::to_vec(&resp).unwrap();
        assert_eq!(
            serde_json::from_slice::<ClusterStatusResponse>(&v).unwrap(),
            resp
        );

        let blank = ClusterStatusResponse {
            replica: None,
            supports_pre_vote: true,
            initialized: false,
            current_leader: None,
            member_count: 0,
            cluster_epoch: None,
            supports_failover_semantics_v2: true,
            config_fingerprint: Some(fingerprint(1)),
            supports_config_identity_v1: true,
            config_identity_enforced: false,
        };
        assert_eq!(
            serde_json::from_slice::<ClusterStatusResponse>(&serde_json::to_vec(&blank).unwrap())
                .unwrap(),
            blank
        );

        // A probe from a peer that predates `cluster_epoch` (key absent) still decodes, as `None`.
        let legacy: ClusterStatusResponse =
            serde_json::from_str(r#"{"initialized":true,"current_leader":null,"member_count":3}"#)
                .unwrap();
        assert_eq!(legacy.cluster_epoch, None);
        assert!(!legacy.supports_failover_semantics_v2);
        assert_eq!(legacy.config_fingerprint, None);
        assert!(!legacy.supports_config_identity_v1);
        assert!(!legacy.config_identity_enforced);
    }

    #[test]
    fn reports_foreign_epoch_only_when_both_concrete_and_different() {
        let with = |e| ClusterStatusResponse {
            replica: None,
            supports_pre_vote: true,
            initialized: true,
            current_leader: Some(crate::raft::types::test_replica(1)),
            member_count: 3,
            cluster_epoch: e,
            supports_failover_semantics_v2: true,
            config_fingerprint: Some(fingerprint(1)),
            supports_config_identity_v1: true,
            config_identity_enforced: true,
        };
        // Different concrete incarnations -> foreign.
        assert!(with(Some(2)).reports_foreign_epoch(Some(1)));
        // Same incarnation -> not foreign.
        assert!(!with(Some(1)).reports_foreign_epoch(Some(1)));
        // Either side blank -> never foreign (peer may be mid-formation; we may be blank).
        assert!(!with(None).reports_foreign_epoch(Some(1)));
        assert!(!with(Some(2)).reports_foreign_epoch(None));
        assert!(!with(None).reports_foreign_epoch(None));
    }

    #[test]
    fn foreign_majority_requires_one_exact_identity_group() {
        let local = fingerprint(1);
        assert_eq!(
            largest_foreign_identity_group(local, [Some(fingerprint(2)), Some(fingerprint(2))]),
            2
        );
        assert_eq!(
            largest_foreign_identity_group(local, [Some(fingerprint(2)), Some(fingerprint(3))]),
            1
        );
        assert_eq!(
            largest_foreign_identity_group(local, [Some(local), None, Some(fingerprint(2))]),
            1
        );
    }

    #[test]
    fn foreign_epoch_majority_requires_one_exact_incarnation() {
        assert_eq!(
            largest_foreign_epoch_group(Some(1), [Some(2), Some(2), Some(3)]),
            2
        );
        assert_eq!(
            largest_foreign_epoch_group(Some(1), [Some(2), Some(2), Some(2)]),
            3
        );
        assert_eq!(
            largest_foreign_epoch_group(Some(1), [Some(1), None, Some(2)]),
            1
        );
        assert_eq!(
            largest_foreign_epoch_group(None, [Some(2), Some(2), Some(2)]),
            0
        );
    }

    #[test]
    fn foreign_majority_requires_consecutive_coherent_quorum_rounds() {
        let mut guard = ConsecutiveMajority::new(3);
        assert!(!guard.observe(1, 2));
        assert!(!guard.observe(2, 2));
        assert!(!guard.observe(2, 2));
        assert!(guard.observe(2, 2));

        assert!(!guard.observe(1, 2));
        assert_eq!(guard.strikes(), 0);
    }

    #[test]
    fn raft_frame_does_not_parse_as_probe_request() {
        // A Vote frame must not be mistaken for a ClusterStatusRequest (missing `probe_from`).
        use super::super::types::TypeConfig;
        use openraft::alias::VoteOf;
        use openraft::raft::VoteRequest;
        let vote: VoteRequest<TypeConfig> = VoteRequest::new(
            VoteOf::<TypeConfig>::new(1, crate::raft::types::test_replica(1)),
            None,
        );
        let v = serde_json::to_vec(&vote).unwrap();
        assert!(serde_json::from_slice::<ClusterStatusRequest>(&v).is_err());
        // And a probe must not be mistaken for a Vote frame.
        let req = ClusterStatusRequest {
            probe_from: 1,
            config_fingerprint: Some(fingerprint(1)),
            supports_cancellation_safe_rpc_v1: true,
        };
        let pv = serde_json::to_vec(&req).unwrap();
        assert!(serde_json::from_slice::<VoteRequest<TypeConfig>>(&pv).is_err());
    }
}
