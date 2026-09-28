//! Cluster-status probe used for automatic cluster formation.
//!
//! On startup a node asks its peers whether a cluster already exists. If none does and a majority
//! of the roster is reachable, the node calls `Raft::initialize` with the full, cluster-wide
//! identical membership. OpenRaft documents concurrent `initialize` with the *same* config as
//! safe, so **every** node may do this and Raft elects a single leader among the reachable
//! majority - no node is special, so any majority can form (or recover) the cluster even if the
//! lowest-id node is permanently down. If a cluster already exists, the node joins via replication
//! instead. See [`crate::raft::start_raft`] for the orchestration and the safety model.
//!
//! The probe travels over the same TCP transport and handshake (node-id + `cluster_secret` +
//! peer-id gate) as Raft RPCs, so it is authenticated identically. The request/response structs
//! carry **required** fields so they cannot be confused with the OpenRaft RPC frames by the
//! try-all-deserialization dispatcher in [`super::network`].

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
    /// True once `Raft::initialize` (or replicated membership) has taken effect locally.
    pub initialized: bool,
    /// Current leader as seen by the responder, if any.
    pub current_leader: Option<u64>,
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

/// Whether a fresh formation may select V2 without waiting for every configured voter.
///
/// A reachable majority is required, and every reachable peer must support V2. Offline legacy
/// peers are fenced when they return; an online legacy peer keeps a fresh formation on legacy.
#[must_use]
pub fn fresh_formation_supports_v2(
    total_voters: usize,
    reachable_voters: usize,
    every_reachable_supports_v2: bool,
) -> bool {
    reachable_voters > total_voters / 2 && every_reachable_supports_v2
}

/// Whether an existing cluster can safely commit the V2-only activation command.
#[must_use]
pub fn all_voters_support_v2(
    total_voters: usize,
    reachable_voters: usize,
    every_reachable_supports_v2: bool,
) -> bool {
    reachable_voters == total_voters && every_reachable_supports_v2
}

/// Whether a fresh formation may require config identity without fencing its reachable quorum.
#[must_use]
pub fn fresh_formation_supports_config_identity(
    total_voters: usize,
    reachable_voters: usize,
    every_reachable_has_matching_identity: bool,
) -> bool {
    reachable_voters > total_voters / 2 && every_reachable_has_matching_identity
}

/// Whether an existing cluster can commit the identity-only activation command.
#[must_use]
pub fn all_voters_support_config_identity(
    total_voters: usize,
    reachable_voters: usize,
    every_reachable_has_matching_identity: bool,
) -> bool {
    reachable_voters == total_voters && every_reachable_has_matching_identity
}

/// Whether this response is a compatible, uninitialized voter that may count toward a new
/// cluster. Legacy responses are deliberately excluded because their configuration is unknown.
#[must_use]
pub fn counts_for_cold_formation(
    response: &ClusterStatusResponse,
    local: ClusterConfigFingerprint,
) -> bool {
    !response.indicates_existing_cluster() && response.config_fingerprint == Some(local)
}

/// Whether a blank node may join an existing cluster during the rolling-upgrade window.
/// A missing legacy identity remains joinable, while a concrete mismatch is always fenced.
#[must_use]
pub fn can_join_existing_cluster(
    response: &ClusterStatusResponse,
    local: ClusterConfigFingerprint,
) -> bool {
    response.indicates_existing_cluster() && !response.reports_foreign_config(local)
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
    /// True when the responder is part of an existing cluster (initialized or already sees a
    /// leader). A `true` here means the asking node must **not** form a new cluster and should
    /// instead join as a follower via normal replication.
    #[must_use]
    pub fn indicates_existing_cluster(&self) -> bool {
        self.initialized || self.current_leader.is_some()
    }

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

    /// True for a concrete, version-aware config identity mismatch.
    #[must_use]
    pub fn reports_foreign_config(&self, mine: ClusterConfigFingerprint) -> bool {
        self.config_fingerprint.is_some_and(|theirs| theirs != mine)
    }
}

/// The action a node takes after one probe round during formation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormationDecision {
    /// An existing cluster was found; join via replication instead of initializing.
    Join,
    /// A majority responded uninitialized; safe to call `Raft::initialize`.
    Form,
    /// Not enough peers reachable yet; keep probing (never form a sub-majority cluster).
    Wait,
}

/// Decide the next formation action from one probe round's results.
///
/// `total_peers` is the full roster size (including self). `reachable_uninit` counts this node
/// itself plus every peer that answered and reported no existing cluster. `found_existing` is true
/// when any peer reported an existing cluster.
#[must_use]
pub fn formation_decision(
    total_peers: usize,
    reachable_uninit: usize,
    found_existing: bool,
) -> FormationDecision {
    if found_existing {
        return FormationDecision::Join;
    }
    let majority = total_peers / 2 + 1;
    if reachable_uninit >= majority {
        FormationDecision::Form
    } else {
        FormationDecision::Wait
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
    fn status_response_roundtrip_and_predicate() {
        let resp = ClusterStatusResponse {
            initialized: true,
            current_leader: Some(2),
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
        assert!(resp.indicates_existing_cluster());

        let blank = ClusterStatusResponse {
            initialized: false,
            current_leader: None,
            member_count: 0,
            cluster_epoch: None,
            supports_failover_semantics_v2: true,
            config_fingerprint: Some(fingerprint(1)),
            supports_config_identity_v1: true,
            config_identity_enforced: false,
        };
        assert!(!blank.indicates_existing_cluster());

        // A probe from a peer that predates `cluster_epoch` (key absent) still decodes, as `None`.
        let legacy: ClusterStatusResponse =
            serde_json::from_str(r#"{"initialized":true,"current_leader":2,"member_count":3}"#)
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
            initialized: true,
            current_leader: Some(1),
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
    fn reports_foreign_config_only_for_a_concrete_difference() {
        let mut response = ClusterStatusResponse {
            config_fingerprint: Some(fingerprint(1)),
            ..ClusterStatusResponse::default()
        };
        assert!(!response.reports_foreign_config(fingerprint(1)));
        assert!(response.reports_foreign_config(fingerprint(2)));
        response.config_fingerprint = None;
        assert!(!response.reports_foreign_config(fingerprint(1)));
    }

    #[test]
    fn cold_formation_counts_only_matching_concrete_identities() {
        let local = fingerprint(1);
        let matching_blank = ClusterStatusResponse {
            config_fingerprint: Some(local),
            ..ClusterStatusResponse::default()
        };
        assert!(counts_for_cold_formation(&matching_blank, local));

        let legacy_blank = ClusterStatusResponse::default();
        assert!(!counts_for_cold_formation(&legacy_blank, local));

        let foreign_blank = ClusterStatusResponse {
            config_fingerprint: Some(fingerprint(2)),
            ..ClusterStatusResponse::default()
        };
        assert!(!counts_for_cold_formation(&foreign_blank, local));
    }

    #[test]
    fn existing_legacy_cluster_remains_joinable_during_rollout() {
        let existing_legacy = ClusterStatusResponse {
            initialized: true,
            ..ClusterStatusResponse::default()
        };
        assert!(can_join_existing_cluster(&existing_legacy, fingerprint(1)));

        let existing_foreign = ClusterStatusResponse {
            config_fingerprint: Some(fingerprint(2)),
            ..existing_legacy
        };
        assert!(!can_join_existing_cluster(
            &existing_foreign,
            fingerprint(1)
        ));
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
    fn formation_waits_below_majority_and_forms_at_majority() {
        // 3-node cluster: majority is 2.
        assert_eq!(formation_decision(3, 1, false), FormationDecision::Wait);
        assert_eq!(formation_decision(3, 2, false), FormationDecision::Form);
        assert_eq!(formation_decision(3, 3, false), FormationDecision::Form);
        // 5-node cluster: majority is 3.
        assert_eq!(formation_decision(5, 2, false), FormationDecision::Wait);
        assert_eq!(formation_decision(5, 3, false), FormationDecision::Form);
        // Single-node cluster forms immediately.
        assert_eq!(formation_decision(1, 1, false), FormationDecision::Form);
    }

    #[test]
    fn formation_joins_when_existing_cluster_found() {
        // An existing cluster always means join, regardless of how many answered uninitialized.
        assert_eq!(formation_decision(3, 1, true), FormationDecision::Join);
        assert_eq!(formation_decision(3, 3, true), FormationDecision::Join);
    }

    #[test]
    fn formation_majority_threshold_across_roster_sizes() {
        // For each roster size, Wait strictly below majority, Form at and above it.
        for total in 1..=7_usize {
            let majority = total / 2 + 1;
            for reachable in 0..=total {
                let decision = formation_decision(total, reachable, false);
                if reachable >= majority {
                    assert_eq!(
                        decision,
                        FormationDecision::Form,
                        "total={total} reachable={reachable} majority={majority}"
                    );
                } else {
                    assert_eq!(
                        decision,
                        FormationDecision::Wait,
                        "total={total} reachable={reachable} majority={majority}"
                    );
                }
            }
        }
    }

    #[test]
    fn formation_even_sized_rosters_need_strict_majority() {
        // 4-node: majority 3 (a 2-2 split must NOT form on either side).
        assert_eq!(formation_decision(4, 2, false), FormationDecision::Wait);
        assert_eq!(formation_decision(4, 3, false), FormationDecision::Form);
        // 6-node: majority 4.
        assert_eq!(formation_decision(6, 3, false), FormationDecision::Wait);
        assert_eq!(formation_decision(6, 4, false), FormationDecision::Form);
    }

    #[test]
    fn formation_existing_cluster_always_joins_regardless_of_reachability() {
        for total in 1..=7_usize {
            for reachable in 0..=total {
                assert_eq!(
                    formation_decision(total, reachable, true),
                    FormationDecision::Join,
                    "total={total} reachable={reachable}"
                );
            }
        }
    }

    #[test]
    fn leader_seen_but_uninitialized_still_indicates_existing_cluster() {
        let leader_only = ClusterStatusResponse {
            initialized: false,
            current_leader: Some(2),
            member_count: 0,
            cluster_epoch: None,
            supports_failover_semantics_v2: true,
            ..ClusterStatusResponse::default()
        };
        assert!(leader_only.indicates_existing_cluster());
        let initialized_no_leader = ClusterStatusResponse {
            initialized: true,
            current_leader: None,
            member_count: 3,
            cluster_epoch: None,
            supports_failover_semantics_v2: true,
            ..ClusterStatusResponse::default()
        };
        assert!(initialized_no_leader.indicates_existing_cluster());
    }

    #[test]
    fn fresh_v2_formation_requires_a_new_binary_majority_and_no_reachable_legacy_peer() {
        assert!(fresh_formation_supports_v2(3, 2, true));
        assert!(!fresh_formation_supports_v2(3, 1, true));
        assert!(!fresh_formation_supports_v2(3, 2, false));
        assert!(fresh_formation_supports_v2(1, 1, true));
    }

    #[test]
    fn existing_cluster_activation_requires_every_voter() {
        assert!(all_voters_support_v2(3, 3, true));
        assert!(!all_voters_support_v2(3, 2, true));
        assert!(!all_voters_support_v2(3, 3, false));
    }

    #[test]
    fn config_identity_activation_requires_matching_capability_on_the_required_quorum() {
        assert!(fresh_formation_supports_config_identity(3, 2, true));
        assert!(!fresh_formation_supports_config_identity(3, 1, true));
        assert!(!fresh_formation_supports_config_identity(3, 2, false));
        assert!(all_voters_support_config_identity(3, 3, true));
        assert!(!all_voters_support_config_identity(3, 2, true));
        assert!(!all_voters_support_config_identity(3, 3, false));
    }

    #[test]
    fn raft_frame_does_not_parse_as_probe_request() {
        // A Vote frame must not be mistaken for a ClusterStatusRequest (missing `probe_from`).
        use super::super::types::TypeConfig;
        use openraft::alias::VoteOf;
        use openraft::raft::VoteRequest;
        let vote: VoteRequest<TypeConfig> = VoteRequest::new(VoteOf::<TypeConfig>::new(1, 1), None);
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
