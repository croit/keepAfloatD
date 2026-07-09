//! Cluster-status probe used for automatic cluster formation.
//!
//! On startup a node asks its peers whether a cluster already exists. If none does and a majority
//! of the roster is reachable, the node calls `Raft::initialize` with the full, cluster-wide
//! identical membership. OpenRaft documents concurrent `initialize` with the *same* config as
//! safe, so **every** node may do this and Raft elects a single leader among the reachable
//! majority — no node is special, so any majority can form (or recover) the cluster even if the
//! lowest-id node is permanently down. If a cluster already exists, the node joins via replication
//! instead. See [`crate::raft::auto_form_cluster`] for the orchestration and the safety model.
//!
//! The probe travels over the same TCP transport and handshake (node-id + `cluster_secret` +
//! peer-id gate) as Raft RPCs, so it is authenticated identically. The request/response structs
//! carry **required** fields so they cannot be confused with the OpenRaft RPC frames by the
//! try-all-deserialization dispatcher in [`super::network`].

use serde::{Deserialize, Serialize};

/// Asks a peer to report whether it is already part of a formed cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterStatusRequest {
    /// `node_id` of the asking node (diagnostic / symmetry with the handshake).
    pub probe_from: u64,
}

/// A peer's answer to [`ClusterStatusRequest`], computed from its local Raft state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

    #[test]
    fn status_request_roundtrip() {
        let req = ClusterStatusRequest { probe_from: 7 };
        let v = serde_json::to_vec(&req).unwrap();
        assert_eq!(
            serde_json::from_slice::<ClusterStatusRequest>(&v).unwrap(),
            req
        );
    }

    #[test]
    fn status_response_roundtrip_and_predicate() {
        let resp = ClusterStatusResponse {
            initialized: true,
            current_leader: Some(2),
            member_count: 3,
            cluster_epoch: Some(42),
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
        };
        assert!(!blank.indicates_existing_cluster());

        // A probe from a peer that predates `cluster_epoch` (key absent) still decodes, as `None`.
        let legacy: ClusterStatusResponse =
            serde_json::from_str(r#"{"initialized":true,"current_leader":2,"member_count":3}"#)
                .unwrap();
        assert_eq!(legacy.cluster_epoch, None);
    }

    #[test]
    fn reports_foreign_epoch_only_when_both_concrete_and_different() {
        let with = |e| ClusterStatusResponse {
            initialized: true,
            current_leader: Some(1),
            member_count: 3,
            cluster_epoch: e,
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
        };
        assert!(leader_only.indicates_existing_cluster());
        let initialized_no_leader = ClusterStatusResponse {
            initialized: true,
            current_leader: None,
            member_count: 3,
            cluster_epoch: None,
        };
        assert!(initialized_no_leader.indicates_existing_cluster());
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
        let req = ClusterStatusRequest { probe_from: 1 };
        let pv = serde_json::to_vec(&req).unwrap();
        assert!(serde_json::from_slice::<VoteRequest<TypeConfig>>(&pv).is_err());
    }
}
