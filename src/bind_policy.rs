//! Pure VIP bind decision mirrored from the daemon reconciliation loop (`vip::run_reconcile_loop`).
//!
//! A node may bind only if all of the following are true:
//! - Raft still reports a current leader,
//! - the local health probe is green,
//! - the node can still submit to consensus (`consensus_fresh`),
//! - committed assignment state names this node as the holder,
//! - the assignment's activation tick has passed,
//! - and, for a first local activation, either the previous holder has released the generation or
//!   is currently ineligible. Once this process safely activates the assigned VIP, later recovery
//!   of that previous holder does not revoke the activation.
//!
//! That last pair of fences is what removes the old "two healthy nodes with different applied
//! views both bind" failure mode.

use std::collections::BTreeMap;

use crate::raft::store::{VipAssignment, is_node_probe_fresh};

/// Process-local preconditions that must all hold before any committed assignment is honored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BindGates {
    /// Raft currently reports a leader.
    pub(crate) has_leader: bool,
    /// The local health probe is green.
    pub(crate) local_healthy: bool,
    /// This node can still submit to consensus.
    pub(crate) consensus_fresh: bool,
}

impl BindGates {
    #[must_use]
    pub(crate) const fn all_open(self) -> bool {
        self.has_leader && self.local_healthy && self.consensus_fresh
    }
}

#[cfg(test)]
use std::net::IpAddr;

/// Whether **this** node should attempt to attach the VIP described by `assignment`.
#[must_use]
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) fn should_bind_vip(
    has_leader: bool,
    local_healthy: bool,
    consensus_fresh: bool,
    node_id: u64,
    assignment: Option<&VipAssignment>,
    node_probe_ticks: &BTreeMap<u64, u64>,
    latest_probe_tick: u64,
    stale_missed_probes: u64,
) -> bool {
    should_bind_or_keep_vip(
        BindGates {
            has_leader,
            local_healthy,
            consensus_fresh,
        },
        node_id,
        assignment,
        node_probe_ticks,
        latest_probe_tick,
        stale_missed_probes,
        None,
    )
}

/// Apply the bind policy while preserving an already-safe local activation of the same assignment
/// generation. The activation proof never bypasses leader, health, freshness, holder, or tick
/// gates.
#[must_use]
pub(crate) fn should_bind_or_keep_vip(
    gates: BindGates,
    node_id: u64,
    assignment: Option<&VipAssignment>,
    node_probe_ticks: &BTreeMap<u64, u64>,
    latest_probe_tick: u64,
    stale_missed_probes: u64,
    activated_generation: Option<u64>,
) -> bool {
    if !gates.all_open() {
        return false;
    }

    let Some(assignment) = assignment else {
        return false;
    };

    if assignment.holder != node_id || latest_probe_tick < assignment.activation_tick {
        return false;
    }

    if activated_generation == Some(assignment.generation) {
        return true;
    }

    match assignment.previous_holder {
        None => true,
        Some(_) if assignment.previous_holder_released => true,
        Some(previous_holder) => !is_node_probe_fresh(
            previous_holder,
            node_probe_ticks,
            latest_probe_tick,
            stale_missed_probes,
        ),
    }
}

/// Count cluster members that would bind `vip` if every node sees the same committed assignment.
#[cfg(test)]
#[must_use]
#[allow(clippy::too_many_arguments)]
pub(crate) fn binder_count_for_vip(
    has_leader: bool,
    vip: IpAddr,
    assignments: &BTreeMap<IpAddr, VipAssignment>,
    peer_ids_sorted: &[u64],
    local_healthy_per_node: &BTreeMap<u64, bool>,
    consensus_fresh_per_node: &BTreeMap<u64, bool>,
    node_probe_ticks: &BTreeMap<u64, u64>,
    latest_probe_tick: u64,
    stale_missed_probes: u64,
) -> usize {
    let assignment = assignments.get(&vip);
    peer_ids_sorted
        .iter()
        .filter(|&&nid| {
            let local_ok = *local_healthy_per_node.get(&nid).unwrap_or(&false);
            let consensus_ok = *consensus_fresh_per_node.get(&nid).unwrap_or(&false);
            should_bind_vip(
                has_leader,
                local_ok,
                consensus_ok,
                nid,
                assignment,
                node_probe_ticks,
                latest_probe_tick,
                stale_missed_probes,
            )
        })
        .count()
}

/// Like [`binder_count_for_vip`] but each node uses its own local committed assignment view.
#[cfg(test)]
#[must_use]
#[allow(clippy::too_many_arguments)]
pub(crate) fn binder_count_for_vip_per_node_view(
    has_leader: bool,
    vip: IpAddr,
    per_node_assignments: &BTreeMap<u64, BTreeMap<IpAddr, VipAssignment>>,
    peer_ids_sorted: &[u64],
    local_healthy_per_node: &BTreeMap<u64, bool>,
    consensus_fresh_per_node: &BTreeMap<u64, bool>,
    node_probe_ticks: &BTreeMap<u64, u64>,
    latest_probe_tick: u64,
    stale_missed_probes: u64,
) -> usize {
    peer_ids_sorted
        .iter()
        .filter(|&&nid| {
            let local_ok = *local_healthy_per_node.get(&nid).unwrap_or(&false);
            let consensus_ok = *consensus_fresh_per_node.get(&nid).unwrap_or(&false);
            let assignment = per_node_assignments.get(&nid).and_then(|m| m.get(&vip));
            should_bind_vip(
                has_leader,
                local_ok,
                consensus_ok,
                nid,
                assignment,
                node_probe_ticks,
                latest_probe_tick,
                stale_missed_probes,
            )
        })
        .count()
}

#[cfg(test)]
mod tests {
    use super::{
        BindGates, binder_count_for_vip, binder_count_for_vip_per_node_view,
        should_bind_or_keep_vip, should_bind_vip,
    };
    use crate::config::VipAddr;
    use crate::raft::store::{
        EligibilityInputs, VipAssignment, recompute_vip_holder, reconcile_vip_assignments,
    };
    use crate::raft::types::{TypeConfig, test_replica};
    use openraft::alias::StoredMembershipOf;
    use openraft::{BasicNode, Membership};
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::{IpAddr, Ipv4Addr};

    fn ip(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    fn assignment(holder: u64) -> VipAssignment {
        VipAssignment {
            holder,
            generation: 1,
            previous_holder: None,
            previous_holder_released: true,
            activation_tick: 5,
        }
    }

    #[test]
    fn failure_no_leader_prevents_any_bind_even_if_health_and_holder_match() {
        let assignments = BTreeMap::from([(ip(10, 0, 0, 5), assignment(1))]);
        let node_ticks = BTreeMap::from([(1_u64, 5_u64)]);
        let local = BTreeMap::from([(1_u64, true)]);
        let consensus = BTreeMap::from([(1_u64, true)]);
        assert_eq!(
            binder_count_for_vip(
                false,
                ip(10, 0, 0, 5),
                &assignments,
                &[1],
                &local,
                &consensus,
                &node_ticks,
                5,
                3
            ),
            0
        );
    }

    #[test]
    fn failure_local_unhealthy_prevents_bind_when_this_node_is_holder() {
        let node_ticks = BTreeMap::from([(1_u64, 5_u64)]);
        assert!(!should_bind_vip(
            true,
            false,
            true,
            1,
            Some(&assignment(1)),
            &node_ticks,
            5,
            3
        ));
    }

    #[test]
    fn failure_consensus_not_fresh_prevents_bind() {
        let node_ticks = BTreeMap::from([(1_u64, 5_u64)]);
        assert!(!should_bind_vip(
            true,
            true,
            false,
            1,
            Some(&assignment(1)),
            &node_ticks,
            5,
            3
        ));
    }

    #[test]
    fn failure_holder_mismatch_even_if_healthy() {
        let node_ticks = BTreeMap::from([(1_u64, 5_u64), (2, 5)]);
        assert!(!should_bind_vip(
            true,
            true,
            true,
            2,
            Some(&assignment(1)),
            &node_ticks,
            5,
            3
        ));
    }

    #[test]
    fn failure_previous_holder_still_eligible_and_unreleased_blocks_replacement() {
        let assignment = VipAssignment {
            holder: 2,
            generation: 2,
            previous_holder: Some(1),
            previous_holder_released: false,
            activation_tick: 5,
        };
        let node_ticks = BTreeMap::from([(1_u64, 5_u64), (2, 5)]);
        assert!(!should_bind_vip(
            true,
            true,
            true,
            2,
            Some(&assignment),
            &node_ticks,
            5,
            3
        ));
    }

    #[test]
    fn failed_unbind_from_fresh_unhealthy_holder_blocks_replacement() {
        let assignment = VipAssignment {
            holder: 2,
            generation: 2,
            previous_holder: Some(1),
            previous_holder_released: false,
            activation_tick: 5,
        };
        let node_ticks = BTreeMap::from([(1_u64, 5_u64), (2, 5)]);

        assert!(!should_bind_vip(
            true,
            true,
            true,
            2,
            Some(&assignment),
            &node_ticks,
            5,
            3
        ));
    }

    #[test]
    fn active_handoff_stays_bound_when_previous_holder_recovers() {
        let assignment = VipAssignment {
            holder: 2,
            generation: 2,
            previous_holder: Some(1),
            previous_holder_released: false,
            activation_tick: 5,
        };
        let node_ticks = BTreeMap::from([(1_u64, 5_u64), (2, 5)]);
        assert!(should_bind_or_keep_vip(
            BindGates {
                has_leader: true,
                local_healthy: true,
                consensus_fresh: true
            },
            2,
            Some(&assignment),
            &node_ticks,
            5,
            3,
            Some(2)
        ));
    }

    #[test]
    fn old_activation_generation_cannot_bypass_a_new_handoff_fence() {
        let assignment = VipAssignment {
            holder: 2,
            generation: 2,
            previous_holder: Some(1),
            previous_holder_released: false,
            activation_tick: 5,
        };
        let node_ticks = BTreeMap::from([(1_u64, 5_u64), (2, 5)]);

        assert!(!should_bind_or_keep_vip(
            BindGates {
                has_leader: true,
                local_healthy: true,
                consensus_fresh: true
            },
            2,
            Some(&assignment),
            &node_ticks,
            5,
            3,
            Some(1)
        ));
    }

    #[test]
    fn active_handoff_cannot_bypass_mandatory_local_fences() {
        let assignment = VipAssignment {
            holder: 2,
            generation: 2,
            previous_holder: Some(1),
            previous_holder_released: false,
            activation_tick: 5,
        };
        let node_ticks = BTreeMap::from([(1_u64, 5_u64), (2, 5)]);
        for (has_leader, local_healthy, consensus_fresh, node_id, latest_tick) in [
            (false, true, true, 2, 5),
            (true, false, true, 2, 5),
            (true, true, false, 2, 5),
            (true, true, true, 3, 5),
            (true, true, true, 2, 4),
        ] {
            assert!(!should_bind_or_keep_vip(
                BindGates {
                    has_leader,
                    local_healthy,
                    consensus_fresh
                },
                node_id,
                Some(&assignment),
                &node_ticks,
                latest_tick,
                3,
                Some(2)
            ));
        }
    }

    #[test]
    fn success_release_ack_allows_replacement() {
        let assignment = VipAssignment {
            holder: 2,
            generation: 2,
            previous_holder: Some(1),
            previous_holder_released: true,
            activation_tick: 5,
        };
        let node_ticks = BTreeMap::from([(1_u64, 5_u64), (2, 5)]);
        assert!(should_bind_vip(
            true,
            true,
            true,
            2,
            Some(&assignment),
            &node_ticks,
            5,
            3
        ));
    }

    #[test]
    fn success_stale_previous_holder_allows_replacement_after_activation_tick() {
        let assignment = VipAssignment {
            holder: 2,
            generation: 2,
            previous_holder: Some(1),
            previous_holder_released: false,
            activation_tick: 6,
        };
        let node_ticks = BTreeMap::from([(1_u64, 1_u64), (2, 6)]);
        assert!(!should_bind_vip(
            true,
            true,
            true,
            2,
            Some(&assignment),
            &node_ticks,
            5,
            3
        ));
        assert!(should_bind_vip(
            true,
            true,
            true,
            2,
            Some(&assignment),
            &node_ticks,
            6,
            3
        ));
    }

    /// All combinations of leader/health/consensus masks with a fixed committed holder map must
    /// still produce at most one binder per VIP.
    #[test]
    fn exhaustive_two_vips_three_peers_fixed_assignment_map_at_most_one_binder() {
        let vips = vec![ip(10, 0, 0, 11), ip(10, 0, 0, 12)];
        let assignments = BTreeMap::from([(vips[0], assignment(2)), (vips[1], assignment(1))]);
        let peers = [1_u64, 2, 3];
        let node_ticks = BTreeMap::from([(1_u64, 5_u64), (2, 5), (3, 5)]);

        for has_leader in [false, true] {
            for local_mask in 0u8..(1 << 3) {
                for consensus_mask in 0u8..(1 << 3) {
                    let mut local = BTreeMap::new();
                    let mut consensus = BTreeMap::new();
                    for (i, p) in peers.iter().enumerate() {
                        local.insert(*p, (local_mask & (1 << i)) != 0);
                        consensus.insert(*p, (consensus_mask & (1 << i)) != 0);
                    }
                    for vip in &vips {
                        let n = binder_count_for_vip(
                            has_leader,
                            *vip,
                            &assignments,
                            &peers,
                            &local,
                            &consensus,
                            &node_ticks,
                            5,
                            3,
                        );
                        assert!(n <= 1);
                    }
                }
            }
        }
    }

    /// Full pipeline: Raft-derived assignments plus fencing metadata still produce an exclusive
    /// binder set.
    #[test]
    fn recompute_health_masks_three_members_exclusive_bind_under_leader() {
        let m = Membership::new(
            vec![BTreeSet::from([
                test_replica(1),
                test_replica(2),
                test_replica(3),
            ])],
            BTreeMap::from([
                (test_replica(1), BasicNode::default()),
                (test_replica(2), BasicNode::default()),
                (test_replica(3), BasicNode::default()),
            ]),
        )
        .unwrap();
        let membership = StoredMembershipOf::<TypeConfig>::new(None, m);
        let vip_list = vec![
            (VipAddr::host(ip(10, 0, 0, 101)), "eth0".into()),
            (VipAddr::host(ip(10, 0, 0, 102)), "eth0".into()),
            (VipAddr::host(ip(10, 0, 0, 103)), "eth0".into()),
        ];
        let peers_vec = vec![1_u64, 2, 3];
        let local = BTreeMap::from([(1_u64, true), (2, true), (3, true)]);
        let consensus = BTreeMap::from([(1_u64, true), (2, true), (3, true)]);

        for mask in 0u8..(1 << 3) {
            let mut node_health = BTreeMap::new();
            let mut node_ticks = BTreeMap::new();
            for i in 0..3_u64 {
                node_health.insert(i + 1, (mask & (1 << i)) != 0);
                node_ticks.insert(i + 1, 4_u64);
            }
            let mut vip_holder = BTreeMap::new();
            recompute_vip_holder(
                &membership,
                &EligibilityInputs {
                    node_health: &node_health,
                    node_probe_ticks: &node_ticks,
                    latest_probe_tick: 4,
                    stale_missed_probes: 3,
                    failback_delay_ticks: 0,
                    node_recovery_tick: &BTreeMap::new(),
                },
                &BTreeSet::new(),
                &vip_list,
                &BTreeMap::new(),
                &mut vip_holder,
            );
            let mut assignments = BTreeMap::new();
            let mut generations = BTreeMap::new();
            reconcile_vip_assignments(
                &vip_holder,
                4,
                &vip_list,
                &mut assignments,
                &mut generations,
                &mut BTreeMap::new(),
            );
            for (vip, _) in &vip_list {
                let cnt = binder_count_for_vip(
                    true,
                    vip.addr,
                    &assignments,
                    &peers_vec,
                    &local,
                    &consensus,
                    &node_ticks,
                    4,
                    3,
                );
                assert!(
                    cnt <= 1,
                    "mask={mask} vip={vip:?} assignments={assignments:?}"
                );
            }
        }
    }

    /// Divergent applied-index scenario: old holder still sees generation N, replacement already
    /// sees generation N+1. The replacement must remain blocked until the previous holder is no
    /// longer eligible or has committed a matching release ack.
    #[test]
    fn divergent_applied_index_release_gate_preserves_at_most_one_binder() {
        let vip = ip(10, 0, 0, 7);
        let peers = [1_u64, 2];
        let mut node1_view = BTreeMap::new();
        node1_view.insert(vip, assignment(1));
        let mut node2_view = BTreeMap::new();
        node2_view.insert(
            vip,
            VipAssignment {
                holder: 2,
                generation: 2,
                previous_holder: Some(1),
                previous_holder_released: false,
                activation_tick: 5,
            },
        );
        let per_node = BTreeMap::from([(1_u64, node1_view), (2, node2_view)]);
        let local = BTreeMap::from([(1_u64, true), (2, true)]);
        let consensus = BTreeMap::from([(1_u64, true), (2, true)]);
        let node_ticks = BTreeMap::from([(1_u64, 5_u64), (2, 5)]);

        let cnt = binder_count_for_vip_per_node_view(
            true,
            vip,
            &per_node,
            &peers,
            &local,
            &consensus,
            &node_ticks,
            5,
            3,
        );
        assert_eq!(cnt, 1);
    }

    #[test]
    fn activation_tick_boundary_gates_first_bind() {
        // No previous holder, so the activation tick is the only remaining fence.
        let a = assignment(1); // previous_holder: None, activation_tick: 5
        let ticks = BTreeMap::from([(1_u64, 5_u64)]);
        // One round before activation: blocked.
        assert!(!should_bind_vip(
            true,
            true,
            true,
            1,
            Some(&a),
            &ticks,
            4,
            3
        ));
        // Exactly at activation: allowed.
        assert!(should_bind_vip(true, true, true, 1, Some(&a), &ticks, 5, 3));
    }

    #[test]
    fn previous_holder_staleness_boundary_gates_replacement() {
        // Replacement (node 2) takes over from node 1, which has not acked a release.
        let a = VipAssignment {
            holder: 2,
            generation: 2,
            previous_holder: Some(1),
            previous_holder_released: false,
            activation_tick: 0,
        };
        // At the stale threshold, node 1 remains eligible and blocks replacement.
        let at_threshold = BTreeMap::from([(1_u64, 7_u64), (2, 10)]);
        assert!(!should_bind_vip(
            true,
            true,
            true,
            2,
            Some(&a),
            &at_threshold,
            10,
            3
        ));
        // One round past the threshold, node 1 is ineligible and replacement is allowed.
        let past_threshold = BTreeMap::from([(1_u64, 6_u64), (2, 10)]);
        assert!(should_bind_vip(
            true,
            true,
            true,
            2,
            Some(&a),
            &past_threshold,
            10,
            3
        ));
    }

    #[test]
    fn node_holding_two_vips_binds_both_when_fresh_and_neither_when_consensus_stale() {
        let a1 = assignment(1);
        let a2 = assignment(1);
        let ticks = BTreeMap::from([(1_u64, 5_u64)]);
        // A healthy, fresh holder with a fresh leader binds both of its VIPs.
        assert!(should_bind_vip(
            true,
            true,
            true,
            1,
            Some(&a1),
            &ticks,
            5,
            3
        ));
        assert!(should_bind_vip(
            true,
            true,
            true,
            1,
            Some(&a2),
            &ticks,
            5,
            3
        ));
        // Losing consensus freshness prevents both binds.
        assert!(!should_bind_vip(
            true,
            true,
            false,
            1,
            Some(&a1),
            &ticks,
            5,
            3
        ));
        assert!(!should_bind_vip(
            true,
            true,
            false,
            1,
            Some(&a2),
            &ticks,
            5,
            3
        ));
    }
}
