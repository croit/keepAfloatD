//! Pure, deterministic VIP eligibility and assignment logic.
//!
//! These functions take only plain data (health maps, probe ticks, membership, the VIP list) and
//! produce the holder map. No clocks, no RNG, no hash-map iteration-order dependence — every
//! iteration is over sorted structures — so all nodes and all replays agree. The state machine in
//! [`super::state_machine`] feeds committed state through here on every applied entry.

use super::super::types::TypeConfig;
use super::state::{OWNERSHIP_ACTIVATION_HOLDOFF_TICKS, VipAssignment};
use crate::config::VipAddr;
use openraft::alias::StoredMembershipOf;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::IpAddr;

/// Assign VIPs to holders in list order, round-robin over `eligible` (must be sorted by id).
///
/// Clears `out`. If `eligible` is empty, leaves `out` empty. Retained as the test oracle that
/// pins cold-start equivalence with [`assign_vips_minimal_movement`]; production assignment now
/// goes through the minimal-movement path, which subsumes this for the empty-prior case.
#[cfg(test)]
pub(crate) fn assign_vips_round_robin_eligible(
    eligible: &[u64],
    vip_addrs_in_order: &[IpAddr],
    out: &mut HashMap<IpAddr, u64>,
) {
    out.clear();
    if eligible.is_empty() {
        return;
    }
    for (i, vip) in vip_addrs_in_order.iter().enumerate() {
        let holder = eligible[i % eligible.len()];
        out.insert(*vip, holder);
    }
}

/// Extreme-load node, ties broken by the lowest id.
///
/// `load` is a `BTreeMap`, so it is iterated in ascending id order and the first node seen at the
/// extreme — the lowest id — wins. `keep_current` decides the extreme: `l >= bl` keeps the smaller
/// load (least-loaded), `l <= bl` keeps the larger (most-loaded). Returns `None` only for an empty
/// map.
fn extreme_loaded_node(
    load: &BTreeMap<u64, usize>,
    keep_current: impl Fn(usize, usize) -> bool,
) -> Option<(u64, usize)> {
    let mut best: Option<(u64, usize)> = None;
    for (&id, &l) in load {
        match best {
            Some((_, bl)) if keep_current(l, bl) => {}
            _ => best = Some((id, l)),
        }
    }
    best
}

/// Eligible node with the fewest assigned VIPs, ties broken by the lowest id.
fn least_loaded_node(load: &BTreeMap<u64, usize>) -> Option<(u64, usize)> {
    extreme_loaded_node(load, |l, bl| l >= bl)
}

/// Eligible node with the most assigned VIPs, ties broken by the lowest id.
fn most_loaded_node(load: &BTreeMap<u64, usize>) -> Option<(u64, usize)> {
    extreme_loaded_node(load, |l, bl| l <= bl)
}

/// Deterministic, stable, minimal-movement, load-balancing VIP assignment.
///
/// `eligible` must be sorted ascending by id. `current_holders` maps each VIP to its prior
/// committed holder; a VIP whose current holder is missing or no longer eligible is an "orphan"
/// that must be re-placed. Unlike plain round-robin, this keeps a VIP on its
/// current holder whenever that holder is still eligible, so a topology change moves the minimum
/// number of VIPs while still converging to a maximally even spread.
///
/// Three deterministic passes:
/// 1. keep every VIP whose holder is still eligible;
/// 2. place each orphan (in sorted order) on the least-loaded eligible node;
/// 3. rebalance until the load spread is at most one, each step moving the donor's highest-IP VIP.
///
/// Clears `out`. If `eligible` is empty, leaves `out` empty. Pure and deterministic: no clocks, no
/// RNG, and every iteration is over sorted structures (`BTreeMap`/`BTreeSet`/sorted `Vec`), so all
/// nodes and replays agree. Idempotent on an already balanced assignment (it then makes no moves).
///
/// With an empty `current_holders` (cold start) every VIP is an orphan placed in sorted order onto
/// loads seeded to zero, which is exactly round-robin over the sorted eligible list — so cold-start
/// steady state matches `assign_vips_round_robin_eligible` for an already-sorted VIP list.
pub(crate) fn assign_vips_minimal_movement(
    eligible: &[u64],
    vip_addrs_in_order: &[IpAddr],
    current_holders: &HashMap<IpAddr, u64>,
    out: &mut HashMap<IpAddr, u64>,
) {
    out.clear();
    if eligible.is_empty() {
        return;
    }

    let eligible_set: BTreeSet<u64> = eligible.iter().copied().collect();
    // Load per eligible node, seeded to 0 so empty nodes are visible to the least-loaded search.
    let mut load: BTreeMap<u64, usize> = eligible.iter().map(|&id| (id, 0)).collect();
    // VIPs currently placed on each node, used to pick a donor's highest-IP VIP during rebalance.
    let mut held: BTreeMap<u64, BTreeSet<IpAddr>> = BTreeMap::new();

    // Iterate VIPs in sorted order for deterministic orphan placement.
    let mut vips_sorted = vip_addrs_in_order.to_vec();
    vips_sorted.sort_unstable();

    // Pass 1 — stability: keep VIPs whose current holder is still eligible.
    let mut orphans: Vec<IpAddr> = Vec::new();
    for vip in &vips_sorted {
        match current_holders.get(vip) {
            Some(holder) if eligible_set.contains(holder) => {
                out.insert(*vip, *holder);
                *load.entry(*holder).or_insert(0) += 1;
                held.entry(*holder).or_default().insert(*vip);
            }
            _ => orphans.push(*vip),
        }
    }

    // Pass 2 — orphan placement: least-loaded eligible node, ties broken by the lowest id.
    for vip in orphans {
        let Some((target, _)) = least_loaded_node(&load) else {
            break;
        };
        out.insert(vip, target);
        *load.entry(target).or_insert(0) += 1;
        held.entry(target).or_default().insert(vip);
    }

    // Pass 3 — rebalance until the load spread is at most one VIP. Each iteration strictly reduces
    // the spread (a non-negative integer), so it terminates. `load` is non-empty here, so the
    // most/least lookups always yield `Some`; the loop ends via the spread check below.
    while let (Some((donor, donor_load)), Some((recv, recv_load))) =
        (most_loaded_node(&load), least_loaded_node(&load))
    {
        if donor_load.saturating_sub(recv_load) <= 1 {
            break;
        }
        // The donor holds at least two VIPs here, so `next_back` (highest IP) always yields one.
        let Some(vip) = held.get(&donor).and_then(|s| s.iter().next_back().copied()) else {
            break;
        };
        out.insert(vip, recv);
        if let Some(donor_set) = held.get_mut(&donor) {
            donor_set.remove(&vip);
        }
        held.entry(recv).or_default().insert(vip);
        if let Some(d) = load.get_mut(&donor) {
            *d -= 1;
        }
        *load.entry(recv).or_insert(0) += 1;
    }
}

/// Decide whether `node_id` is currently eligible to hold a VIP.
///
/// Eligibility requires:
/// 1. Not permanently blocked by `failback: false` (`node_failback_blocked`).
/// 2. Last reported health is `true`.
/// 3. Most recent committed probe round is within `stale_missed_probes` of the cluster frontier.
/// 4. If `failback_delay_ticks > 0` and the node is recovering (has a `node_recovery_tick`), at
///    least `failback_delay_ticks` probe rounds must have passed since recovery started.
///    A node that was never unhealthy has no recovery tick and is immediately eligible.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn is_node_eligible(
    node_id: u64,
    node_health: &HashMap<u64, bool>,
    node_probe_ticks: &HashMap<u64, u64>,
    latest_probe_tick: u64,
    stale_missed_probes: u64,
    failback_delay_ticks: u64,
    node_recovery_tick: &HashMap<u64, u64>,
    node_failback_blocked: &HashSet<u64>,
) -> bool {
    // Permanently blocked (failback: false path).
    if node_failback_blocked.contains(&node_id) {
        return false;
    }
    if !*node_health.get(&node_id).unwrap_or(&false) {
        return false;
    }
    let last_tick = match node_probe_ticks.get(&node_id) {
        Some(&t) => t,
        None => return false,
    };
    if latest_probe_tick.saturating_sub(last_tick) > stale_missed_probes {
        return false;
    }
    // Failback delay: if the node is in recovery, it must have been healthy for enough rounds.
    if failback_delay_ticks > 0 {
        if let Some(&recovery_tick) = node_recovery_tick.get(&node_id) {
            if latest_probe_tick.saturating_sub(recovery_tick) < failback_delay_ticks {
                return false;
            }
        }
        // No recovery tick = node was never unhealthy on this run = eligible immediately.
    }
    true
}

/// Compute a node's next committed probe tick when it publishes a `HealthUpdate`.
///
/// A publishing node advances by one but never trails the cluster frontier
/// (`latest_probe_tick`): a node returning after downtime (or a freshly joined node) catches up to
/// the frontier in a single update and so regains freshness immediately, while a node that has
/// *stopped* publishing keeps falling behind as the frontier advances — which is what drives
/// failover. Pure and deterministic (no clocks), so every node and every replay agree.
#[must_use]
pub fn next_probe_tick(prev: Option<u64>, latest_probe_tick: u64) -> u64 {
    prev.unwrap_or(0).saturating_add(1).max(latest_probe_tick)
}

#[allow(clippy::too_many_arguments)]
fn eligible_nodes(
    membership: &StoredMembershipOf<TypeConfig>,
    node_health: &HashMap<u64, bool>,
    node_probe_ticks: &HashMap<u64, u64>,
    latest_probe_tick: u64,
    stale_missed_probes: u64,
    failback_delay_ticks: u64,
    node_recovery_tick: &HashMap<u64, u64>,
    node_failback_blocked: &HashSet<u64>,
) -> Vec<u64> {
    let mut members: Vec<u64> = membership.membership().nodes().map(|(id, _)| *id).collect();
    members.sort_unstable();
    members
        .into_iter()
        .filter(|id| {
            is_node_eligible(
                *id,
                node_health,
                node_probe_ticks,
                latest_probe_tick,
                stale_missed_probes,
                failback_delay_ticks,
                node_recovery_tick,
                node_failback_blocked,
            )
        })
        .collect()
}

/// Recompute `vip_holder` from membership, reported health, committed probe rounds, the configured
/// VIP list and the prior committed holders. Eligible nodes are voter members with
/// `is_node_eligible(...) == true`. VIPs stay on their current eligible holder where possible and
/// only orphaned/imbalanced VIPs move, via [`assign_vips_minimal_movement`] (minimal-movement
/// multi-VIP rebalance). `current_holders` must come from the committed `vip_assignments` so the
/// recompute stays deterministic across nodes and replays.
#[allow(clippy::too_many_arguments)]
pub fn recompute_vip_holder(
    membership: &StoredMembershipOf<TypeConfig>,
    node_health: &HashMap<u64, bool>,
    node_probe_ticks: &HashMap<u64, u64>,
    latest_probe_tick: u64,
    stale_missed_probes: u64,
    failback_delay_ticks: u64,
    node_recovery_tick: &HashMap<u64, u64>,
    node_failback_blocked: &HashSet<u64>,
    vip_list: &[(VipAddr, String)],
    current_holders: &HashMap<IpAddr, u64>,
    out: &mut HashMap<IpAddr, u64>,
) {
    let eligible = eligible_nodes(
        membership,
        node_health,
        node_probe_ticks,
        latest_probe_tick,
        stale_missed_probes,
        failback_delay_ticks,
        node_recovery_tick,
        node_failback_blocked,
    );
    let vips: Vec<IpAddr> = vip_list.iter().map(|(v, _)| v.addr).collect();
    assign_vips_minimal_movement(&eligible, &vips, current_holders, out);
}

/// Merge the newly recomputed `vip_holder` map into the committed fenced assignment state.
///
/// Every holder change bumps the per-VIP generation and records the previous holder that has to
/// release before a healthy replacement may activate.
pub fn reconcile_vip_assignments(
    new_holders: &HashMap<IpAddr, u64>,
    latest_probe_tick: u64,
    vip_list: &[(VipAddr, String)],
    out_assignments: &mut HashMap<IpAddr, VipAssignment>,
    vip_generation: &mut HashMap<IpAddr, u64>,
) {
    let mut next = HashMap::new();

    for (v, _) in vip_list {
        let vip = &v.addr;
        let next_holder = new_holders.get(vip).copied();
        let old_assignment = out_assignments.remove(vip);
        let current_generation = vip_generation.get(vip).copied().unwrap_or(0);

        let Some(holder) = next_holder else {
            continue;
        };

        let assignment = match old_assignment {
            Some(existing) if existing.holder == holder => existing,
            Some(existing) => {
                let generation = current_generation.saturating_add(1);
                vip_generation.insert(*vip, generation);
                VipAssignment {
                    holder,
                    generation,
                    previous_holder: Some(existing.holder),
                    previous_holder_released: false,
                    activation_tick: latest_probe_tick
                        .saturating_add(OWNERSHIP_ACTIVATION_HOLDOFF_TICKS),
                }
            }
            None => {
                let generation = current_generation.saturating_add(1);
                vip_generation.insert(*vip, generation);
                VipAssignment {
                    holder,
                    generation,
                    previous_holder: None,
                    previous_holder_released: true,
                    activation_tick: latest_probe_tick,
                }
            }
        };

        next.insert(*vip, assignment);
    }

    *out_assignments = next;
}

#[cfg(test)]
mod tests {
    use super::super::super::types::TypeConfig;
    use super::super::state::{KafSnapshot, OWNERSHIP_ACTIVATION_HOLDOFF_TICKS, VipAssignment};
    use super::{
        assign_vips_minimal_movement, assign_vips_round_robin_eligible, is_node_eligible,
        next_probe_tick, recompute_vip_holder, reconcile_vip_assignments,
    };
    use crate::config::VipAddr;
    use openraft::alias::StoredMembershipOf;
    use openraft::{BasicNode, Membership};
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    use std::net::{IpAddr, Ipv4Addr};
    use std::str::FromStr;

    fn ip4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    #[test]
    fn round_robin_empty_eligible_clears_map() {
        let mut out = HashMap::from([(ip4(10, 0, 0, 1), 99_u64)]);
        assign_vips_round_robin_eligible(&[], &[ip4(10, 0, 0, 1)], &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn round_robin_single_holder_all_vips() {
        let mut out = HashMap::new();
        let vips = [
            ip4(10, 0, 0, 1),
            ip4(10, 0, 0, 2),
            IpAddr::from_str("2001:db8::1").unwrap(),
        ];
        assign_vips_round_robin_eligible(&[7], &vips, &mut out);
        assert_eq!(out.len(), 3);
        assert_eq!(out[&vips[0]], 7);
        assert_eq!(out[&vips[1]], 7);
        assert_eq!(out[&vips[2]], 7);
    }

    #[test]
    fn round_robin_two_holders_distributes() {
        let mut out = HashMap::new();
        let vips = [
            ip4(192, 168, 1, 1),
            ip4(192, 168, 1, 2),
            ip4(192, 168, 1, 3),
        ];
        assign_vips_round_robin_eligible(&[1, 2], &vips, &mut out);
        assert_eq!(out[&vips[0]], 1);
        assert_eq!(out[&vips[1]], 2);
        assert_eq!(out[&vips[2]], 1);
    }

    #[test]
    fn round_robin_three_holders_wraps() {
        let mut out = HashMap::new();
        let vips = [
            ip4(1, 1, 1, 1),
            ip4(2, 2, 2, 2),
            ip4(3, 3, 3, 3),
            ip4(4, 4, 4, 4),
        ];
        assign_vips_round_robin_eligible(&[10, 20, 30], &vips, &mut out);
        assert_eq!(out[&vips[0]], 10);
        assert_eq!(out[&vips[1]], 20);
        assert_eq!(out[&vips[2]], 30);
        assert_eq!(out[&vips[3]], 10);
    }

    #[test]
    fn eligibility_requires_healthy_and_fresh_probe_round() {
        let nh = HashMap::from([(1_u64, true), (2, true), (3, false)]);
        let ticks = HashMap::from([(1_u64, 10_u64), (2, 4), (3, 10)]);
        let latest = 10_u64;
        let stale = 3_u64;
        let empty_rt = HashMap::new();
        let empty_fb = HashSet::new();
        assert!(is_node_eligible(
            1, &nh, &ticks, latest, stale, 0, &empty_rt, &empty_fb
        ));
        assert!(!is_node_eligible(
            2, &nh, &ticks, latest, stale, 0, &empty_rt, &empty_fb
        ));
        assert!(!is_node_eligible(
            3, &nh, &ticks, latest, stale, 0, &empty_rt, &empty_fb
        ));
        assert!(!is_node_eligible(
            4, &nh, &ticks, latest, stale, 0, &empty_rt, &empty_fb
        ));
    }

    #[test]
    fn next_probe_tick_advances_frontier_and_catches_up() {
        // First-ever publish.
        assert_eq!(next_probe_tick(None, 0), 1);
        // Frontier node advances by one.
        assert_eq!(next_probe_tick(Some(5), 5), 6);
        // A node one behind the frontier reaches it.
        assert_eq!(next_probe_tick(Some(5), 6), 6);
        // A node far behind (e.g. just back from downtime) jumps straight to the frontier.
        assert_eq!(next_probe_tick(Some(1), 20), 20);
        // A brand-new node joining an advanced cluster is immediately at the frontier.
        assert_eq!(next_probe_tick(None, 9), 9);
    }

    #[test]
    fn returning_node_regains_eligibility_after_one_update() {
        let nh = HashMap::from([(1_u64, true)]);
        let latest = 20_u64;
        let stale = 3_u64;
        // Stale after a long downtime: tick far behind the frontier.
        let before = HashMap::from([(1_u64, 1_u64)]);
        let empty_rt = HashMap::new();
        let empty_fb = HashSet::new();
        assert!(!is_node_eligible(
            1, &nh, &before, latest, stale, 0, &empty_rt, &empty_fb
        ));
        // One published HealthUpdate catches the node up to the frontier...
        let caught_up = next_probe_tick(before.get(&1).copied(), latest);
        let after = HashMap::from([(1_u64, caught_up)]);
        // ...so it is fresh (eligible) again.
        assert!(is_node_eligible(
            1, &nh, &after, latest, stale, 0, &empty_rt, &empty_fb
        ));
    }

    #[test]
    fn eligibility_saturates_on_missing_latest_progress() {
        let nh = HashMap::from([(1_u64, true)]);
        let ticks = HashMap::from([(1_u64, 20_u64)]);
        assert!(is_node_eligible(
            1,
            &nh,
            &ticks,
            10,
            3,
            0,
            &HashMap::new(),
            &HashSet::new()
        ));
    }

    #[test]
    fn recompute_no_raft_members_clears_holders_even_if_health_present() {
        let membership = StoredMembershipOf::<TypeConfig>::default();
        let node_health = HashMap::from([(1_u64, true)]);
        let ticks = HashMap::from([(1_u64, 10_u64)]);
        let vips = vec![(VipAddr::host(ip4(10, 0, 0, 99)), "lo".into())];
        let mut out = HashMap::from([(ip4(1, 1, 1, 1), 999_u64)]);
        recompute_vip_holder(
            &membership,
            &node_health,
            &ticks,
            10,
            3,
            0,
            &HashMap::new(),
            &HashSet::new(),
            &vips,
            &HashMap::new(),
            &mut out,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn recompute_unhealthy_members_only_leaves_empty() {
        let m = Membership::<u64, BasicNode>::new(
            vec![BTreeSet::from([1_u64])],
            BTreeMap::from([(1_u64, BasicNode::default())]),
        )
        .unwrap();
        let membership = StoredMembershipOf::<TypeConfig>::new(None, m);
        let node_health = HashMap::from([(1_u64, false)]);
        let ticks = HashMap::from([(1_u64, 5_u64)]);
        let vips = vec![(VipAddr::host(ip4(10, 0, 0, 1)), "eth0".into())];
        let mut out = HashMap::new();
        recompute_vip_holder(
            &membership,
            &node_health,
            &ticks,
            5,
            3,
            0,
            &HashMap::new(),
            &HashSet::new(),
            &vips,
            &HashMap::new(),
            &mut out,
        );
        assert!(out.is_empty());

        let healthy = HashMap::from([(1_u64, true)]);
        let mut out2 = HashMap::new();
        recompute_vip_holder(
            &membership,
            &healthy,
            &ticks,
            5,
            3,
            0,
            &HashMap::new(),
            &HashSet::new(),
            &vips,
            &HashMap::new(),
            &mut out2,
        );
        assert_eq!(out2.len(), 1);
        assert_eq!(out2[&ip4(10, 0, 0, 1)], 1_u64);
    }

    #[test]
    fn recompute_stale_holder_loses_vip_to_fresh_peer() {
        let m = Membership::<u64, BasicNode>::new(
            vec![BTreeSet::from([1_u64, 2])],
            BTreeMap::from([(1_u64, BasicNode::default()), (2, BasicNode::default())]),
        )
        .unwrap();
        let membership = StoredMembershipOf::<TypeConfig>::new(None, m);
        let nh = HashMap::from([(1_u64, true), (2, true)]);
        let ticks = HashMap::from([(1_u64, 1_u64), (2, 7)]);
        let vips = vec![(VipAddr::host(ip4(10, 0, 0, 1)), "eth0".into())];
        let mut out = HashMap::new();
        recompute_vip_holder(
            &membership,
            &nh,
            &ticks,
            7,
            3,
            0,
            &HashMap::new(),
            &HashSet::new(),
            &vips,
            &HashMap::new(),
            &mut out,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[&ip4(10, 0, 0, 1)], 2_u64);
    }

    #[test]
    fn recompute_each_vip_maps_to_exactly_one_eligible_holder() {
        let m = Membership::<u64, BasicNode>::new(
            vec![BTreeSet::from([10_u64, 20])],
            BTreeMap::from([(10_u64, BasicNode::default()), (20, BasicNode::default())]),
        )
        .unwrap();
        let membership = StoredMembershipOf::<TypeConfig>::new(None, m);
        let nh = HashMap::from([(10_u64, true), (20, true)]);
        let ticks = HashMap::from([(10_u64, 4_u64), (20, 4)]);
        let vips = vec![
            (VipAddr::host(ip4(10, 0, 0, 1)), "eth0".into()),
            (VipAddr::host(ip4(10, 0, 0, 2)), "eth0".into()),
            (VipAddr::host(ip4(10, 0, 0, 3)), "eth0".into()),
        ];
        let mut out = HashMap::new();
        recompute_vip_holder(
            &membership,
            &nh,
            &ticks,
            4,
            3,
            0,
            &HashMap::new(),
            &HashSet::new(),
            &vips,
            &HashMap::new(),
            &mut out,
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out[&vips[0].0.addr], 10);
        assert_eq!(out[&vips[1].0.addr], 20);
        assert_eq!(out[&vips[2].0.addr], 10);
    }

    #[test]
    fn reconcile_assignments_records_previous_holder_and_generation() {
        let vip = ip4(10, 0, 0, 50);
        let vip_list = vec![(VipAddr::host(vip), "eth0".into())];
        let mut assignments = HashMap::from([(
            vip,
            VipAssignment {
                holder: 1,
                generation: 4,
                previous_holder: None,
                previous_holder_released: true,
                activation_tick: 2,
            },
        )]);
        let mut generations = HashMap::from([(vip, 4_u64)]);
        let new_holders = HashMap::from([(vip, 2_u64)]);

        reconcile_vip_assignments(
            &new_holders,
            9,
            &vip_list,
            &mut assignments,
            &mut generations,
        );

        let a = assignments.get(&vip).unwrap();
        assert_eq!(a.holder, 2);
        assert_eq!(a.generation, 5);
        assert_eq!(a.previous_holder, Some(1));
        assert!(!a.previous_holder_released);
        assert_eq!(a.activation_tick, 9 + OWNERSHIP_ACTIVATION_HOLDOFF_TICKS);
    }

    #[test]
    fn reconcile_assignments_keeps_generation_when_holder_unchanged() {
        let vip = ip4(10, 0, 0, 51);
        let vip_list = vec![(VipAddr::host(vip), "eth0".into())];
        let existing = VipAssignment {
            holder: 2,
            generation: 6,
            previous_holder: Some(1),
            previous_holder_released: true,
            activation_tick: 12,
        };
        let mut assignments = HashMap::from([(vip, existing.clone())]);
        let mut generations = HashMap::from([(vip, 6_u64)]);
        let new_holders = HashMap::from([(vip, 2_u64)]);

        reconcile_vip_assignments(
            &new_holders,
            20,
            &vip_list,
            &mut assignments,
            &mut generations,
        );

        assert_eq!(assignments.get(&vip), Some(&existing));
        assert_eq!(generations.get(&vip), Some(&6_u64));
    }

    // Four VIPs in ascending IP order for the minimal-movement tests:
    // ipa(.10) < ipb(.20) < ipc(.30) < ipd(.40).
    fn ipa() -> IpAddr {
        ip4(10, 0, 0, 10)
    }
    fn ipb() -> IpAddr {
        ip4(10, 0, 0, 20)
    }
    fn ipc() -> IpAddr {
        ip4(10, 0, 0, 30)
    }
    fn ipd() -> IpAddr {
        ip4(10, 0, 0, 40)
    }

    #[test]
    fn minmove_cold_start_matches_round_robin() {
        // With no prior holders the minimal-movement placement reduces to round-robin over the
        // sorted eligible list, so cold-start steady state is unchanged from the old algorithm.
        let cases: &[(&[u64], Vec<IpAddr>)] = &[
            (&[1, 2, 3], vec![ipa(), ipb(), ipc(), ipd()]),
            (
                &[10, 20],
                vec![ip4(10, 0, 0, 1), ip4(10, 0, 0, 2), ip4(10, 0, 0, 3)],
            ),
            (&[7], vec![ipa(), ipb(), ipc()]),
        ];
        for (eligible, vips) in cases {
            let mut min_move = HashMap::new();
            assign_vips_minimal_movement(eligible, vips, &HashMap::new(), &mut min_move);
            let mut round_robin = HashMap::new();
            assign_vips_round_robin_eligible(eligible, vips, &mut round_robin);
            assert_eq!(min_move, round_robin, "eligible {eligible:?}");
        }
    }

    #[test]
    fn minmove_empty_eligible_clears() {
        let mut out = HashMap::from([(ipa(), 99_u64)]);
        let prior = HashMap::from([(ipa(), 1_u64)]);
        assign_vips_minimal_movement(&[], &[ipa()], &prior, &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn minmove_keeps_eligible_holder_stable() {
        // Both holders stay eligible and the spread is only 1, so nothing moves and the spare node
        // stays empty (we do not rebalance below a spread of 1).
        let prior = HashMap::from([(ipa(), 1_u64), (ipb(), 2_u64)]);
        let mut out = HashMap::new();
        assign_vips_minimal_movement(&[1, 2, 3], &[ipa(), ipb()], &prior, &mut out);
        assert_eq!(out, HashMap::from([(ipa(), 1_u64), (ipb(), 2_u64)]));
    }

    #[test]
    fn minmove_orphan_goes_to_least_loaded_lowest_id() {
        // Holder of ipb became ineligible: ipb is re-placed on the empty eligible node, ipa stays.
        let prior = HashMap::from([(ipa(), 1_u64), (ipb(), 2_u64)]);
        let mut out = HashMap::new();
        assign_vips_minimal_movement(&[1, 3], &[ipa(), ipb()], &prior, &mut out);
        assert_eq!(out, HashMap::from([(ipa(), 1_u64), (ipb(), 3_u64)]));

        // A never-assigned VIP (ipd) lands on the only empty node; the rest keep their holders.
        let prior = HashMap::from([(ipa(), 1_u64), (ipb(), 1_u64), (ipc(), 2_u64)]);
        let mut out = HashMap::new();
        assign_vips_minimal_movement(&[1, 2, 3], &[ipa(), ipb(), ipc(), ipd()], &prior, &mut out);
        assert_eq!(out[&ipa()], 1);
        assert_eq!(out[&ipb()], 1);
        assert_eq!(out[&ipc()], 2);
        assert_eq!(out[&ipd()], 3);
    }

    #[test]
    fn minmove_rebalance_donates_highest_ip_vip() {
        // Donor node 1 holds two VIPs while node 2 is empty: the donor's HIGHEST-IP VIP (ipc) moves.
        let prior = HashMap::from([(ipa(), 1_u64), (ipc(), 1_u64)]);
        let mut out = HashMap::new();
        assign_vips_minimal_movement(&[1, 2], &[ipa(), ipc()], &prior, &mut out);
        assert_eq!(out, HashMap::from([(ipa(), 1_u64), (ipc(), 2_u64)]));

        // With three VIPs on the donor only ONE moves (highest IP), leaving a spread of 1.
        let prior = HashMap::from([(ipa(), 1_u64), (ipb(), 1_u64), (ipc(), 1_u64)]);
        let mut out = HashMap::new();
        assign_vips_minimal_movement(&[1, 2], &[ipa(), ipb(), ipc()], &prior, &mut out);
        assert_eq!(out[&ipc()], 2, "highest-IP VIP is the one donated");
        assert_eq!(out[&ipa()], 1);
        assert_eq!(out[&ipb()], 1);
    }

    /// Drive the exact 5-node / 4-VIP failover narrative through the minimal-movement algorithm:
    /// B fails (IPB -> empty E), C fails (IPC -> A, A holds two), B returns (IPC rebalances to B).
    /// Nodes A=1..E=5; VIPs ipa<ipb<ipc<ipd.
    #[test]
    fn minmove_five_node_four_vip_down_down_up_sequence() {
        let vips = [ipa(), ipb(), ipc(), ipd()];

        // Start: all five eligible, cold start -> A,B,C,D each take one, E stays empty.
        let mut start = HashMap::new();
        assign_vips_minimal_movement(&[1, 2, 3, 4, 5], &vips, &HashMap::new(), &mut start);
        assert_eq!(start[&ipa()], 1);
        assert_eq!(start[&ipb()], 2);
        assert_eq!(start[&ipc()], 3);
        assert_eq!(start[&ipd()], 4);
        assert!(!start.values().any(|&h| h == 5), "node E starts empty");

        // B (id 2) down: only IPB is orphaned and it goes to the empty node E (id 5).
        let mut b_down = HashMap::new();
        assign_vips_minimal_movement(&[1, 3, 4, 5], &vips, &start, &mut b_down);
        assert_eq!(b_down[&ipa()], 1, "IPA stays on A");
        assert_eq!(b_down[&ipc()], 3, "IPC stays on C");
        assert_eq!(b_down[&ipd()], 4, "IPD stays on D");
        assert_eq!(b_down[&ipb()], 5, "IPB fails over to E");

        // C (id 3) down: IPC is orphaned and goes to the least-loaded lowest-id node, A (id 1),
        // so A now holds IPA + IPC.
        let mut c_down = HashMap::new();
        assign_vips_minimal_movement(&[1, 4, 5], &vips, &b_down, &mut c_down);
        assert_eq!(c_down[&ipa()], 1);
        assert_eq!(c_down[&ipc()], 1, "IPC fails over to A");
        assert_eq!(c_down[&ipd()], 4);
        assert_eq!(c_down[&ipb()], 5);

        // B (id 2) returns while C stays down: A is overloaded (2) and B is empty (0), so the
        // rebalance moves A's highest-IP VIP (IPC) to B; A drops back to a single VIP.
        let mut b_up = HashMap::new();
        assign_vips_minimal_movement(&[1, 2, 4, 5], &vips, &c_down, &mut b_up);
        assert_eq!(b_up[&ipa()], 1, "IPA stays on A");
        assert_eq!(b_up[&ipc()], 2, "IPC rebalances to B");
        assert_eq!(b_up[&ipd()], 4);
        assert_eq!(b_up[&ipb()], 5);

        // Maximally even: every VIP held by a distinct one of A,B,D,E; C (down) holds none.
        let holders: BTreeSet<u64> = b_up.values().copied().collect();
        assert_eq!(holders, BTreeSet::from([1, 2, 4, 5]));
    }

    #[test]
    fn minmove_idempotent_no_oscillation() {
        // A balanced assignment must be a fixed point: recomputing with the same eligible set
        // moves nothing, so there are no spurious holder changes (and thus no generation churn).
        let vips = [ipa(), ipb(), ipc(), ipd()];
        let balanced = HashMap::from([
            (ipa(), 1_u64),
            (ipc(), 2_u64),
            (ipd(), 4_u64),
            (ipb(), 5_u64),
        ]);
        let mut current = balanced.clone();
        for _ in 0..3 {
            let mut next = HashMap::new();
            assign_vips_minimal_movement(&[1, 2, 4, 5], &vips, &current, &mut next);
            assert_eq!(next, balanced);
            current = next;
        }
    }

    #[test]
    fn minmove_at_most_one_vip_per_node_when_eligible_ge_vips() {
        // Invariant: whenever there are at least as many eligible nodes as VIPs, NO node ends up
        // holding more than one VIP. Pass 3 rebalances until the load spread is <= 1; with
        // eligible >= vips some node stays empty (load 0) until every holder is down to one, so the
        // spread check forces the maximum load to 1. We sweep both N == V and N > V and start each
        // case from the worst prior -- every VIP hoarded on a single node -- to prove the rebalance
        // spreads them out rather than relying on a lucky starting layout.
        let cases: &[&[u64]] = &[
            // N == V (a node empty only transiently; final spread is 0)
            &[1, 2],
            &[1, 2, 3],
            &[10, 20, 30, 40],
            // N > V (at least one node is permanently idle)
            &[1, 2, 3],
            &[1, 2, 3, 4, 5],
            &[1, 2, 3, 4, 5],
        ];
        let vip_pool = [ipa(), ipb(), ipc(), ipd()];
        for (i, eligible) in cases.iter().enumerate() {
            // Pick a VIP count <= number of eligible nodes for this case.
            let n_vips = match i {
                0 => 2, // [1,2]
                1 => 3, // [1,2,3]
                2 => 4, // [10,20,30,40]
                3 => 2, // [1,2,3] with 2 vips  (N > V)
                4 => 3, // [1,2,3,4,5] with 3 vips
                _ => 1, // [1,2,3,4,5] with 1 vip
            };
            let vips: Vec<IpAddr> = vip_pool[..n_vips].to_vec();
            assert!(eligible.len() >= vips.len(), "case {i} must have N >= V");

            // Adversarial prior: every VIP hoarded on the lowest-id eligible node.
            let hoarded: HashMap<IpAddr, u64> = vips.iter().map(|&v| (v, eligible[0])).collect();
            let mut out = HashMap::new();
            assign_vips_minimal_movement(eligible, &vips, &hoarded, &mut out);

            // Every VIP placed, on an eligible node...
            assert_eq!(out.len(), vips.len(), "all VIPs placed for {eligible:?}");
            assert!(
                out.values().all(|h| eligible.contains(h)),
                "holders within eligible for {eligible:?}"
            );
            // ...and no node holds more than one (equivalently: all holders distinct).
            let mut counts: HashMap<u64, usize> = HashMap::new();
            for &h in out.values() {
                *counts.entry(h).or_insert(0) += 1;
            }
            assert!(
                counts.values().all(|&c| c <= 1),
                "no node may exceed 1 VIP for eligible {eligible:?}, got {counts:?}"
            );
            let distinct: BTreeSet<u64> = out.values().copied().collect();
            assert_eq!(
                distinct.len(),
                vips.len(),
                "holders must be distinct for {eligible:?}"
            );
        }
    }

    #[test]
    fn recompute_minimal_movement_keeps_stable_holders_through_membership() {
        // End-to-end through recompute_vip_holder with a 5-voter membership: node 2 is unhealthy,
        // so only its VIP moves (to the empty node 5) and the others keep their holders.
        let m = Membership::<u64, BasicNode>::new(
            vec![BTreeSet::from([1_u64, 2, 3, 4, 5])],
            BTreeMap::from([
                (1_u64, BasicNode::default()),
                (2, BasicNode::default()),
                (3, BasicNode::default()),
                (4, BasicNode::default()),
                (5, BasicNode::default()),
            ]),
        )
        .unwrap();
        let membership = StoredMembershipOf::<TypeConfig>::new(None, m);
        let nh = HashMap::from([(1_u64, true), (2, false), (3, true), (4, true), (5, true)]);
        let ticks = HashMap::from([(1_u64, 4_u64), (2, 4), (3, 4), (4, 4), (5, 4)]);
        let vip_list = vec![
            (VipAddr::host(ipa()), "eth0".into()),
            (VipAddr::host(ipb()), "eth0".into()),
            (VipAddr::host(ipc()), "eth0".into()),
            (VipAddr::host(ipd()), "eth0".into()),
        ];
        let prior = HashMap::from([
            (ipa(), 1_u64),
            (ipb(), 2_u64),
            (ipc(), 3_u64),
            (ipd(), 4_u64),
        ]);
        let mut out = HashMap::new();
        recompute_vip_holder(
            &membership,
            &nh,
            &ticks,
            4,
            3,
            0,
            &HashMap::new(),
            &HashSet::new(),
            &vip_list,
            &prior,
            &mut out,
        );
        assert_eq!(out[&ipa()], 1);
        assert_eq!(out[&ipc()], 3);
        assert_eq!(out[&ipd()], 4);
        assert_eq!(
            out[&ipb()],
            5,
            "unhealthy node 2's VIP moves to the empty node 5"
        );
    }

    #[test]
    fn snapshot_roundtrips_cluster_epoch_and_defaults_to_none() {
        // Incarnations routinely exceed u64::MAX, so the real snapshot paths use `to_vec`/
        // `from_slice` (full-precision integer literals), never the `Value` DOM (which caps at u64).
        let snap = KafSnapshot {
            cluster_epoch: Some(u128::MAX),
            ..KafSnapshot::default()
        };
        let bytes = serde_json::to_vec(&snap).unwrap();
        let back: KafSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.cluster_epoch, Some(u128::MAX));

        // A snapshot encoded before this field existed (key absent) must decode to `None`. Use the
        // `None` case here so the `Value` DOM round-trip stays within range.
        let none_snap = KafSnapshot::default();
        let mut val = serde_json::to_value(&none_snap).unwrap();
        val.as_object_mut().unwrap().remove("cluster_epoch");
        let legacy: KafSnapshot = serde_json::from_value(val).unwrap();
        assert_eq!(legacy.cluster_epoch, None);
    }

    #[test]
    fn is_node_eligible_exact_staleness_boundary() {
        let nh = HashMap::from([(1_u64, true)]);
        // latest - last == stale → still eligible.
        let at = HashMap::from([(1_u64, 7_u64)]);
        assert!(is_node_eligible(
            1,
            &nh,
            &at,
            10,
            3,
            0,
            &HashMap::new(),
            &HashSet::new()
        ));
        // latest - last == stale + 1 → fenced off.
        let past = HashMap::from([(1_u64, 6_u64)]);
        assert!(!is_node_eligible(
            1,
            &nh,
            &past,
            10,
            3,
            0,
            &HashMap::new(),
            &HashSet::new()
        ));
    }

    #[test]
    fn is_node_eligible_unhealthy_or_missing_tick_is_ineligible() {
        let nh = HashMap::from([(1_u64, false), (2, true)]);
        let ticks = HashMap::from([(1_u64, 10_u64)]); // node 2 has never reported a tick
        let empty_rt = HashMap::new();
        let empty_fb = HashSet::new();
        assert!(!is_node_eligible(
            1, &nh, &ticks, 10, 3, 0, &empty_rt, &empty_fb
        )); // unhealthy flag
        assert!(!is_node_eligible(
            2, &nh, &ticks, 10, 3, 0, &empty_rt, &empty_fb
        )); // missing probe tick
        assert!(!is_node_eligible(
            3, &nh, &ticks, 10, 3, 0, &empty_rt, &empty_fb
        )); // unknown node
    }

    #[test]
    fn next_probe_tick_saturates_without_overflow() {
        assert_eq!(next_probe_tick(Some(u64::MAX), 0), u64::MAX);
        assert_eq!(next_probe_tick(Some(u64::MAX), u64::MAX), u64::MAX);
    }

    #[test]
    fn failback_false_blocks_node_after_going_unhealthy() {
        // A node in the failback_blocked set is never eligible regardless of health/ticks.
        let nh = HashMap::from([(1_u64, true)]);
        let ticks = HashMap::from([(1_u64, 10_u64)]);
        let mut blocked = HashSet::new();
        blocked.insert(1_u64);
        assert!(!is_node_eligible(
            1,
            &nh,
            &ticks,
            10,
            3,
            0,
            &HashMap::new(),
            &blocked
        ));
    }

    #[test]
    fn failback_enabled_zero_delay_is_eligible_immediately_after_recovery() {
        // Recovery tick set, delay == 0: node is immediately eligible.
        let nh = HashMap::from([(1_u64, true)]);
        let ticks = HashMap::from([(1_u64, 10_u64)]);
        let recovery = HashMap::from([(1_u64, 10_u64)]);
        assert!(is_node_eligible(
            1,
            &nh,
            &ticks,
            10,
            3,
            0,
            &recovery,
            &HashSet::new()
        ));
    }

    #[test]
    fn failback_enabled_with_delay_waits_n_ticks() {
        let nh = HashMap::from([(1_u64, true)]);
        let ticks = HashMap::from([(1_u64, 10_u64)]);
        // Recovery tick = 8, delay = 3: need latest - recovery >= 3, i.e. latest >= 11.
        let recovery = HashMap::from([(1_u64, 8_u64)]);
        // At tick 10: 10 - 8 = 2 < 3 → not eligible yet.
        assert!(!is_node_eligible(
            1,
            &nh,
            &ticks,
            10,
            3,
            3,
            &recovery,
            &HashSet::new()
        ));
        // At tick 11: 11 - 8 = 3 >= 3 → eligible.
        let ticks11 = HashMap::from([(1_u64, 11_u64)]);
        assert!(is_node_eligible(
            1,
            &nh,
            &ticks11,
            11,
            3,
            3,
            &recovery,
            &HashSet::new()
        ));
    }

    #[test]
    fn failback_oscillating_node_resets_recovery_timer() {
        // Node recovers at tick 5 (recovery_tick = 5), delay = 3.
        // At tick 7 it's not yet eligible (7 - 5 = 2 < 3).
        // If it goes unhealthy again, recovery_tick is cleared; once it recovers again the
        // new recovery_tick resets the timer.
        let nh = HashMap::from([(1_u64, true)]);
        let ticks = HashMap::from([(1_u64, 7_u64)]);
        let recovery_at_5 = HashMap::from([(1_u64, 5_u64)]);
        assert!(!is_node_eligible(
            1,
            &nh,
            &ticks,
            7,
            3,
            3,
            &recovery_at_5,
            &HashSet::new()
        ));
        // After unhealthy→healthy again, recovery_tick resets to 7.
        let recovery_at_7 = HashMap::from([(1_u64, 7_u64)]);
        let ticks10 = HashMap::from([(1_u64, 10_u64)]);
        // 10 - 7 = 3 >= 3 → eligible.
        assert!(is_node_eligible(
            1,
            &nh,
            &ticks10,
            10,
            3,
            3,
            &recovery_at_7,
            &HashSet::new()
        ));
    }
}
