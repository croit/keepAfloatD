#!/usr/bin/env bash
# D10: Asymmetric partition. Drop raft traffic in ONE direction between node A and node C (A can
# send to C but not receive, simulating a one-way link failure). The cluster must still maintain a
# stable leader and unique VIP holders - a one-way break must not cause flapping or double-binds.
SCENARIO_NAME=D10_asymmetric_partition
source "$(dirname "$0")/../scenario.sh"
scenario_start "asymmetric (one-way) partition does not cause flapping or double-bind"

A="${NODE_IPS[0]}"   # node 14
C="${NODE_IPS[2]}"   # node 16
evid "installing ONE-WAY drop: ${C} drops inbound raft/submit FROM ${A} (A->C broken, C->A ok)"

# Only block A->C on C's INPUT. C can still reach A; A cannot deliver to C.
partition_input_from "${C}" "${A}"

# The cluster must remain functional: a single agreed leader and unique holders. A one-way break
# between two followers (or follower/leader) should be tolerated by Raft's retries/other links.
check "single agreed leader persists under asymmetric break" wait_until 60 single_agreed_leader

# Parallel SSH reads are near-simultaneous samples, not a continuous point-in-time monitor. Sample
# 20 bursts across the asymmetric window; any observed duplicate is a real safety violation, while
# zero observations are bounded sampled evidence rather than proof that no sub-sample event existed.
atomic_double=0
for i in $(seq 1 20); do
  snapshot_vips
  for vip in "${VIPS[@]}"; do
    case "$(holder_for_vip "${vip}")" in duplicate:*) atomic_double=$((atomic_double+1)) ;; esac
  done
  sleep 2
done
evid "sampled double-bind observations across 20 near-simultaneous bursts: ${atomic_double}"
check "no sampled double-bind under asymmetric partition" test "${atomic_double}" -eq 0
check "ends uniquely held under asymmetric partition" wait_until 30 all_vips_uniquely_held

evid "healing the one-way drop"
heal_node "${C}"
safe_healed_cluster() {
  all_daemons_active &&
    all_nodes_agree_on_leader &&
    all_vips_uniquely_held &&
    all_vips_pingable
}
check "safe reachable ownership persists after heal" \
  wait_until 60 holds_for 5 safe_healed_cluster

scenario_end
