#!/usr/bin/env bash
# D2: Consensus-freshness self-fence (README trigger 2). A holder isolated from the leader
# (raft+submit traffic dropped) cannot commit health/release submits, flips consensus_fresh
# false, and SELF-UNBINDS its VIPs even though its own process is still up and "healthy".
# This prevents a partitioned node from serving a stale VIP.
SCENARIO_NAME=D2_consensus_self_fence
source "$(dirname "$0")/../scenario.sh"
scenario_start "isolated holder self-fences (consensus_fresh=false) and releases VIPs"

snapshot_vips
victim="${NODE_IPS[2]}"   # isolate node 3
held_before=""
for vip in "${VIPS[@]}"; do
  node_sh "${victim}" "ip -o -4 addr show dev ${IFACE} | grep -F -q ' ${vip}/32 '" \
    && held_before+="${vip} "
done
evid "isolating ${victim} from peers (drop raft/submit); it holds: ${held_before:-none}"
check "partition victim starts as a VIP holder" test -n "${held_before}"

partition_node "${victim}"

# The isolated node must self-unbind even though its keepafloatd is still running.
check "isolated node ${victim} self-releases all VIPs" wait_until 60 node_lacks_all_vips "${victim}"
check "isolated node keepafloatd still running (self-fenced, not crashed)" node_active "${victim}"
fence_log="$(kafd_log "${victim}" 60 | grep -iE 'consensus|fresh|self|release|no leader' | tail -3)"
evid "self-fence log on ${victim}:"; evid "${fence_log:-  (none)}"

# Majority (other two) keep all VIPs uniquely held.
check "majority holds all VIPs uniquely and reachably" \
  wait_for_live_service_without 60 "${victim}"

# Heal: the node rejoins without requiring nopreempt ownership to rebalance.
safe_healed_cluster() {
  all_daemons_active && single_agreed_leader && all_vips_uniquely_held && all_vips_pingable
}
heal_node "${victim}"
check "healed node rejoins with safe reachable ownership" \
  wait_until 90 holds_for 5 safe_healed_cluster

scenario_end
