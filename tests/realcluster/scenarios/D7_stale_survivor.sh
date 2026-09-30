#!/usr/bin/env bash
# D7: Stale-survivor fencing (cluster incarnation). Partition node 1 from {2,3}. Restart
# nodes 2+3 from scratch so they reform under a NEW cluster incarnation. Heal the partition.
# The stale survivor (node 1, still on the OLD incarnation) must be FENCED by epochs_compatible
# so its higher-term vote/append can't corrupt the reformed majority - and run_cluster_guard
# should make it reset (exit) to rejoin cleanly via replication.
SCENARIO_NAME=D7_stale_survivor
source "$(dirname "$0")/../scenario.sh"
scenario_start "stale survivor is fenced by incarnation after majority reforms"

survivor="${NODE_IPS[0]}"
peers=("${NODE_IPS[1]}" "${NODE_IPS[2]}")
evid "isolating stale survivor ${survivor}; peers ${peers[*]} will reform"
survivor_context="$(timed_node_command "${survivor}" :)"

# Isolate node 1 from the other two (blackhole raft/submit both ways via node 1's iptables).
partition_node "${survivor}"
sleep 5

# The two peers keep a quorum (2 of 3) and hold the VIPs; node 1 self-fences (no VIPs).
check "isolated survivor sheds VIPs" wait_until 60 node_lacks_all_vips "${survivor}"

# Force the peers to reform under a new incarnation: stop both, then start both fresh. With the
# survivor partitioned, the two reforming peers can't see its old incarnation and mint a new one.
evid "reforming peers from scratch (new incarnation)"
for ip in "${peers[@]}"; do kafd_stop "${ip}"; done
sleep 3
for ip in "${peers[@]}"; do kafd_start "${ip}"; done
check "peers reform with a leader" wait_until 90 single_agreed_leader
check "peers hold every VIP uniquely while the survivor is isolated" \
  wait_for_live_service_without 90 "${survivor}"

# Heal the partition. The stale survivor now sees the reformed majority on a different
# incarnation. It must be fenced (not corrupt the cluster) and reset itself.
evid "healing partition; survivor must be fenced + reset (run_cluster_guard)"
heal_node "${survivor}"
check "original survivor confirms an incarnation fence and restarts" \
  wait_until 120 incarnation_reset_observed "${survivor}" "${survivor_context}"

# Eventually the survivor rejoins cleanly via replication under the new incarnation. Nopreempt
# does not require it to take a VIP back from a healthy incumbent.
check "survivor rejoins cleanly under new incarnation" wait_until 120 all_vips_uniquely_held
check "all-node service is safely available after incarnation repair" \
  wait_for_available_cluster

scenario_end
