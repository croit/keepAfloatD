#!/usr/bin/env bash
# D8: Full outage + majority recovery. Stop all 3 daemons (VIPs gone), then restart a
# majority (2 of 3); the cluster must reform, elect a leader, and redistribute the VIPs across
# the available nodes - even though the 3rd node stays down.
SCENARIO_NAME=D8_full_outage_recovery
source "$(dirname "$0")/../scenario.sh"
scenario_start "full outage, majority restart reforms cluster and redistributes VIPs"

evid "stopping all 3 keepafloatd daemons"
for ip in "${NODE_IPS[@]}"; do kafd_stop "${ip}"; done
# With graceful stop, VIPs should be unbound everywhere.
check "all VIPs released during outage" wait_until 10 all_cluster_vips_absent
if [[ "${_PASS}" -eq 0 ]]; then
  evid "  kernel state after graceful stop: $(current_assignments_summary)"
fi

# Bring back a majority (nodes 1 and 2); node 3 stays down.
down="${NODE_IPS[2]}"
evid "restarting majority: ${NODE_IPS[0]} ${NODE_IPS[1]} (keeping ${down} down)"
kafd_start "${NODE_IPS[0]}"; kafd_start "${NODE_IPS[1]}"
check "cold majority completes activation without overlapping VIPs" \
  wait_for_startup_activation 30 "${NODE_IPS[0]}" "${NODE_IPS[1]}"

check "majority reforms with a leader" wait_until 90 single_agreed_leader
check "cold-start majority redistributes VIPs across both live voters" \
  wait_for_even 90 "${NODE_IPS[0]}" "${NODE_IPS[1]}"

# Bring the third node back; nopreempt does not require redistribution.
kafd_start "${down}"
check "third node rejoins with safe all-node service" wait_for_available_cluster

scenario_end
