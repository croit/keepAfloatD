#!/usr/bin/env bash
# Baseline: cluster is in steady state with the fixed binary, VIPs uniquely + evenly held,
# leader agreed, all VIPs pingable, Ceph healthy.
SCENARIO_NAME=00_baseline
source "$(dirname "$0")/../scenario.sh"
scenario_start "steady-state baseline"

# Fixed binary on every node.
for ip in "${NODE_IPS[@]}"; do
  bid="$(node_sh "${ip}" "file -L /usr/bin/keepafloatd 2>/dev/null | grep -oE 'BuildID\[sha1\]=[a-f0-9]+' | cut -d= -f2")"
  check_eq "node ${ip} runs fixed binary" "${bid}" "${FIXED_BUILDID}"
  check_eq "node ${ip} keepafloatd active" "$(kafd_active "${ip}")" "active"
done

check "VIPs uniquely held" assert_unique_holders
check "VIPs evenly distributed" even_over_nodes "${NODE_IPS[@]}"
check "single agreed leader" single_agreed_leader
check "all VIPs pingable" all_vips_pingable

ceph_health="$(ceph_health_status)"
check_eq "ceph healthy" "${ceph_health}" "HEALTH_OK"

evid "assignments: $(current_assignments_summary)"
scenario_end
