#!/usr/bin/env bash
# Coordinated restart replaces the obsolete mixed-authentication rolling upgrade.
SCENARIO_NAME=D15_mixed_version_activation
source "$(dirname "$0")/../scenario.sh"
scenario_start "full-cluster stop releases every VIP before same-version restart"

check "cluster is available before coordinated restart" wait_for_available_cluster
for ip in "${NODE_IPS[@]}"; do
  check_eq "node ${ip} uses the candidate BuildID" \
    "$(running_keepafloatd_buildid "${ip}")" "${FIXED_BUILDID}"
done
for ip in "${NODE_IPS[@]}"; do kafd_stop "${ip}"; done
for ip in "${NODE_IPS[@]}"; do
  check "stopped node ${ip} released every managed VIP" node_lacks_all_vips "${ip}"
done
# Never restart a member if clean shutdown or VIP release failed.
if [[ "${_PASS}" != 1 ]]; then scenario_end; fi
for ip in "${NODE_IPS[@]}"; do kafd_start "${ip}"; done
check "all-current cluster returns to safe service" wait_for_available_cluster
for ip in "${NODE_IPS[@]}"; do
  check_eq "restarted node ${ip} uses the candidate BuildID" \
    "$(running_keepafloatd_buildid "${ip}")" "${FIXED_BUILDID}"
done
scenario_end
