#!/usr/bin/env bash
# D19: Turn two nodes into a matching new-config majority while a current VIP owner keeps running
# the old config. Its original process must confirm the fatal mismatch and release its VIPs,
# either on quorum loss or during shutdown, without rebinding. Exact repair lets all nodes rejoin.
SCENARIO_NAME=D19_config_identity
source "$(dirname "$0")/../scenario.sh"
scenario_start "old-config VIP owner releases its addresses, confirms mismatch and repairs cleanly"

tag=d19
backup_cluster_configs "${tag}"

snapshot_vips
victim="$(holder_for_vip "${VIPS[0]}")"
check "baseline victim is a concrete VIP owner" test "${victim}" != "none"
peers=( $(nodes_except "${victim}") )

first_peer_inst="$(instance_for_ip "${peers[0]}")"
first_peer_cfg="/etc/keepafloatd/config-${first_peer_inst}.yaml"
original_stale="$(node_sh "${peers[0]}" "sed -n 's/^  stale_secs: \([0-9][0-9]*\).*/\1/p' ${first_peer_cfg}")"
check "baseline config has an explicit stale_secs" test -n "${original_stale}"
interval_ms="$(node_sh "${peers[0]}" "sed -n 's/^  interval_ms: \([0-9][0-9]*\).*/\1/p' ${first_peer_cfg}")"
check "baseline config has an explicit probe interval" test -n "${interval_ms}"
mismatched_stale="$(next_behavior_changing_stale_secs "${interval_ms}" "${original_stale}")"
mismatch_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
mismatch_context="$(timed_node_command "${victim}" :)"
snapshot_vips
original_vips=()
for vip in "${VIPS[@]}"; do
  if node_has_vip_bound "${victim}" "${vip}"; then original_vips+=("${vip}"); fi
done
check "original process still owns VIPs before config mutation" test "${#original_vips[@]}" -gt 0

for peer in "${peers[@]}"; do
  peer_inst="$(instance_for_ip "${peer}")"
  peer_cfg="/etc/keepafloatd/config-${peer_inst}.yaml"
  evid "changing ${peer} health.stale_secs ${original_stale} -> ${mismatched_stale}"
  node_sh "${peer}" \
    "sed -i 's/^  stale_secs: .*/  stale_secs: ${mismatched_stale}/' ${peer_cfg}"
done

# Sequential replacement keeps the old-config pair quorate until the second restart, then creates
# one coherent new-config majority around the still-running old-config owner.
kafd_restart "${peers[0]}"
sleep 2
kafd_restart "${peers[1]}"

check "new-config majority completes its startup fence without overlapping VIPs" \
  wait_for_startup_activation 30 "${peers[@]}"
check "new-config majority serves every VIP uniquely and reachably" \
  wait_for_live_service_without 90 "${victim}"
check "old-config owner leaves no kernel VIP after fatal cleanup" \
  holds_for 12 node_lacks_all_vips "${victim}"
check "original process ends with confirmed config mismatch and every owned VIP released" \
  wait_until 30 config_fence_observed "${victim}" "${mismatch_context}" "${original_vips[@]}"

for peer in "${peers[@]}"; do
  peer_exit="$(journal_event_count "${peer}" "${mismatch_since}" \
    'configuration mismatch confirmed; shutting down safely')"
  check_eq "new-config peer ${peer} does not self-fence after majority forms" "${peer_exit}" "0"
done

evid "restoring exact baseline configs and cleanly reforming"
restore_cluster_configs "${tag}"
clean_reform
check "repaired node rejoins the existing cluster" wait_until 60 node_active "${victim}"
check "all three nodes return to safe unique service" wait_for_available_cluster

scenario_end
