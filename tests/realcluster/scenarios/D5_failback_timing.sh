#!/usr/bin/env bash
# D5: Failback (preempt) timing. With failback:true (default) and failback_delay_secs, a
# recovered node only regains eligibility after it has been continuously healthy for the
# delay. We make a node unhealthy with its sentinel so it loses its VIPs, restore health, and assert
# it does NOT immediately reclaim a VIP - it waits ~failback_delay before re-entering.
#
SCENARIO_NAME=D5_failback_timing
source "$(dirname "$0")/../scenario.sh"
scenario_start "recovered node observes failback delay before reclaiming VIPs"

tag=d5
delay_secs=6
backup_cluster_configs "${tag}"
clear_health_sentinels
configure_sentinel_health 500 100 3
set_cluster_scalar failover_delay_secs 0
set_cluster_scalar failback true
set_cluster_scalar failback_delay_secs "${delay_secs}"
clean_reform
check "cluster steady with an explicit nonzero failback delay" wait_for_steady_state

snapshot_vips
ip="$(holder_for_vip "${VIPS[0]}")"
check "timing victim starts as a concrete VIP holder" instance_for_ip "${ip}"
if ! instance_for_ip "${ip}" >/dev/null; then
  clear_health_sentinels
  restore_cluster_configs "${tag}"
  clean_reform
  scenario_end
fi
evid "node ${ip} configured failback_delay_secs=${delay_secs}"
check "failback timing fixture has a meaningful delay" test "${delay_secs}" -ge 5

evid "making ${ip} unhealthy so it sheds its VIPs"
sentinel_fail "${ip}"
check "node ${ip} sheds its VIPs while unhealthy" wait_until 60 node_lacks_all_vips "${ip}"

evid "restoring health on ${ip}; it must wait ~${delay_secs}s before reclaiming"
recovery_context="$(timed_sentinel_recover "${ip}")"
check "node remains VIP-less throughout the first four recovery seconds" \
  holds_for 4 node_lacks_all_vips "${ip}"

check "node eventually regains a VIP" wait_until 120 node_has_any_vip "${ip}"
recovery_elapsed_ms=""
capture_first_bind_time() {
  recovery_elapsed_ms="$(vip_bind_elapsed_ms "${ip}" "${recovery_context}")"
}
check "first returning VIP has node-local journal timing evidence" \
  wait_until 5 capture_first_bind_time
if [[ "${recovery_elapsed_ms}" =~ ^[0-9]+$ ]]; then
  evid "first VIP returned ${recovery_elapsed_ms}ms after health restoration"
  check "first failback is not earlier than ${delay_secs}s" \
    test "${recovery_elapsed_ms}" -ge "$(( delay_secs * 1000 ))"
fi
check "node re-enters even spread after failback delay" wait_for_even 60 "${NODE_IPS[@]}"

clear_health_sentinels
restore_cluster_configs "${tag}"
clean_reform
scenario_end
