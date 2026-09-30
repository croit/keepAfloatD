#!/usr/bin/env bash
# D13: Fractional-interval failover timing and oscillation reset. interval_ms=500 converts a
# three-second delay to six full probe rounds. A short failed streak must not release the VIP, a
# healthy probe must reset it, and the next continuous streak must wait the full delay again.
SCENARIO_NAME=D13_failure_delay_reset
source "$(dirname "$0")/../scenario.sh"
scenario_start "500ms failover rounds reset after an interrupted failure streak"

tag=d13
backup_cluster_configs "${tag}"
clear_health_sentinels
configure_sentinel_health 500 100 3
set_cluster_scalar failover_delay_secs 3
set_cluster_scalar failback true
set_cluster_scalar failback_delay_secs 0
clean_reform
check "cluster steady with 500ms probes" wait_for_steady_state

victim="${NODE_IPS[0]}"
snapshot_vips
victim_vip=""
for vip in "${VIPS[@]}"; do
  node_has_vip_bound "${victim}" "${vip}" && victim_vip="${vip}"
done
check "timing victim starts with a VIP" test -n "${victim_vip}"

victim_still_has_vip() {
  node_sh "${victim}" \
    "ip -o -4 addr show dev ${IFACE} | grep -F -q ' ${victim_vip}/32 '"
}

victim_unbind_count() {
  journal_event_count_all "${victim}" "unbound ${victim_vip}/32"
}

evid "first failure streak spans multiple probes but remains below the configured three seconds"
unbinds_before="$(victim_unbind_count)"
first_failure_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
sentinel_fail "${victim}"
sleep 1.2
sentinel_recover "${victim}"
first_failure_log="$(kafd_log_since "${victim}" "${first_failure_since}" | grep -F 'health probe failed; delaying failover' | tail -1)"
check "the interrupted streak contains a current-run failed probe" test -n "${first_failure_log}"
# Four successful 500ms opportunities are enough to observe and reset the local streak.
sleep 2
check "VIP remains on the recovered node after the interrupted streak" victim_still_has_vip
check_eq "short first streak emitted no VIP release" \
  "$(victim_unbind_count)" "${unbinds_before}"

evid "starting second continuous failure; prior failed rounds must not carry over"
second_context="$(timed_sentinel_fail "${victim}")"
sleep 1
check "second streak still holds the VIP before its delay" victim_still_has_vip
check "second streak eventually releases the VIP" wait_until 15 node_lacks_all_vips "${victim}"
second_elapsed_ms=""
capture_second_release_time() {
  second_elapsed_ms="$(vip_release_elapsed_ms "${victim}" "${victim_vip}" "${second_context}")"
}
check "second release has node-local journal timing evidence" \
  wait_until 5 capture_second_release_time
if [[ "${second_elapsed_ms}" =~ ^[0-9]+$ ]]; then
  evid "second continuous streak released after ${second_elapsed_ms}ms"
  check "reset streak observes the full three-second lower bound" \
    test "${second_elapsed_ms}" -ge 3000
  check "500ms round conversion releases within a bounded window" \
    test "${second_elapsed_ms}" -le 9000
fi
check "all VIPs move to healthy nodes uniquely" wait_for_even 30 $(nodes_except "${victim}")

clear_health_sentinels
restore_cluster_configs "${tag}"
clean_reform
scenario_end
