#!/usr/bin/env bash
# D14: Silent failure must create the same failback:false history as an explicit health failure.
# A SIGKILLed owner recovers without preempting healthy holders, yet remains eligible for a later
# orphan when one of those holders fails.
SCENARIO_NAME=D14_silent_nopreempt_fallback
source "$(dirname "$0")/../scenario.sh"
scenario_start "silent-death recovery is nopreempt but still accepts an orphan"

tag=d14
backup_cluster_configs "${tag}"
clear_health_sentinels
configure_sentinel_health 500 100 3
set_cluster_scalar failover_delay_secs 0
set_cluster_scalar failback false
set_cluster_scalar failback_delay_secs 0
clean_reform
check "cluster steady before silent death" wait_for_steady_state

victim="${NODE_IPS[0]}"
snapshot_vips
victim_vip=""
for vip in "${VIPS[@]}"; do
  node_has_vip_bound "${victim}" "${vip}" && victim_vip="${vip}"
done
check "silent-death victim starts with a VIP" test -n "${victim_vip}"

evid "SIGKILLing ${victim}; no unhealthy report will be published"
kafd_kill "${victim}"
check "silent owner is staled and every VIP gains a live holder" \
  wait_until 30 vips_uniquely_served_by_live "${victim}"

replacement=""
capture_live_replacement() {
  local ip found=""
  snapshot_vips
  for ip in $(nodes_except "${victim}"); do
    if node_has_vip_bound "${ip}" "${victim_vip}"; then
      [[ -z "${found}" ]] || return 1
      found="${ip}"
    fi
  done
  [[ -n "${found}" ]] || return 1
  replacement="${found}"
}
check "the silently orphaned VIP has exactly one live replacement" \
  wait_until 20 capture_live_replacement
evid "${victim_vip} live replacement is ${replacement:-none}"

kafd_start "${victim}"
check "startup cleanup removes the killed node's orphan" wait_until 30 all_vips_uniquely_held
check "silent-recovered node does not preempt healthy holders" holds_for 6 \
  node_lacks_all_vips "${victim}"

evid "failing replacement ${replacement}; recovered node must accept the new orphan"
sentinel_fail "${replacement}"
check "silent-recovered nopreempt node accepts an orphan" \
  wait_until 30 node_has_any_vip "${victim}"
check "all VIPs converge to unique holders during fallback" \
  wait_until 30 all_vips_uniquely_held
check "converged fallback ownership has no duplicates" assert_unique_holders

clear_health_sentinels
restore_cluster_configs "${tag}"
clean_reform
scenario_end
