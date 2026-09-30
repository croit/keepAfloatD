#!/usr/bin/env bash
# D11: A six-second explicit failure delay keeps the VIP
# bound during a short probe failure. With failback:false, recovery does not preempt healthy
# holders, but the recovered node remains available when one of those holders later fails.
SCENARIO_NAME=D11_failover_delay_nopreempt
source "$(dirname "$0")/../scenario.sh"
scenario_start "failover delay and nopreempt fallback preserve service ownership"

configure_delayed_failover_policy() {
  local ip inst
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    node_sh "${ip}" "cfg=/etc/keepafloatd/config-${inst}.yaml; test ! -e \"\$cfg.d11-bak\"; cp \"\$cfg\" \"\$cfg.d11-bak\"; sed -i '/^failover_delay_secs:/d; /^failback:/d' \"\$cfg\"; printf '\\nfailover_delay_secs: 6\\nfailback: false\\n' >> \"\$cfg\""
  done
  clean_reform
}

restore_policy() {
  local ip inst
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    node_sh "${ip}" "cfg=/etc/keepafloatd/config-${inst}.yaml; test ! -f \"\$cfg.d11-bak\" || mv \"\$cfg.d11-bak\" \"\$cfg\""
  done
}

configure_delayed_failover_policy
check "cluster reforms with delayed failover and nopreempt" wait_for_steady_state

victim_still_has_vip() {
  snapshot_vips
  node_has_vip_bound "${victim}" "${victim_vip}"
}

snapshot_vips
victim="${NODE_IPS[0]}"
victim_vip=""
for vip in "${VIPS[@]}"; do
  node_has_vip_bound "${victim}" "${vip}" && victim_vip="${vip}"
done
check "victim starts with a VIP" test -n "${victim_vip}"

evid "failing ${victim}; ${victim_vip} must stay bound inside the six-second delay"
set_unhealthy_async "${victim}" timed
failure_context="${HEALTH_FAILURE_CONTEXT}"
check "VIP stays bound throughout the first four seconds" holds_for 4 \
  victim_still_has_vip
check "victim sheds VIPs after failover delay" wait_until 30 node_lacks_all_vips "${victim}"
failure_elapsed_ms=""
capture_failure_release_time() {
  failure_elapsed_ms="$(vip_event_elapsed_ms "${victim}" "${failure_context}" unbound "${VIPS[@]}")"
}
check "first VIP release has node-local journal timing evidence" \
  wait_until 5 capture_failure_release_time
if [[ "${failure_elapsed_ms}" =~ ^[0-9]+$ ]]; then
  evid "first release occurred ${failure_elapsed_ms}ms after failure injection"
  check "release is not earlier than configured six-second delay" \
    test "${failure_elapsed_ms}" -ge 6000
fi

replacement=""
capture_replacement() {
  local candidate
  snapshot_vips
  candidate="$(holder_for_vip "${victim_vip}")"
  case "${candidate}" in
    none | duplicate:*) return 1 ;;
    *) replacement="${candidate}" ;;
  esac
}
check "failed VIP has one replacement holder" wait_until 30 capture_replacement
if [[ -z "${replacement}" ]]; then
  set_healthy "${victim}"
  restore_policy
  clean_reform
  scenario_end
fi

evid "recovering ${victim}; failback:false must not preempt ${replacement}"
set_healthy "${victim}"
sleep 12
check "recovered node remains VIP-less while holders are healthy" node_lacks_all_vips "${victim}"

evid "failing replacement ${replacement}; recovered ${victim} must accept an orphan"
set_unhealthy_async "${replacement}"
check "recovered nopreempt node accepts an orphan" wait_until 30 node_has_any_vip "${victim}"
# Release acknowledgements are per VIP, so the replacement can finish multiple safe handoffs in
# separate commits. The first orphan reaching the recovered node does not imply that every other
# VIP has crossed its release fence in the same polling instant.
check "all VIPs converge to unique holders" wait_until 30 all_vips_uniquely_held
check "converged VIP ownership has no duplicates" assert_unique_holders

for ip in "${NODE_IPS[@]}"; do set_healthy "${ip}"; done
restore_policy
clean_reform
scenario_end
