#!/usr/bin/env bash
# D20: Ignored failback delays and raw stale seconds with the same missed-round threshold are
# semantically equivalent. The nodes must share one effective identity and form a healthy cluster.
SCENARIO_NAME=D20_config_identity_equivalence
source "$(dirname "$0")/../scenario.sh"
scenario_start "nopreempt nodes ignore unequal failback delays in config identity"

tag=d20
backup_cluster_configs "${tag}"
equivalence_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"

# Use a deterministic interval with two distinct whole-second windows in the same missed-probe
# bucket. A 1000ms interval cannot express that fixture because every second changes the bucket.
interval_ms=2500
base_stale=3
equivalent_stale="$(next_behaviorally_equivalent_stale_secs "${interval_ms}" "${base_stale}")"
check "fixture has two unequal but behaviorally equivalent stale windows" \
  test "${equivalent_stale}" -gt "${base_stale}"
clear_health_sentinels
configure_sentinel_health "${interval_ms}" 500 "${base_stale}"

for index in "${!NODE_IPS[@]}"; do
  ip="${NODE_IPS[${index}]}"
  inst="$(instance_for_ip "${ip}")"
  cfg="/etc/keepafloatd/config-${inst}.yaml"
  delay=$((index == 2 ? 99 : 1))
  stale=$((index == 2 ? equivalent_stale : base_stale))
  evid "setting ${ip} failback=false, delay=${delay}, interval=${interval_ms}ms, stale=${stale}s"
  node_sh "${ip}" \
    "sed -i '/^failback:/d; /^failback_delay_secs:/d; s/^  stale_secs: .*/  stale_secs: ${stale}/' ${cfg}; printf '\\nfailback: false\\nfailback_delay_secs: ${delay}\\n' >> ${cfg}"
done

clean_reform
check "all three equivalent-config daemons remain active" wait_until 30 all_daemons_active
check "equivalent nopreempt configs form one steady cluster" wait_for_steady_state
check "equivalent nopreempt configs hold every VIP uniquely" all_vips_uniquely_held

for ip in "${NODE_IPS[@]}"; do
  mismatch_count="$(journal_event_count "${ip}" "${equivalence_since}" \
    'cluster configuration mismatch confirmed')"
  check_eq "${ip} does not trigger the config mismatch fence" "${mismatch_count}" "0"
done

evid "restoring exact baseline configs and cleanly reforming"
restore_cluster_configs "${tag}"
clean_reform
check "all three nodes remain active after exact config restore" wait_until 30 all_daemons_active
check "restored cluster returns to steady unique ownership" wait_for_steady_state

scenario_end
