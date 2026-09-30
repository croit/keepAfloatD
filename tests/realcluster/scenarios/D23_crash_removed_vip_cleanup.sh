#!/usr/bin/env bash
# D23: Cross the two lifecycle events that A5 and C5 cover separately. A holder is SIGKILLed,
# its marked VIP is removed from every config while every daemon is down, and that node starts
# alone. Startup must remove the no-longer-configured marked address before a majority can form,
# while leaving an unmarked administrator address and another instance's marker untouched.
SCENARIO_NAME=D23_crash_removed_vip_cleanup
source "$(dirname "$0")/../scenario.sh"
scenario_start "crash-time config removal reclaims only keepafloatd-marked addresses"

removed_vip="${MUTATION_TEST_VIP:?MUTATION_TEST_VIP is required}"
admin_vip="${OWNERSHIP_ADMIN_VIP:?OWNERSHIP_ADMIN_VIP is required}"
foreign_vip="${OWNERSHIP_FOREIGN_VIP:?OWNERSHIP_FOREIGN_VIP is required}"
foreign_protocol="${OWNERSHIP_FOREIGN_PROTOCOL:?OWNERSHIP_FOREIGN_PROTOCOL is required}"
marker_table_base=10000
address_protocol=246
[[ "${foreign_protocol}" =~ ^[0-9]+$ ]] && (( foreign_protocol >= 1 && foreign_protocol <= 255 ))
tag=d23
backup_live=0

marker_table_for_protocol() {
  printf '%d\n' "$((marker_table_base + ${1:?protocol required}))"
}

remove_disposable_addresses() {
  cleanup_ownership_test_artifacts
}

restore_d23() {
  local status=$?
  trap - ERR EXIT
  if [[ "${backup_live}" -eq 1 ]]; then
    for ip in "${NODE_IPS[@]}"; do kafd_stop "${ip}" || status=1; done
    remove_disposable_addresses || status=1
    restore_cluster_configs "${tag}" || status=1
    clean_reform || status=1
    backup_live=0
  fi
  exit "${status}"
}
trap restore_d23 EXIT

add_removed_vip_to_configs() {
  local ip inst
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    node_sh "${ip}" "
      cfg=/etc/keepafloatd/config-${inst}.yaml
      grep -F -q 'address: \"${removed_vip}\"' \"\$cfg\" ||
        sed -i '/^vips:/a\\  - address: \"${removed_vip}\"\\n    interface: ${IFACE}' \"\$cfg\"
    "
  done
}

remove_vip_from_configs() {
  local ip inst
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    node_sh "${ip}" "
      cfg=/etc/keepafloatd/config-${inst}.yaml
      sed -i '/address: \"${removed_vip}\"/,+1d' \"\$cfg\"
    "
  done
}

address_has_marker() {
  local ip="${1:?}" address="${2:?}" protocol="${3:?}" protocol_hex
  printf -v protocol_hex '0x%02x' "${protocol}"
  local marker_table
  marker_table="$(marker_table_for_protocol "${protocol}")"
  node_sh "${ip}" "
    set -o pipefail
    ip -j -4 addr show dev ${IFACE} to ${address}/32 |
      jq -e '[.[].addr_info[]? | select(.local == \"${address}\" and .prefixlen == 32)] |
        length == 1' >/dev/null &&
    ip -N -j -4 route show table all |
      jq -e '[.[] | select(
        ((.type | tostring) == \"9\" or .type == \"throw\") and
        (.table | tostring) == \"${marker_table}\" and
        (.dst == \"${address}\" or .dst == \"${address}/32\") and
        (.protocol == \"${protocol_hex}\" or .protocol == \"${protocol}\" or .protocol == ${protocol})
      )] | length == 1' >/dev/null
  "
}

address_present_without_marker() {
  local ip="${1:?}" address="${2:?}"
  local own_table foreign_table
  own_table="$(marker_table_for_protocol "${address_protocol}")"
  foreign_table="$(marker_table_for_protocol "${foreign_protocol}")"
  node_sh "${ip}" "
    set -o pipefail
    ip -j -4 addr show dev ${IFACE} to ${address}/32 |
      jq -e '[.[].addr_info[]? | select(.local == \"${address}\" and .prefixlen == 32)] as \$a |
        (\$a | length == 1)' >/dev/null &&
    ip -N -j -4 route show table all |
      jq -e '[.[] | select(
        ((.table | tostring) == \"${own_table}\" or (.table | tostring) == \"${foreign_table}\") and
        (.dst == \"${address}\" or .dst == \"${address}/32\"))] |
        length == 0' >/dev/null
  "
}

vip_and_marker_absent_everywhere() {
  [[ "$(ipv4_bound_count "${removed_vip}" 32 "${IFACE}")" == "0" ]] || return 1
  local ip marker_table
  marker_table="$(marker_table_for_protocol "${address_protocol}")"
  for ip in "${NODE_IPS[@]}"; do
    node_sh "${ip}" "
      set -o pipefail
      ip -N -j -4 route show table all |
        jq -e '[.[] | select((.table | tostring) == \"${marker_table}\" and
          (.dst == \"${removed_vip}\" or .dst == \"${removed_vip}/32\"))] |
          length == 0' >/dev/null
    " || return 1
  done
}

only_restarted_holder_is_active() {
  local ip status active=0
  for ip in "${NODE_IPS[@]}"; do
    status="$(kafd_active "${ip}")" || return 1
    [[ "${status}" == active || "${status}" == inactive ]] || return 1
    if [[ "${status}" == active ]]; then
      [[ "${ip}" == "${holder}" ]] || return 1
      active=$((active + 1))
    fi
  done
  [[ "${active}" -eq 1 ]]
}

backup_cluster_configs "${tag}"
backup_live=1
add_removed_vip_to_configs
clean_reform
# With failback disabled, a valid cold start may keep all four VIPs on the first healthy node.
# Baseline steady-state requires the original three VIPs to be evenly spread, so it is not a valid
# readiness oracle until the exact baseline config has been restored below.
check "cluster elects one leader with the temporary VIP configured" \
  wait_until 60 single_agreed_leader

holder="$(wait_for_ipv4_holder 40 "${removed_vip}" 32 "${IFACE}")"
holder_inst="$(instance_for_ip "${holder}")"
address_protocol="$(node_sh "${holder}" "sed -n 's/^address_protocol:[[:space:]]*//p' /etc/keepafloatd/config-${holder_inst}.yaml")"
address_protocol="${address_protocol:-246}"
[[ "${address_protocol}" =~ ^[0-9]+$ ]] && (( address_protocol >= 1 && address_protocol <= 255 ))
[[ "${address_protocol}" != "${foreign_protocol}" ]]
evid "temporary VIP ${removed_vip} holder: ${holder}"
check "new VIP is uniquely held" ipv4_vip_uniquely_held "${removed_vip}" 32 "${IFACE}"
check "keepafloatd address carries route ownership protocol ${address_protocol}" \
  address_has_marker "${holder}" "${removed_vip}" "${address_protocol}"

node_sh "${holder}" "ip -4 addr replace ${admin_vip}/32 dev ${IFACE}"
node_sh "${holder}" "
  ip -4 route replace table $(marker_table_for_protocol "${foreign_protocol}") \
    throw ${foreign_vip}/32 proto ${foreign_protocol}
  ip -4 addr replace ${foreign_vip}/32 dev ${IFACE}
"
check "administrator address is present without ownership marker" \
  address_present_without_marker "${holder}" "${admin_vip}"
check "other instance address carries a distinct ownership marker" \
  address_has_marker "${holder}" "${foreign_vip}" "${foreign_protocol}"

evid "SIGKILLing holder ${holder}, then removing ${removed_vip} from every config"
kafd_kill "${holder}"
check "SIGKILL leaves the marked address in the holder kernel" \
  address_has_marker "${holder}" "${removed_vip}" "${address_protocol}"
remove_vip_from_configs

# Stop the survivors before they can reassign the old-config VIP. Starting only the killed holder
# makes the ordering proof explicit: no Raft majority can exist while startup cleanup runs.
for ip in "${NODE_IPS[@]}"; do
  [[ "${ip}" == "${holder}" ]] || kafd_stop "${ip}"
done
kafd_start "${holder}"
check "restarted holder process is active" wait_until 20 node_active "${holder}"
check "only one of three voters is active, so no Raft majority exists during cleanup" \
  holds_for 5 only_restarted_holder_is_active
check "removed marked VIP and ownership route disappear before rejoin" \
  wait_until 20 vip_and_marker_absent_everywhere
check "unmarked administrator address survives startup cleanup" \
  address_present_without_marker "${holder}" "${admin_vip}"
check "other keepafloatd instance's marker survives startup cleanup" \
  address_has_marker "${holder}" "${foreign_vip}" "${foreign_protocol}"

# Restore the exact baseline before the generic scenario cleanup evaluates steady state.
for ip in "${NODE_IPS[@]}"; do kafd_stop "${ip}"; done
remove_disposable_addresses
restore_cluster_configs "${tag}"
backup_live=0
clean_reform
check "cluster returns to exact baseline behavior" wait_for_steady_state
trap - EXIT
scenario_end
