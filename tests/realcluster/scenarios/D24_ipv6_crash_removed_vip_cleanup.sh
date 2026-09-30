#!/usr/bin/env bash
# D24: Real-kernel IPv6 counterpart to D23. A SIGKILL orphan is removed from YAML while every
# daemon is stopped; the killed holder must reclaim both the /128 and its route marker before it
# can rejoin a quorum.
SCENARIO_NAME=D24_ipv6_crash_removed_vip_cleanup
source "$(dirname "$0")/../scenario.sh"
scenario_start "IPv6 crash orphan removed from config is reclaimed before rejoin"

removed_vip="${IPV6_TEST_VIP:?IPV6_TEST_VIP is required}"
marker_table_base=10000
address_protocol=246
tag=d24
backup_live=0

marker_table_for_protocol() {
  printf '%d\n' "$((marker_table_base + ${1:?protocol required}))"
}

remove_disposable_ipv6() {
  cleanup_ownership_test_artifacts
}

restore_d24() {
  local status=$?
  trap - ERR EXIT INT TERM
  if [[ "${backup_live}" -eq 1 ]]; then
    for ip in "${NODE_IPS[@]}"; do kafd_stop "${ip}" || status=1; done
    remove_disposable_ipv6 || status=1
    restore_cluster_configs "${tag}" || status=1
    clean_reform || status=1
    backup_live=0
  fi
  exit "${status}"
}
trap restore_d24 EXIT

add_ipv6_to_configs() {
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

remove_ipv6_from_configs() {
  local ip inst
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    node_sh "${ip}" "sed -i '/address: \"${removed_vip}\"/,+1d' /etc/keepafloatd/config-${inst}.yaml"
  done
}

ipv6_has_marker() {
  local ip="${1:?}" protocol="${2:?}" protocol_hex marker_table
  printf -v protocol_hex '0x%02x' "${protocol}"
  marker_table="$(marker_table_for_protocol "${protocol}")"
  node_sh "${ip}" "
    set -o pipefail
    ip -j -6 addr show dev ${IFACE} to ${removed_vip}/128 |
      jq -e '[.[].addr_info[]? | select(.local == \"${removed_vip}\" and .prefixlen == 128)] |
        length == 1' >/dev/null &&
    ip -N -j -6 route show table all |
      jq -e '[.[] | select(
        ((.type | tostring) == \"9\" or .type == \"throw\") and
        (.table | tostring) == \"${marker_table}\" and
        (.dst == \"${removed_vip}\" or .dst == \"${removed_vip}/128\") and
        (.protocol == \"${protocol_hex}\" or .protocol == \"${protocol}\" or
          .protocol == ${protocol})
      )] | length == 1' >/dev/null
  "
}

find_ipv6_holder() {
  local ip count holder=""
  for ip in "${NODE_IPS[@]}"; do
    count="$(node_sh "${ip}" "set -o pipefail; ip -N -j -6 addr show dev ${IFACE} | jq -r '[.[].addr_info[]? | select(.local == \"${removed_vip}\" and .prefixlen == 128)] | length'")" || return 1
    [[ "${count}" =~ ^[0-9]+$ && "${count}" -le 1 ]] || return 1
    if [[ "${count}" -eq 1 ]]; then
      [[ -z "${holder}" ]] || return 1
      holder="${ip}"
    fi
  done
  [[ -n "${holder}" ]] || return 1
  printf '%s\n' "${holder}"
}

wait_for_ipv6_holder() {
  local deadline=$(( $(date +%s) + 40 )) holder
  while true; do
    if holder="$(find_ipv6_holder)"; then
      printf '%s\n' "${holder}"
      return 0
    fi
    (( $(date +%s) >= deadline )) && return 1
    sleep 2
  done
}

ipv6_and_marker_absent_everywhere() {
  [[ "$(ipv6_bound_count "${removed_vip}" 128 "${IFACE}")" == 0 ]] || return 1
  local ip marker_table
  marker_table="$(marker_table_for_protocol "${address_protocol}")"
  for ip in "${NODE_IPS[@]}"; do
    node_sh "${ip}" "
      set -o pipefail
      ip -N -j -6 route show table all |
        jq -e '[.[] | select((.table | tostring) == \"${marker_table}\" and
          (.dst == \"${removed_vip}\" or .dst == \"${removed_vip}/128\"))] |
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

backup_cluster_configs "${tag}" || {
  evid "config capture failed; recovery was not started"
  exit 1
}
backup_live=1
add_ipv6_to_configs
clean_reform
check "cluster elects one leader with the temporary IPv6 VIP" \
  wait_until 60 single_agreed_leader

holder="$(wait_for_ipv6_holder)"
holder_inst="$(instance_for_ip "${holder}")"
address_protocol="$(node_sh "${holder}" "sed -n 's/^address_protocol:[[:space:]]*//p' /etc/keepafloatd/config-${holder_inst}.yaml")"
address_protocol="${address_protocol:-246}"
[[ "${address_protocol}" =~ ^[0-9]+$ ]] && (( address_protocol >= 1 && address_protocol <= 255 ))
evid "temporary IPv6 VIP ${removed_vip} holder: ${holder}"
check "IPv6 VIP and marker are uniquely present on its holder" \
  ipv6_has_marker "${holder}" "${address_protocol}"

evid "SIGKILLing ${holder}, then removing ${removed_vip} from every config"
kafd_kill "${holder}"
check "SIGKILL leaves the marked IPv6 address in the holder kernel" \
  ipv6_has_marker "${holder}" "${address_protocol}"
remove_ipv6_from_configs

for ip in "${NODE_IPS[@]}"; do
  [[ "${ip}" == "${holder}" ]] || kafd_stop "${ip}"
done
kafd_start "${holder}"
check "restarted IPv6 holder process becomes active" wait_until 20 node_active "${holder}"
check "only one of three voters is active during IPv6 startup cleanup" \
  holds_for 5 only_restarted_holder_is_active
check "removed IPv6 address and ownership marker disappear before rejoin" \
  wait_until 20 ipv6_and_marker_absent_everywhere

for ip in "${NODE_IPS[@]}"; do kafd_stop "${ip}"; done
remove_disposable_ipv6
restore_cluster_configs "${tag}"
backup_live=0
clean_reform
check "cluster returns to exact baseline behavior" wait_for_steady_state
trap - EXIT
scenario_end
