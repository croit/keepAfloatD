#!/usr/bin/env bash
# C5: Add and remove a VIP using coordinated restarts, then verify its unique ownership and
# removal from every configured node. An unreadable node is not proof of absence.
#
# Edit node configs directly to test daemon configuration changes independently of the
# management API: the additional VIP must be distributed and uniquely held.
SCENARIO_NAME=C5_config_mutation
source "$(dirname "$0")/../scenario.sh"
scenario_start "adding/removing a VIP in keepafloatd configs redistributes cleanly"

NEW_VIP="${MUTATION_TEST_VIP:?MUTATION_TEST_VIP is required}"

add_vip() {   # inject NEW_VIP into every node config + restart
  local ip inst
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    node_sh "${ip}" "cfg=/etc/keepafloatd/config-${inst}.yaml; grep -q '${NEW_VIP}' \$cfg || sed -i '/^vips:/a\\  - address: \"${NEW_VIP}\"\\n    interface: ${IFACE}' \$cfg"
  done
  clean_reform
}
remove_vip() {  # remove NEW_VIP from every node config + restart
  local ip inst
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    node_sh "${ip}" "cfg=/etc/keepafloatd/config-${inst}.yaml; sed -i '/address: \"${NEW_VIP}\"/,+1d' \$cfg"
  done
  clean_reform
}

evid "adding 4th VIP ${NEW_VIP} to all node configs"
add_vip
check "cluster healthy after adding VIP" wait_until 60 single_agreed_leader

# The new VIP must appear on exactly one node and existing 3 stay reachable.
new_held=""
for attempt in $(seq 1 40); do
  for ip in "${NODE_IPS[@]}"; do
    node_sh "${ip}" "ip -o -4 addr show dev ${IFACE} | grep -F -q ' ${NEW_VIP}/32 '" && { new_held="${ip}"; break; }
  done
  [[ -n "${new_held}" ]] && break
  sleep 3
done
evid "new VIP ${NEW_VIP} holder: ${new_held:-none}"
check "new VIP bound on a node" test -n "${new_held}"
check "new VIP has exactly one kernel holder" wait_until 30 ipv4_vip_uniquely_held \
  "${NEW_VIP}" 32 "${IFACE}"
check "original VIPs still all pingable" wait_until 15 all_vips_pingable

evid "removing the 4th VIP again"
remove_vip

# The removed VIP must be gone from all nodes; the 3 originals even.
vip_absent_everywhere() {
  local target="${1:?}" holder
  holder="$(ipv4_holder_on_interface "${target}" 32 "${IFACE}")" || return 1
  [[ "${holder}" == none ]]
}
check "removed VIP ${NEW_VIP} gone from all nodes" wait_until 40 vip_absent_everywhere "${NEW_VIP}"
check "baseline 3 VIPs even after removal" wait_for_even 60 "${NODE_IPS[@]}"

scenario_end
