#!/usr/bin/env bash
# A3: VLAN sub-interface binding. keepafloatd's `vlan` field makes `ip addr` operations target
# `{interface}.{vlan}` (config.rs sorted_vips). The sub-interface must pre-exist. We pre-create
# `<base>.<vlan>` on every node, add a vlan-tagged VIP to each config, restart, and assert the VIP
# binds on the SUB-INTERFACE (not the base). Then we revert config + tear down the sub-interface.
SCENARIO_NAME=A3_vlan_binding
source "$(dirname "$0")/../scenario.sh"
scenario_start "VLAN-tagged VIP binds on ${IFACE}.<vlan> sub-interface"

# Use a configured disposable VLAN and address that do not collide with cluster networking.
VLAN="${VLAN_TEST_ID:?VLAN_TEST_ID is required}"
VLAN_VIP="${VLAN_TEST_VIP:?VLAN_TEST_VIP is required}"
SUBIF="${IFACE}.${VLAN}"

# Config edits use sed line-deletion (robust through the two-hop base64 transport). The injected
# block is exactly 3 lines anchored on the VLAN_VIP address line.
revert() {
  local ip inst
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    node_sh "${ip}" "cfg=/etc/keepafloatd/config-${inst}.yaml; sed -i '/address: \"${VLAN_VIP}\"/,+2d' \$cfg"
  done
  clean_reform
  cleanup_ownership_test_artifacts
}

# Pre-create the VLAN sub-interface + inject the vlan VIP into every node's config (3-line block
# appended after the 'vips:' key).
for ip in "${NODE_IPS[@]}"; do
  inst="$(instance_for_ip "${ip}")"
  node_sh "${ip}" "modprobe 8021q 2>/dev/null; ip link show ${SUBIF} >/dev/null 2>&1 || ip link add link ${IFACE} name ${SUBIF} type vlan id ${VLAN}; ip link set ${SUBIF} up; cfg=/etc/keepafloatd/config-${inst}.yaml; grep -q '${VLAN_VIP}' \$cfg || sed -i '/^vips:/a\\  - address: \"${VLAN_VIP}\"\\n    interface: ${IFACE}\\n    vlan: ${VLAN}' \$cfg"
done
clean_reform
evid "injected VLAN VIP ${VLAN_VIP} (vlan ${VLAN}) on all nodes; sub-interface ${SUBIF} created"

check "cluster healthy with the extra VLAN VIP" wait_until 60 single_agreed_leader

# The VLAN VIP must settle on exactly one node on the sub-interface and never the base interface.
# Do not capture the first observed holder: four-VIP balancing may move it once during convergence.
vlan_placement_correct() {
  [[ "$(ipv4_bound_count "${VLAN_VIP}" 32 "${SUBIF}")" -eq 1 ]] \
    && [[ "$(ipv4_bound_count "${VLAN_VIP}" 32 "${IFACE}")" -eq 0 ]]
}
check "VLAN VIP converges uniquely on ${SUBIF}, stays there for five seconds, and is absent from ${IFACE}" \
  wait_until 60 holds_for 5 vlan_placement_correct

vlan_holder="$(wait_for_ipv4_holder 15 "${VLAN_VIP}" 32 "${SUBIF}")"
evid "VLAN VIP ${VLAN_VIP} holder: ${vlan_holder:-none}"
check "VLAN VIP bound on a node" test -n "${vlan_holder}"

evid "reverting VLAN config + tearing down ${SUBIF}"
revert
check "baseline VIPs even after VLAN revert" wait_for_even 60 "${NODE_IPS[@]}"

scenario_end
