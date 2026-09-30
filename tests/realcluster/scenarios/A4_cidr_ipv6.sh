#!/usr/bin/env bash
# A4: CIDR-suffixed and IPv6 VIPs. VipAddr parses "IP/prefix" (default /32 v4, /128 v6). We add a
# /24 IPv4 VIP and an IPv6 VIP to the configs and assert each binds with the correct address
# family and prefix on the kernel (ip -4 / ip -6 addr), then revert.
SCENARIO_NAME=A4_cidr_ipv6
source "$(dirname "$0")/../scenario.sh"
scenario_start "CIDR /24 IPv4 VIP and IPv6 VIP bind with correct family + prefix"

CIDR_VIP="${CIDR_TEST_VIP:?CIDR_TEST_VIP is required}"
CIDR_PREFIX="${CIDR_TEST_PREFIX:?CIDR_TEST_PREFIX is required}"
V6_VIP="${IPV6_TEST_VIP:?IPV6_TEST_VIP is required}"

revert() {
  local ip inst
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    node_sh "${ip}" "cfg=/etc/keepafloatd/config-${inst}.yaml; sed -i '/address: \"${CIDR_VIP}\/${CIDR_PREFIX}\"/,+1d; \\#address: \"${V6_VIP}\"#,+1d' \$cfg"
  done
  clean_reform
  cleanup_ownership_test_artifacts
}

# Inject a /24 IPv4 VIP and an IPv6 VIP into every node's config (2-line blocks after 'vips:').
for ip in "${NODE_IPS[@]}"; do
  inst="$(instance_for_ip "${ip}")"
  node_sh "${ip}" "cfg=/etc/keepafloatd/config-${inst}.yaml; grep -q '${CIDR_VIP}/${CIDR_PREFIX}' \$cfg || sed -i '/^vips:/a\\  - address: \"${CIDR_VIP}/${CIDR_PREFIX}\"\\n    interface: ${IFACE}' \$cfg; grep -q '${V6_VIP}' \$cfg || sed -i '/^vips:/a\\  - address: \"${V6_VIP}\"\\n    interface: ${IFACE}' \$cfg"
done
clean_reform
evid "injected CIDR VIP ${CIDR_VIP}/${CIDR_PREFIX} and IPv6 VIP ${V6_VIP} on all nodes"

check "cluster healthy with CIDR + IPv6 VIPs" wait_until 60 single_agreed_leader

# CIDR /24 VIP: must be bound with /24 prefix (not /32) on exactly one node.
cidr_holder=""
for attempt in $(seq 1 30); do
  for ip in "${NODE_IPS[@]}"; do
    if node_sh "${ip}" "ip -o -4 addr show dev ${IFACE} 2>/dev/null | grep -F -q ' ${CIDR_VIP}/${CIDR_PREFIX} '"; then
      cidr_holder="${ip}"; break
    fi
  done
  [[ -n "${cidr_holder}" ]] && break
  sleep 2
done
evid "CIDR VIP ${CIDR_VIP}/${CIDR_PREFIX} holder: ${cidr_holder:-none}"
check "CIDR VIP bound with /${CIDR_PREFIX} prefix (not /32)" test -n "${cidr_holder}"
check "CIDR VIP has exactly one kernel holder" wait_until 30 ipv4_vip_uniquely_held \
  "${CIDR_VIP}" "${CIDR_PREFIX}" "${IFACE}"

# IPv6 VIP: must be bound via ip -6 on exactly one node (/128 default).
v6_holder=""
for attempt in $(seq 1 30); do
  for ip in "${NODE_IPS[@]}"; do
    if node_sh "${ip}" "ip -o -6 addr show dev ${IFACE} 2>/dev/null | grep -F -q ' ${V6_VIP}/128 '"; then
      v6_holder="${ip}"; break
    fi
  done
  [[ -n "${v6_holder}" ]] && break
  sleep 2
done
evid "IPv6 VIP ${V6_VIP}/128 holder: ${v6_holder:-none}"
check "IPv6 VIP bound via ip -6 with /128" test -n "${v6_holder}"
check "IPv6 VIP has exactly one kernel holder" wait_until 30 ipv6_vip_uniquely_held \
  "${V6_VIP}" 128 "${IFACE}"

evid "reverting CIDR + IPv6 VIPs"
revert
check "baseline VIPs even after revert" wait_for_even 60 "${NODE_IPS[@]}"

scenario_end
