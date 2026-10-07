#!/usr/bin/env bash
# A1: Real kernel ip addr bind/unbind. When a holder's keepafloatd stops, the VIP must
# actually leave the kernel (ip addr del) and re-appear on a new holder (ip addr add).
SCENARIO_NAME=A1_ip_bind_unbind
source "$(dirname "$0")/../scenario.sh"
scenario_start "real ip addr bind/unbind on failover"

snapshot_vips
victim="$(holder_for_vip "${VIPS[0]}")"
vip="${VIPS[0]}"
evid "VIP ${vip} currently on ${victim}; stopping its keepafloatd"

# Confirm the VIP is really in the kernel on the victim first.
check "VIP ${vip} present in kernel on ${victim}" node_has_vip_bound "${victim}" "${vip}"

kafd_stop "${victim}"

# The VIP must leave the victim's kernel and land on exactly one other node.
check "victim ${victim} released all VIPs from kernel" wait_until 30 node_lacks_all_vips "${victim}"
check "VIPs uniquely held and reachable on survivors" \
  wait_for_live_service_without 40 "${victim}"
snapshot_vips
newholder="$(holder_for_vip "${vip}")"
evid "VIP ${vip} moved to ${newholder}"
check "VIP ${vip} now in kernel on ${newholder}" node_has_vip_bound "${newholder}" "${vip}"
check "VIP ${vip} reachable after move" vip_pingable "${vip}"

# Bring the victim back. With failback disabled it must not preempt a healthy survivor, but every
# VIP must remain uniquely held and reachable while the daemon rejoins.
safe_rejoin() {
  all_daemons_active && all_vips_uniquely_held && all_vips_pingable
}
kafd_start "${victim}"
check "restarted holder completes activation without overlapping VIPs" \
  wait_for_startup_activation 30 "${victim}"
check "unique reachable ownership persists after rejoin" wait_until 60 holds_for 5 safe_rejoin

scenario_end
