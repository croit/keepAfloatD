#!/usr/bin/env bash
# A2: Gratuitous ARP. After a VIP moves to a new holder, the mgmt node's ARP table must
# resolve the VIP to the NEW holder's MAC (keepafloatd arping on bind updates neighbors).
SCENARIO_NAME=A2_arp
source "$(dirname "$0")/../scenario.sh"
scenario_start "gratuitous ARP updates neighbor MAC after failover"

vip="${VIPS[1]}"
snapshot_vips
old="$(holder_for_vip "${vip}")"
old_valid=0
for ip in "${NODE_IPS[@]}"; do [[ "${old}" == "${ip}" ]] && old_valid=1; done
check "target VIP starts with exactly one cluster-node holder" test "${old_valid}" -eq 1
[[ "${old_valid}" -eq 1 ]] || scenario_end
old_mac="$(node_sh "${old}" "cat /sys/class/net/${IFACE}/address")"
mgmt_mac="$(vip_mac "${vip}")"
evid "VIP ${vip} on ${old} (mac ${old_mac}); mgmt sees mac ${mgmt_mac}"
check_eq "ARP resolves VIP to current holder MAC" "${mgmt_mac}" "${old_mac}"

# Move the VIP by stopping the current holder.
kafd_stop "${old}"
check "victim released VIPs" wait_until 30 node_lacks_all_vips "${old}"
# Only inspect kernel ownership here. Reachability probes would repair a stale ARP
# entry and mask missing gratuitous announcements.
passive_unique_survivors() {
  node_lacks_all_vips "${old}" && all_vips_uniquely_held
}
check "VIPs settle uniquely on survivors without an active probe" \
  wait_until 60 holds_for 5 passive_unique_survivors
snapshot_vips
new="$(holder_for_vip "${vip}")"
new_valid=0
for ip in "${NODE_IPS[@]}"; do [[ "${new}" == "${ip}" ]] && new_valid=1; done
check "target VIP settles on exactly one surviving cluster node" test "${new_valid}" -eq 1
[[ "${new_valid}" -eq 1 ]] || scenario_end
new_mac="$(node_sh "${new}" "cat /sys/class/net/${IFACE}/address")"
check "VIP moved to a different interface MAC" test "${new_mac}" != "${old_mac}"

# Do not ping or flush after the move: either would solicit ordinary ARP and let a broken
# gratuitous-ARP implementation pass. The already-primed neighbor entry must change unsolicited.
neighbor_matches_new_holder() { [[ "$(neighbor_mac "${vip}")" == "${new_mac}" ]]; }
check "unsolicited ARP updates the primed neighbor entry" \
  wait_until 15 neighbor_matches_new_holder
new_seen="$(neighbor_mac "${vip}")"
evid "VIP ${vip} now on ${new} (mac ${new_mac}); mgmt now sees ${new_seen}"
check_eq "ARP now resolves VIP to NEW holder MAC" "${new_seen}" "${new_mac}"

kafd_start "${old}"
scenario_end
