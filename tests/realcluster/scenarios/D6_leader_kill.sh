#!/usr/bin/env bash
# D6: Kill the Raft leader. A new leader must be elected and VIPs stay correctly assigned
# (the cluster must not lose its VIP placement just because leadership moved).
SCENARIO_NAME=D6_leader_kill
source "$(dirname "$0")/../scenario.sh"
scenario_start "leader kill triggers re-election; VIPs remain correctly held"

old_leader_id="$(leader_seen_by "${NODE_IPS[0]}")"
evid "current leader raft id: ${old_leader_id}"
# Map raft id -> node ip.
leader_ip=""
for i in "${!NODE_RAFT_IDS[@]}"; do
  [[ "${NODE_RAFT_IDS[$i]}" == "${old_leader_id}" ]] && leader_ip="${NODE_IPS[$i]}"
done
evid "leader node: ${leader_ip}"
[[ -n "${leader_ip}" ]] || { evid "could not resolve leader ip"; _PASS=0; scenario_end; }

snapshot_vips
leader_vip=""
for vip in "${VIPS[@]}"; do
  node_has_vip_bound "${leader_ip}" "${vip}" && leader_vip="${vip}"
done
check "leader starts with one VIP to track across recovery" test -n "${leader_vip}"
if [[ -z "${leader_vip}" ]]; then
  scenario_end
fi

fault_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
kafd_stop "${leader_ip}"

# After stopping the leader, the surviving majority must elect a leader and keep operating. We assert
# the operational properties (a leader exists, VIPs served by live nodes, cluster recovers) rather
# than reading a specific raft id from journals - that grep is racy and Raft may legitimately
# re-elect a fast-rejoining node, so the id itself is not a meaningful invariant.
survivors_agree_on_new_leader() {
  local s lid agreed=""
  for s in $(nodes_except "${leader_ip}"); do
    lid="$(leader_seen_by_since "${s}" "${fault_since}")"
    [[ -n "${lid}" && "${lid}" != "${old_leader_id}" ]] || return 1
    [[ -z "${agreed}" || "${agreed}" == "${lid}" ]] || return 1
    agreed="${lid}"
  done
  [[ -n "${agreed}" ]]
}
check "survivors elect and agree on a leader different from ${old_leader_id}" \
  wait_until 60 survivors_agree_on_new_leader
new_leader_id="$(leader_seen_by_since "$(nodes_except "${leader_ip}" | head -1)" "${fault_since}")"
evid "leader after stopping ${leader_ip}: raft id ${new_leader_id:-unknown} (old ${old_leader_id})"

# Graceful stop must remove the leader's addresses; nopreempt permits any unique survivor spread.
check "stopped leader gracefully releases every VIP" wait_until 30 node_lacks_all_vips "${leader_ip}"
check "survivors hold every VIP uniquely and reachably" \
  wait_for_live_service_without 60 "${leader_ip}"
snapshot_vips
leader_vip_replacement="$(holder_for_vip "${leader_vip}")"
check "failed leader's VIP has one survivor replacement" \
  instance_for_ip "${leader_vip_replacement}"
if ! instance_for_ip "${leader_vip_replacement}" >/dev/null; then
  kafd_start "${leader_ip}"
  scenario_end
fi

# Restart the old leader. The release-ack fence permits a short ownerless gap, but it must never
# permit two kernel holders while the diskless process catches up. Under the nopreempt baseline,
# the recovered leader is not required to take a VIP back from a healthy survivor.
kafd_start "${leader_ip}"
check "old leader remains active after restart" \
  wait_until 30 holds_for 5 node_active "${leader_ip}"
check "leader restart never double-binds a VIP" \
  holds_for 20 no_vip_is_duplicate
check "rejoined cluster sustains daemon, leader, ownership, and reachability invariants" \
  wait_for_available_cluster

scenario_end
