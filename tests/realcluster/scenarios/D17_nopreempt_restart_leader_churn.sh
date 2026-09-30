#!/usr/bin/env bash
# D17: A recovered failback:false node must retain its nopreempt history after its diskless daemon
# restarts. A later leader kill may reassign only orphaned VIPs; live incumbents remain stable, and
# restarting the old leader must not trigger proactive failback.
SCENARIO_NAME=D17_nopreempt_restart_leader_churn
source "$(dirname "$0")/../scenario.sh"
scenario_start "nopreempt history survives node restart and independent leader churn"

tag=d17
backup_cluster_configs "${tag}"
clear_health_sentinels
configure_sentinel_health 500 100 3
set_cluster_scalar failover_delay_secs 0
set_cluster_scalar failback false
set_cluster_scalar failback_delay_secs 0
clean_reform
check "cluster steady before restart coverage" wait_for_steady_state

declare -A before_restart=() before_leader=() after_failover=()

capture_assignments() {
  local -n output="${1:?map name required}"
  local vip holder
  output=()
  snapshot_vips
  for vip in "${VIPS[@]}"; do
    holder="$(holder_for_vip "${vip}")"
    case "${holder}" in none|duplicate:*) return 1 ;; esac
    output["${vip}"]="${holder}"
  done
}

assignments_match() {
  local -n expected="${1:?map name required}"
  local vip
  snapshot_vips
  for vip in "${VIPS[@]}"; do
    [[ "$(holder_for_vip "${vip}")" == "${expected[${vip}]-}" ]] || return 1
  done
}

capture_live_assignments() {
  local dead="${1:?dead node required}"
  local -n output="${2:?map name required}"
  local vip ip holder
  output=()
  snapshot_vips
  for vip in "${VIPS[@]}"; do
    holder=""
    for ip in "${NODE_IPS[@]}"; do
      [[ "${ip}" == "${dead}" ]] && continue
      if node_has_vip_bound "${ip}" "${vip}"; then
        [[ -z "${holder}" ]] || return 1
        holder="${ip}"
      fi
    done
    [[ -n "${holder}" ]] || return 1
    output["${vip}"]="${holder}"
  done
}

live_incumbents_unchanged() {
  local dead="${1:?dead node required}"
  local vip
  snapshot_vips
  for vip in "${VIPS[@]}"; do
    [[ "${before_leader[${vip}]-}" == "${dead}" ]] && continue
    [[ "$(holder_for_vip "${vip}")" == "${before_leader[${vip}]-}" ]] || return 1
  done
}

recovered_restart_stable() {
  all_daemons_active &&
    all_nodes_agree_on_leader &&
    assignments_match before_restart &&
    node_lacks_all_vips "${victim}"
}

leader_restart_stable() {
  all_daemons_active && all_nodes_agree_on_leader && assignments_match after_failover
}

victim="${NODE_IPS[0]}"
sentinel_fail "${victim}"
check "first holder loses every VIP" wait_until 30 node_lacks_all_vips "${victim}"
sentinel_recover "${victim}"
check "recovered node is nopreempt while incumbents are healthy" holds_for 6 \
  node_lacks_all_vips "${victim}"
check "pre-restart ownership is unique" capture_assignments before_restart

evid "restarting recovered nopreempt node ${victim}"
kafd_kill "${victim}"
kafd_start "${victim}"
check "restarted recovered node stays live, unique, and nopreempt for eight seconds" \
  wait_until 30 holds_for 8 recovered_restart_stable

check "capture ownership before leader fault" capture_assignments before_leader
old_leader_id="$(cluster_leader_id)"
old_leader_ip=""
for i in "${!NODE_RAFT_IDS[@]}"; do
  [[ "${NODE_RAFT_IDS[$i]}" == "${old_leader_id}" ]] && old_leader_ip="${NODE_IPS[$i]}"
done
check "current leader maps to a cluster node" test -n "${old_leader_ip}"

fault_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
evid "killing leader ${old_leader_ip} (raft id ${old_leader_id})"
kafd_kill "${old_leader_ip}"

survivors_agree_on_new_leader() {
  local ip leader agreed=""
  for ip in $(nodes_except "${old_leader_ip}"); do
    leader="$(leader_seen_by_since "${ip}" "${fault_since}")"
    [[ -n "${leader}" && "${leader}" != "${old_leader_id}" ]] || return 1
    [[ -z "${agreed}" || "${agreed}" == "${leader}" ]] || return 1
    agreed="${leader}"
  done
}

check "survivors elect a different leader" wait_until 60 survivors_agree_on_new_leader
check "every VIP gains exactly one live survivor" wait_until 30 \
  capture_live_assignments "${old_leader_ip}" after_failover
check "VIPs on live incumbents do not move during leader failover" \
  live_incumbents_unchanged "${old_leader_ip}"

kafd_start "${old_leader_ip}"
check "old leader stays live without preempting survivor ownership for eight seconds" \
  wait_until 30 holds_for 8 leader_restart_stable
check "final ownership has no duplicates" assert_unique_holders
check "all VIPs remain pingable" all_vips_pingable

clear_health_sentinels
restore_cluster_configs "${tag}"
clean_reform
scenario_end
