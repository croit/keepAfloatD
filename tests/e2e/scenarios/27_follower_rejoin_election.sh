#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

wait_for_steady_state
leader_id="$(current_leader_id)"
leader="$(service_for_id "${leader_id}")"
follower=""
for node in "${NODES[@]}"; do
  if [[ "${node}" != "${leader}" ]]; then follower="${node}"; break; fi
done
[[ -n "${follower}" ]] || fail 'no follower available for election regression'

majority=()
peer_ips=()
for node in "${NODES[@]}"; do
  [[ "${node}" == "${follower}" ]] && continue
  majority+=("${node}")
  container="$(service_container_id "${node}")"
  peer_ip="$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{"\n"}}{{end}}' "${container}")"
  [[ -n "${peer_ip}" && "${peer_ip}" != *$'\n'* ]] || fail 'expected one disposable peer network'
  peer_ips+=("${peer_ip}")
done

cleanup() {
  local status=$?
  trap - EXIT
  if service_is_running "${follower}"; then
    signal_keepafloatd "${follower}" CONT || status=1
    for peer_ip in "${peer_ips[@]}"; do remove_blackhole_route "${follower}" "${peer_ip}"; done
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

count_service_log() {
  local output
  output="$(compose logs --no-color "${1}")"
  awk -v pattern="${2}" '$0 ~ pattern { count++ } END { print count + 0 }' <<<"${output}"
}

declare -A leader_events
for node in "${majority[@]}"; do
  leader_events["${node}"]="$(count_service_log "${node}" 'raft current leader is now')"
done

for mode in partition pause; do
  old_boot="$(node_boot_replica "${follower}")"
  pause_seconds="$(admission_pause_seconds "${follower}")"
  if [[ "${mode}" == pause ]]; then
    paused_vip=""
    for vip in "${VIPS[@]}"; do
      [[ "$(holder_for_vip "${vip}")" != "${follower}" ]] || paused_vip="${vip}"
    done
    [[ -n "${paused_vip}" ]] || fail 'follower has no VIP before suspension'
    signal_keepafloatd "${follower}" STOP
  fi
  for peer_ip in "${peer_ips[@]}"; do add_blackhole_route "${follower}" "${peer_ip}"; done
  if [[ "${mode}" == pause ]]; then
    # Cross permission expiry, then resume before the successor cleanup fence.
    observe_no_duplicates_for "${pause_seconds}"
    signal_keepafloatd "${follower}" CONT
  fi
  wait_until_no_duplicates 20 node_lacks_all_vips "${follower}" || {
    fail "${mode}: follower did not withdraw VIPs within the health fence"; exit 1;
  }
  wait_for_admission_exit "${follower}" "$(admission_exit_budget_seconds "${follower}" 20)"
  wait_for_even_without_overlap "$(cleanup_budget_seconds 30)" "${majority[@]}"
  restart_checkpoint="$(log_checkpoint)"
  start_service "${follower}"
  startup_budget="$(startup_budget_seconds 30 "${follower}")"
  wait_for_log_after_without_overlap "${restart_checkpoint}" "${startup_budget}" 'committed learner promotion'
  wait_for_startup_without_overlap 30 "${NODES[@]}"
  wait_for_single_agreed_leader 20
  [[ "$(node_boot_replica "${follower}")" != "${old_boot}" ]] || fail 'restart reused the expired boot'
  [[ "$(current_leader_id)" == "${leader_id}" ]] || fail "${mode} rejoin changed the leader"
  for node in "${majority[@]}"; do
    events="$(count_service_log "${node}" 'raft current leader is now')"
    [[ "${events}" == "${leader_events[${node}]}" ]] || fail "${mode} disrupted majority leadership on ${node}"
  done
  assert_unique_holders
  log "${mode}: returning follower preserved majority leadership and unique ownership"
done
