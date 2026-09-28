#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly PRESSURE_STATUS=/shared/connection-pressure.json
readonly PRESSURE_STOP=/shared/connection-pressure.stop
readonly PRESSURE_ARTIFACTS="${ARTIFACT_DIR}/14_connection_pressure"
mkdir -p "${PRESSURE_ARTIFACTS}"
pressure_pid=""

stop_pressure() {
  local status=0
  if [[ -n "${pressure_pid}" ]]; then
    runner_sh "touch '${PRESSURE_STOP}'" || status=1
    if ! wait "${pressure_pid}"; then
      status=1
    fi
    runner_sh "cat '${PRESSURE_STATUS}'" >"${PRESSURE_ARTIFACTS}/pressure.json" || status=1
    pressure_pid=""
  fi
  return "${status}"
}

cleanup() {
  local status=$?
  trap - EXIT
  stop_pressure || status=1
  exit "${status}"
}
trap cleanup EXIT

pressure_started() {
  kill -0 "${pressure_pid}" 2>/dev/null || return 1
  runner_sh "jq -e '.running and all(.endpoints[]; .connected >= 32 and .early_closed > 0)' '${PRESSURE_STATUS}' >/dev/null 2>&1"
}

assert_pressure_bounded() {
  kill -0 "${pressure_pid}" 2>/dev/null || {
    fail "pressure client stopped before failover completed"
    return 1
  }
  local snapshot
  snapshot="$(runner_sh "cat '${PRESSURE_STATUS}'")"
  printf '%s\n' "${snapshot}" >"${PRESSURE_ARTIFACTS}/pressure.json"
  runner_sh "jq -e '.running and all(.endpoints[]; .max_held <= 8 and .early_closed > 0)' '${PRESSURE_STATUS}' >/dev/null" || {
    printf '%s\n' "${snapshot}" >&2
    fail "unauthenticated connections exceeded the eight-connection source quota"
    return 1
  }
}

runner_sh "rm -f '${PRESSURE_STOP}' '${PRESSURE_STATUS}'"
compose exec -T e2e-runner timeout 100 python3 -u - \
  --status "${PRESSURE_STATUS}" --stop "${PRESSURE_STOP}" --duration 90 \
  --endpoint node-a,10.50.0.10,17100,raft \
  --endpoint node-a,10.50.0.10,17101,submit \
  --endpoint node-b,10.50.0.11,17110,raft \
  --endpoint node-b,10.50.0.11,17111,submit \
  --endpoint node-c,10.50.0.12,17120,raft \
  --endpoint node-c,10.50.0.12,17121,submit \
  <"${ROOT_DIR}/tests/e2e/scripts/connection-pressure.py" \
  >"${PRESSURE_ARTIFACTS}/pressure.out" 2>&1 &
pressure_pid=$!

wait_until 10 pressure_started || {
  fail "pressure did not exercise all six listeners"
  exit 1
}
assert_pressure_bounded
log "connection pressure is active on every Raft and submit listener"

set_node_unhealthy node-a
wait_for_even_over_nodes 15 node-b node-c
assert_node_lacks_all_vips node-a
assert_pressure_bounded
log "health-driven VIP handoff passed while connection pressure continued"

set_node_healthy node-a
wait_for_even_over_nodes 15 "${NODES[@]}"
wait_for_single_agreed_leader 10
old_leader_id="$(current_leader_id)"
old_leader_svc="$(service_for_id "${old_leader_id}")"
kill_service "${old_leader_svc}" KILL
wait_for_service_exit "${old_leader_svc}" 10
wait_for_leader_other_than "${old_leader_id}" 20
survivors=()
for node in "${NODES[@]}"; do
  [[ "${node}" == "${old_leader_svc}" ]] || survivors+=("${node}")
done
# #26: after election, stale detection can take 7 probe intervals, followed by
# the 6.5s proof lifetime + 1.75s cleanup allowance and bounded bind work.
# Reserve 20s for that normal fence and a Docker ownership observation.
wait_for_even_over_nodes 20 "${survivors[@]}"
assert_pressure_bounded
log "leader failover and unique reachable VIPs passed under sustained connection pressure"

stop_pressure
start_services_no_deps "${old_leader_svc}"
wait_for_steady_state
log "all three nodes recovered after connection pressure stopped"
