#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly VIP="${VIPS[0]}"
original="$(wait_for_stable_vip_holder "${VIP}" 15 3)" || {
  dump_cluster_diagnostics
  fail "${VIP} did not settle before the leader-restart check"
  exit 1
}

vip_moved_from() {
  local current
  current="$(holder_for_vip "$1")"
  [[ "${current}" != "$2" && "${current}" != "none" && "${current}" != duplicate:* ]]
}

vip_held_by() {
  [[ "$(holder_for_vip "$1")" == "$2" ]]
}

# Make the first holder nopreempt. Restarting that diskless node must restore the replicated policy
# before it can make a bind decision, so it must not take the VIP back from the healthy replacement.
set_node_unhealthy "${original}"
wait_until 15 vip_moved_from "${VIP}" "${original}" || {
  dump_cluster_diagnostics
  fail "${VIP} did not leave unhealthy original holder ${original}"
  exit 1
}
set_node_healthy "${original}"
replacement="$(holder_for_vip "${VIP}")"

kill_service "${original}" KILL
wait_for_service_exit "${original}" 10
wait_until 20 vip_held_by "${VIP}" "${replacement}" || {
  dump_cluster_diagnostics
  fail "${VIP} moved off healthy replacement ${replacement} when ${original} stopped"
  exit 1
}
start_service "${original}"
wait_for_service_running "${original}" 10
wait_for_single_agreed_leader 20
wait_until 20 vip_held_by "${VIP}" "${replacement}" || {
  dump_cluster_diagnostics
  fail "${VIP} did not reconverge on ${replacement} after ${original} restarted"
  exit 1
}
for _ in {1..30}; do
  if node_has_vip_bound "${original}" "${VIP}"; then
    dump_cluster_diagnostics
    fail "restarted nopreempt ${original} took ${VIP} from ${replacement}"
    exit 1
  fi
  sleep 0.2
done
wait_until 20 vip_held_by "${VIP}" "${replacement}" || {
  dump_cluster_diagnostics
  fail "${VIP} did not settle back on incumbent ${replacement} after ${original} restarted"
  exit 1
}

# Force a leader change separately. If the leader owns the VIP, taking the resulting orphan is
# legitimate nopreempt fallback; otherwise the incumbent must not move. In both cases, restarting
# the old leader must not proactively preempt the survivor that owns the VIP after election.
wait_for_single_agreed_leader 20
old_leader_id="$(current_leader_id)"
old_leader_svc="$(service_for_id "${old_leader_id}")"
incumbent="$(holder_for_vip "${VIP}")"
kill_service "${old_leader_svc}" KILL
wait_for_service_exit "${old_leader_svc}" 10
wait_for_leader_other_than "${old_leader_id}" 30
if [[ "${old_leader_svc}" == "${incumbent}" ]]; then
  wait_until 20 vip_moved_from "${VIP}" "${incumbent}" || {
    dump_cluster_diagnostics
    fail "orphaned ${VIP} did not move off killed leader ${incumbent}"
    exit 1
  }
else
  wait_until 20 vip_held_by "${VIP}" "${incumbent}" || {
    dump_cluster_diagnostics
    fail "${VIP} moved off live incumbent ${incumbent} during leader failover"
    exit 1
  }
fi
survivor="$(holder_for_vip "${VIP}")"

start_service "${old_leader_svc}"
wait_for_service_running "${old_leader_svc}" 10
wait_for_single_agreed_leader 20
wait_until 20 vip_held_by "${VIP}" "${survivor}" || {
  dump_cluster_diagnostics
  fail "${VIP} did not reconverge on ${survivor} after ${old_leader_svc} restarted"
  exit 1
}
for _ in {1..30}; do
  if node_has_vip_bound "${old_leader_svc}" "${VIP}"; then
    dump_cluster_diagnostics
    fail "restarted leader ${old_leader_svc} preempted ${VIP} from ${survivor}"
    exit 1
  fi
  sleep 0.2
done
wait_until 20 vip_held_by "${VIP}" "${survivor}" || {
  dump_cluster_diagnostics
  fail "${VIP} did not settle back on survivor ${survivor} after leader restart"
  exit 1
}

assert_unique_holders
log "${VIP} preserved nopreempt ownership across node restart, leader failover, and leader restart"
