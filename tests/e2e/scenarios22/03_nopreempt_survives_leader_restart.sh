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

restart_observation_now() {
  printf '%s\n' "${SECONDS}"
}

restarted_effects_ready() {
  local service="${1:?service required}" previous_boot="${2:?previous boot required}"
  local logs replica activation_ms now
  logs="$(compose logs --no-color "${service}")" || return 1
  logs="$(printf '%s\n' "${logs}" | current_boot_log | sed -E $'s/\033\\[[0-9;]*[mK]//g')" || return 1
  replica="$(grep 'runtime admission acquired' <<<"${logs}" |
    grep -oE 'replica=[0-9a-f]{16}:[0-9a-f]{64}' | tail -n 1)" || return 1
  replica="${replica#replica=}"
  [[ -n "${replica}" && "${replica}" != "${previous_boot}" ]] || return 1
  now="$(restart_observation_now)"
  if [[ -z "${restart_seen_boot}" ]]; then
    restart_seen_boot="${replica}"
    # SECONDS truncates; round the observed admission upward, never start early.
    restart_admitted_at=$((now + 1))
  fi
  [[ "${replica}" == "${restart_seen_boot}" ]] || return 1
  activation_ms="$(awk '/runtime admission timing/ {
    for (i = 1; i <= NF; i++) if ($i ~ /^vip_activation_ms=/)
      print substr($i, length("vip_activation_ms=") + 1)
  }' <<<"${logs}")"
  [[ "${activation_ms}" =~ ^(0|[1-9][0-9]{0,11})$ ]] || return 1
  grep -q 'VIP effects armed after diskless replay reached the committed frontier' <<<"${logs}" || return 1
  # Replay arming can precede activation; observed admission is a conservative local origin.
  ((now - restart_admitted_at >= (activation_ms + 999) / 1000)) || return 1
  single_agreed_leader
}

assert_nopreempt_after_restart() {
  local service="${1:?service required}" previous_boot="${2:?previous boot required}"
  local incumbent="${3:?incumbent required}" budget deadline observation_deadline now
  local restart_seen_boot='' restart_admitted_at=''
  budget="$(startup_budget_seconds 30 "${service}")" || return 1
  deadline=$(($(restart_observation_now) + budget))
  while true; do
    assert_no_duplicate_holders && assert_unique_holders || return 1
    service_is_running "${service}" || { fail "${service} stopped during restart readiness"; return 1; }
    vip_held_by "${VIP}" "${incumbent}" || { fail "${VIP} left ${incumbent} during restart readiness"; return 1; }
    restarted_effects_ready "${service}" "${previous_boot}" && break
    (($(restart_observation_now) < deadline)) || {
      fail "${service} did not reach current-boot VIP activation readiness"; return 1;
    }
    sleep 0.2
  done
  # Include the partial clock second so the observation lasts at least six seconds.
  observation_deadline=$(($(restart_observation_now) + 7))
  while true; do
    assert_no_duplicate_holders && assert_unique_holders || return 1
    service_is_running "${service}" && single_agreed_leader || return 1
    [[ "$(node_boot_replica "${service}")" == "${restart_seen_boot}" ]] || return 1
    vip_held_by "${VIP}" "${incumbent}" || {
      fail "restarted ${service} preempted ${VIP} from ${incumbent}"; return 1;
    }
    now="$(restart_observation_now)"
    ((now < observation_deadline)) || break
    sleep 0.2
  done
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
original_boot="$(node_boot_replica "${original}")"
start_service "${original}"
wait_for_service_running "${original}" 10
assert_nopreempt_after_restart "${original}" "${original_boot}" "${replacement}" || {
  dump_cluster_diagnostics
  exit 1
}

# Make the leader own the VIP so election randomness cannot skip orphan takeover.
# Restarting that leader must not preempt the survivor after the new election.
wait_for_single_agreed_leader 20
old_leader_id="$(current_leader_id)"
old_leader_svc="$(service_for_id "${old_leader_id}")"
if ! vip_held_by "${VIP}" "${old_leader_svc}"; then
  for node in "${NODES[@]}"; do
    [[ "${node}" == "${old_leader_svc}" ]] || set_node_unhealthy "${node}"
  done
  wait_until 15 vip_held_by "${VIP}" "${old_leader_svc}" || {
    dump_cluster_diagnostics
    fail "leader ${old_leader_svc} did not acquire ${VIP} during setup"
    exit 1
  }
  for node in "${NODES[@]}"; do
    set_node_healthy "${node}"
  done
fi
incumbent="$(wait_for_stable_vip_holder "${VIP}" 15 3)" || {
  dump_cluster_diagnostics
  fail "${VIP} did not settle on the leader before its crash"
  exit 1
}
wait_for_single_agreed_leader 20
if [[ "${incumbent}" != "${old_leader_svc}" || "$(current_leader_id)" != "${old_leader_id}" ]]; then
  dump_cluster_diagnostics
  fail "leader ownership changed before the crash; orphan takeover would not be tested"
  exit 1
fi
handoff_started=$SECONDS
log "stopping leader ${old_leader_svc}; VIP incumbent is ${incumbent}"
kill_service "${old_leader_svc}" KILL
wait_for_service_exit "${old_leader_svc}" 10
wait_for_leader_other_than "${old_leader_id}" 30
log "new leader agreed after $((SECONDS - handoff_started))s"
wait_until "$(cleanup_budget_seconds 20)" vip_moved_from "${VIP}" "${incumbent}" || {
  dump_cluster_diagnostics
  fail "orphaned ${VIP} did not move off killed leader ${incumbent}"
  exit 1
}
survivor="$(holder_for_vip "${VIP}")"
log "VIP survivor is ${survivor} after $((SECONDS - handoff_started))s"

leader_boot="$(node_boot_replica "${old_leader_svc}")"
start_service "${old_leader_svc}"
wait_for_service_running "${old_leader_svc}" 10
assert_nopreempt_after_restart "${old_leader_svc}" "${leader_boot}" "${survivor}" || {
  dump_cluster_diagnostics
  exit 1
}

assert_unique_holders
log "${VIP} preserved nopreempt ownership across node restart, leader failover, and leader restart"
