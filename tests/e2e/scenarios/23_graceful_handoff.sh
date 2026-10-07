#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR KEEPAFLOATD_E2E_CONFIG_DIR=configs-stop
# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

role=setup
cleanup() {
  local status=$?
  trap - EXIT
  capture_cluster_artifacts "23_graceful_handoff/${role}" || status=1
  compose down -v --remove-orphans || status=1
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for role in follower leader; do
  reset_cluster
  wait_for_startup_state
  leader="$(service_for_id "$(current_leader_id)")"
  stopping="${leader}"
  if [[ "${role}" == follower ]]; then
    for node in "${NODES[@]}"; do
      if [[ "${node}" != "${leader}" ]]; then
        stopping="${node}"
        break
      fi
    done
  fi
  survivors=()
  for node in "${NODES[@]}"; do
    [[ "${node}" == "${stopping}" ]] || survivors+=("${node}")
  done
  held=()
  for vip in "${VIPS[@]}"; do
    [[ "$(holder_for_vip "${vip}")" != "${stopping}" ]] || held+=("${vip}")
  done
  [[ ${#held[@]} -gt 0 ]] || { fail "${role} held no VIP"; exit 1; }

  log "stopping ${role} ${stopping}, held VIPs: ${held[*]}"
  started=${SECONDS}
  signal_keepafloatd "${stopping}" TERM
  wait_for_service_exit "${stopping}" 10
  assert_service_exit_code "${stopping}" 0
  wait_until 10 all_vips_uniquely_held || {
    dump_cluster_diagnostics
    fail "${role} handoff did not restore unique VIP holders"
    exit 1
  }
  wait_until 5 all_vips_arpable || { fail "${role} handoff VIPs are not ARP-reachable"; exit 1; }
  assert_unique_holders
  # This bound is below both the explicit-failure delay and the stale-holder fence.
  (( SECONDS - started < 30 )) || { fail "planned handoff waited for a failure window"; exit 1; }
  assert_log_contains "${stopping}" 'shutdown unhealthy report committed and applied'
  for vip in "${held[@]}"; do
    assert_log_contains "${stopping}" "unbound ${vip//./\\.}/32 on eth0"
    assert_log_contains "${stopping}" "shutdown VIP release committed.*${vip//./\\.}"
  done
  log "${role} handoff restored unique, ARP-reachable VIPs before the failure windows"
  # A survivor's recovery delay can defer balancing after all VIPs are already reachable.
  wait_for_even_over_nodes 30 "${survivors[@]}"
  capture_cluster_artifacts "23_graceful_handoff/${role}"
  log "${role} survivors reached even VIP distribution after handoff"
done
