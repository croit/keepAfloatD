#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Same safe expiry and restart property as scenario 10, against a DIFFERENT leader placement: we first
# force a leadership change so the leader under test is not whichever node happens to win the
# cold-start election. This guards against any hidden dependence on a particular node id.

# Phase 0: steady cluster with some initial leader L0.
wait_for_single_agreed_leader 20
l0_id="$(current_leader_id)"
l0_svc="$(service_for_id "${l0_id}")"
log "initial leader ${l0_svc} (id ${l0_id})"

# Phase 1: force the leader to move by killing L0, then restart L0 so we have a full 3-node cluster
# again, now led by a different node (L1 != L0).
kill_service "${l0_svc}" KILL
wait_for_service_exit "${l0_svc}" 10
wait_for_leader_other_than "${l0_id}" 30
start_service "${l0_svc}"
wait_for_service_running "${l0_svc}" 10
wait_for_startup_without_overlap 45 "${NODES[@]}"
wait_for_single_agreed_leader 15

l1_id="$(current_leader_id)"
l1_svc="$(service_for_id "${l1_id}")"
[[ "${l1_id}" != "${l0_id}" ]] || {
  dump_cluster_diagnostics
  fail "leadership did not move off ${l0_svc}"
  exit 1
}
log "leadership moved to ${l1_svc} (id ${l1_id})"

# Phase 2: survivor-rejoin against the NEW leader L1 - kill L1 plus one follower, keep one survivor.
others=()
for svc in "${NODES[@]}"; do
  [[ "${svc}" == "${l1_svc}" ]] || others+=("${svc}")
done
victim="${others[0]}"
survivor="${others[1]}"
survivor_boot="$(node_boot_replica "${survivor}")"
log "killing leader ${l1_svc} + ${victim}; survivor=${survivor}"

kill_service "${l1_svc}" KILL
kill_service "${victim}" KILL
wait_for_service_exit "${l1_svc}" 10
wait_for_service_exit "${victim}" 10

# No quorum on the lone survivor -> it unbinds its VIP(s).
wait_until 20 node_lacks_all_vips "${survivor}" || {
  dump_cluster_diagnostics
  fail "survivor ${survivor} kept VIPs without quorum"
  exit 1
}

wait_for_admission_exit "${survivor}" "$(admission_exit_budget_seconds "${survivor}" 20)"
start_services_no_deps "${l1_svc}" "${victim}"
wait_for_service_running "${l1_svc}" 10
wait_for_service_running "${victim}" 10
service_is_not_running "${survivor}" || fail 'majority startup restarted the expired survivor'

wait_for_startup_without_overlap 45 "${l1_svc}" "${victim}"
wait_for_single_agreed_leader 15
service_is_not_running "${survivor}" || fail 'expired survivor resumed without a restart'
majority_leader="$(node_last_leader_replica "${l1_svc}")"
restart_checkpoint="$(log_checkpoint)"
start_service "${survivor}"
startup_budget="$(startup_budget_seconds 40 "${survivor}")"
wait_for_log_after_without_overlap "${restart_checkpoint}" "${startup_budget}" 'committed learner promotion'
wait_for_startup_without_overlap 45 "${NODES[@]}"
assert_unique_holders
wait_for_single_agreed_leader 15

[[ "$(node_boot_replica "${survivor}")" != "${survivor_boot}" ]]
[[ "$(node_last_leader_replica "${survivor}")" == "${majority_leader}" ]]
log 'changed-leader outage recovered only after the expired survivor stopped'
