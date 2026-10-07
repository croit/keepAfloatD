#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Loss of the majority expires the remaining process's admission. It must
# release its VIPs and stop before blank replacements recover service.
wait_for_single_agreed_leader 20

leader_id="$(current_leader_id)"
leader_svc="$(service_for_id "${leader_id}")"

# The two non-leader services: kill one of them alongside the leader; the other is the survivor.
others=()
for svc in "${NODES[@]}"; do
  [[ "${svc}" == "${leader_svc}" ]] || others+=("${svc}")
done
victim="${others[0]}"
survivor="${others[1]}"
survivor_boot="$(node_boot_replica "${survivor}")"
log "leader=${leader_svc} (id ${leader_id}); killing leader + ${victim}; survivor=${survivor}"

# Kill the leader and one follower: only ${survivor} remains (1 of 3 -> no quorum).
kill_service "${leader_svc}" KILL
kill_service "${victim}" KILL
wait_for_service_exit "${leader_svc}" 10
wait_for_service_exit "${victim}" 10

# Without quorum the survivor cannot lead, so it unbinds its VIP(s): a real outage on one node.
wait_until 20 node_lacks_all_vips "${survivor}" || {
  dump_cluster_diagnostics
  fail "survivor ${survivor} kept VIPs without quorum"
  exit 1
}

wait_for_admission_exit "${survivor}" "$(admission_exit_budget_seconds "${survivor}" 20)"

# Recover a fresh physical majority while the expired process remains stopped.
start_services_no_deps "${leader_svc}" "${victim}"
wait_for_service_running "${leader_svc}" 10
wait_for_service_running "${victim}" 10
service_is_not_running "${survivor}" || fail 'majority startup restarted the expired survivor'

wait_for_startup_without_overlap 45 "${leader_svc}" "${victim}"
wait_for_single_agreed_leader 15
service_is_not_running "${survivor}" || fail 'expired process resumed without a restart'
majority_leader="$(node_last_leader_replica "${leader_svc}")"

# The explicitly restarted survivor must join the recovered majority as a learner.
restart_checkpoint="$(log_checkpoint)"
start_service "${survivor}"
startup_budget="$(startup_budget_seconds 40 "${survivor}")"
wait_for_log_after_without_overlap "${restart_checkpoint}" "${startup_budget}" 'committed learner promotion'
wait_for_startup_without_overlap 45 "${NODES[@]}"
assert_unique_holders
wait_for_single_agreed_leader 15

[[ "$(node_boot_replica "${survivor}")" != "${survivor_boot}" ]]
[[ "$(node_last_leader_replica "${survivor}")" == "${majority_leader}" ]]

log 'expired survivor stopped safely and rejoined the recovered majority'
