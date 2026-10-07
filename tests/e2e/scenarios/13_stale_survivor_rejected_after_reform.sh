#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# An isolated old process must lose authority and release its VIPs before a
# wiped majority recovers. Explicit restart must not resurrect its old boot.
wait_for_single_agreed_leader 20
old_b="$(node_boot_replica node-b)"
old_c="$(node_boot_replica node-c)"
isolate_node_a
wait_until_no_duplicates 20 node_lacks_all_vips node-a || {
  fail 'isolated old member did not withdraw VIPs within the health fence'; exit 1;
}
wait_for_admission_exit node-a "$(admission_exit_budget_seconds node-a 20)"
wait_for_even_without_overlap "$(cleanup_budget_seconds 25)" node-b node-c
assert_node_lacks_all_vips node-a

kill_service node-b KILL
kill_service node-c KILL
wait_for_service_exit node-b 10
wait_for_service_exit node-c 10
start_services_no_deps node-b node-c
wait_for_startup_without_overlap 40 node-b node-c
wait_for_single_agreed_leader 20
[[ "$(node_boot_replica node-b)" != "${old_b}" ]]
[[ "$(node_boot_replica node-c)" != "${old_c}" ]]
service_is_not_running node-a || fail 'expired old boot resumed by itself'

# Docker recreates the stopped namespace without its old blackhole routes.
old_a="$(node_boot_replica node-a)"
majority_leader="$(node_last_leader_replica node-b)"
restart_checkpoint="$(log_checkpoint)"
start_service node-a
startup_budget="$(startup_budget_seconds 45 node-a)"
wait_for_log_after_without_overlap "${restart_checkpoint}" "${startup_budget}" 'committed learner promotion'
wait_for_startup_without_overlap 45 "${NODES[@]}"
wait_for_single_agreed_leader 20
[[ "$(node_boot_replica node-a)" != "${old_a}" ]]
[[ "$(node_last_leader_replica node-a)" == "${majority_leader}" ]] || {
  fail 'restarted old member disrupted the recovered majority leader'
  exit 1
}
assert_unique_holders
log 'expired old member stayed stopped during majority recovery and rejoined with a fresh boot'
