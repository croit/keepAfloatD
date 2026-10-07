#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Leadership is non-deterministic, so discover the current leader and kill *that* node, then assert
# a new leader is elected among the survivors and VIPs re-collapse onto them.
wait_for_single_agreed_leader 20
old_leader_id="$(current_leader_id)"
old_leader_svc="$(service_for_id "${old_leader_id}")"
declare -A original_holders
for vip in "${VIPS[@]}"; do
  original_holders["${vip}"]="$(holder_for_vip "${vip}")"
done
assert_unique_holders
log "current leader is ${old_leader_svc} (id ${old_leader_id}); killing it"

kill_service "${old_leader_svc}" KILL
wait_for_service_exit "${old_leader_svc}" 10

# A new leader must emerge among the two survivors (necessarily a different id). node_last_leader
# reports the stale leader until the new one is logged, so wait for the id to actually change.
wait_for_leader_other_than "${old_leader_id}" 30

# VIPs re-collapse onto the two survivors: every VIP held exactly once, none dropped or duplicated.
wait_until "$(cleanup_budget_seconds 30)" all_vips_uniquely_held || {
  dump_cluster_diagnostics
  fail "VIPs not uniquely held after leader kill (current: $(current_assignments_summary))"
  exit 1
}
assert_unique_holders

for vip in "${VIPS[@]}"; do
  before="${original_holders[${vip}]}"
  [[ "${before}" == "${old_leader_svc}" ]] && continue
  after="$(holder_for_vip "${vip}")"
  [[ "${after}" == "${before}" ]] || {
    dump_cluster_diagnostics
    fail "leader loss moved healthy survivor VIP ${vip} from ${before} to ${after}"
    exit 1
  }
done
log "healthy survivors retained their VIP assignments after leader failover"
