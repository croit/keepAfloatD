#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Same property as scenario 10 (two nodes down incl. leader -> survivor keeps the cluster ->
# returning nodes join, not re-form), but exercised against a DIFFERENT leader placement: we first
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
wait_for_even_over_nodes 45 "${NODES[@]}"
wait_for_single_agreed_leader 15

l1_id="$(current_leader_id)"
l1_svc="$(service_for_id "${l1_id}")"
[[ "${l1_id}" != "${l0_id}" ]] || {
  dump_cluster_diagnostics
  fail "leadership did not move off ${l0_svc}"
  exit 1
}
log "leadership moved to ${l1_svc} (id ${l1_id})"

# Phase 2: survivor-rejoin against the NEW leader L1 — kill L1 plus one follower, keep one survivor.
others=()
for svc in "${NODES[@]}"; do
  [[ "${svc}" == "${l1_svc}" ]] || others+=("${svc}")
done
victim="${others[0]}"
survivor="${others[1]}"
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

# Returning blank nodes must join the survivor's cluster, not re-form one.
# Snapshot the auto-form count before the restart (see scenario 10): a count increase afterwards
# means a returning node re-formed. Robust to compose-log reordering, unlike a line-number
# checkpoint.
autoform_before="$(cluster_logs_count 'auto-formed Raft cluster')"
restart_checkpoint="$(log_checkpoint)"
start_service "${l1_svc}"
start_service "${victim}"
wait_for_service_running "${l1_svc}" 10
wait_for_service_running "${victim}" 10

wait_for_log_any_after "${restart_checkpoint}" 40 'joining via replication instead of forming a new one'
wait_for_even_over_nodes 45 "${NODES[@]}"
assert_unique_holders
wait_for_single_agreed_leader 15

autoform_after="$(cluster_logs_count 'auto-formed Raft cluster')"
if (( autoform_after > autoform_before )); then
  dump_cluster_diagnostics
  fail "a returning node re-formed a cluster instead of joining survivor ${survivor} (auto-formed ${autoform_before} -> ${autoform_after})"
  exit 1
fi
