#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Two nodes go down INCLUDING the leader, leaving one survivor that still holds the committed
# cluster state in memory. When the two blank (diskless) nodes return, they must DISCOVER the
# survivor's existing cluster and JOIN it via replication — not form a fresh one. This is the
# opposite of scenario 08 (full outage, no survivor -> cold-form): here a stateful node remains, so
# its session is preserved and the survivor's log wins (only it can be elected against blank peers).
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

# Bring the two blank nodes back. They must join the survivor's cluster, not re-form.
# Snapshot the auto-form count before the restart: the only 'auto-formed' line so far is the
# initial cold-form, so a count increase after the restart means a returning node re-formed. A
# count delta is robust to compose-log reordering; the old line-number checkpoint intermittently
# let that initial line slip past and failed this assertion falsely.
autoform_before="$(cluster_logs_count 'auto-formed Raft cluster')"
restart_checkpoint="$(log_checkpoint)"
start_service "${leader_svc}"
start_service "${victim}"
wait_for_service_running "${leader_svc}" 10
wait_for_service_running "${victim}" 10

# Evidence of rejoin (not re-formation): returning nodes join via replication...
wait_for_log_any_after "${restart_checkpoint}" 40 'joining via replication instead of forming a new one'

# ...the cluster returns to an even 3-node spread with one leader and unique holders...
wait_for_even_over_nodes 45 "${NODES[@]}"
assert_unique_holders
wait_for_single_agreed_leader 15

# ...and crucially NO node re-formed a cluster after the restart (the survivor's session continued):
# no new 'auto-formed' line appears, so the count is unchanged.
autoform_after="$(cluster_logs_count 'auto-formed Raft cluster')"
if (( autoform_after > autoform_before )); then
  dump_cluster_diagnostics
  fail "a returning node re-formed a cluster instead of joining survivor ${survivor} (auto-formed ${autoform_before} -> ${autoform_after})"
  exit 1
fi
