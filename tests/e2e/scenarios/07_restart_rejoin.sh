#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# A node that goes down and comes back blank (diskless) must REJOIN the live cluster via
# replication, not form a second one. node-a is the lowest id, i.e. the node most likely to try to
# re-initialize, so it is the strongest case to exercise.
wait_for_single_agreed_leader 20

kill_service node-a KILL
wait_for_service_exit node-a 10

# Survivors keep serving and collapse VIPs evenly onto the remaining pair (leader id not asserted).
wait_for_even_over_nodes 30 node-b node-c

# Bring node-a back blank; it must discover the existing cluster and join, not re-form.
restart_checkpoint="$(log_checkpoint)"
start_service node-a
wait_for_service_running node-a 10
wait_for_log_any_after "${restart_checkpoint}" 30 'joining via replication instead of forming a new one'

# Cluster stays consistent (no split brain): unique holders, back to an even 3-node spread.
wait_for_even_over_nodes 45 "${NODES[@]}"
assert_unique_holders
