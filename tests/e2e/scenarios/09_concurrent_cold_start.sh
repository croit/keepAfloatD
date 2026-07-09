#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Dead-heat cold start: tear the whole cluster down and bring all three back together (blank,
# diskless). With no special node, all three may call Raft::initialize() concurrently. Identical
# config + Raft election must still converge to exactly ONE cluster: a single agreed leader and
# unique VIP ownership (no split brain).
wait_for_single_agreed_leader 20

kill_service node-a KILL
kill_service node-b KILL
kill_service node-c KILL
wait_for_service_exit node-a 10
wait_for_service_exit node-b 10
wait_for_service_exit node-c 10

# Bring all three back at once — a genuine concurrent cold start.
start_service node-a
start_service node-b
start_service node-c
wait_for_service_running node-a 10
wait_for_service_running node-b 10
wait_for_service_running node-c 10

# Exactly one cluster forms.
wait_for_even_over_nodes 45 "${NODES[@]}"
assert_unique_holders
wait_for_single_agreed_leader 15
