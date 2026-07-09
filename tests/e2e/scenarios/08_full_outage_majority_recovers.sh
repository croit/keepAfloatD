#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Diskless full-outage recovery: every node goes down, and only a majority (node-b + node-c) comes
# back. The survivors must reform the cluster on their own, elect a leader, and bind VIPs — no node
# is special and there is no saved state.
#
# The node held down is node-a, the LOWEST id, on purpose: it is the strongest check that no node
# has a privileged role in formation (a regression guard against any lowest-id-special behavior).
wait_for_single_agreed_leader 20

checkpoint="$(log_checkpoint)"

# Whole-cluster outage.
kill_service node-a KILL
kill_service node-b KILL
kill_service node-c KILL
wait_for_service_exit node-a 10
wait_for_service_exit node-b 10
wait_for_service_exit node-c 10

# Only the two higher-id nodes return (blank). node-a stays down — use --no-deps so compose does
# not pull node-a back up via node-b's depends_on.
start_services_no_deps node-b node-c
wait_for_service_running node-b 10
wait_for_service_running node-c 10

# A majority (2 of 3) is enough: the pair forms a cluster and elects a leader among {2,3}.
wait_for_log_any_after "${checkpoint}" 40 'raft current leader is now Some\((2|3)\)'

# VIPs come up on the surviving pair, uniquely and evenly; node-a is still down and holds nothing.
wait_for_even_over_nodes 40 node-b node-c
assert_unique_holders
service_is_not_running node-a || fail "node-a was expected to remain down"
