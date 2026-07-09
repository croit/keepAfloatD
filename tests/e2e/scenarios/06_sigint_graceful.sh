#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Capture the VIP(s) node-a currently holds before shutdown — placement is non-deterministic under
# minimal-movement, so we assert it unbinds whatever it actually held, not a fixed address.
held_by_a=()
for vip in "${VIPS[@]}"; do
  [[ "$(holder_for_vip "${vip}")" == node-a ]] && held_by_a+=("${vip}")
done
[[ "${#held_by_a[@]}" -ge 1 ]] || {
  dump_cluster_diagnostics
  fail "node-a held no VIP at steady state; nothing to exercise"
  exit 1
}

signal_keepafloatd node-a INT
# Graceful shutdown unbinds every VIP and tears down Raft before exiting; under a
# loaded CI runner this can take noticeably longer than a local run, so keep the
# exit budget well clear of the worst case rather than tight to the happy path.
wait_for_service_exit node-a 30
assert_service_exit_code node-a 0
# Graceful shutdown leaves the departed node's last health update fresh for the
# stale_secs window (6s in the e2e configs), so the surviving pair only rebalances
# after it ages out. Allow generous slack on top for reconcile + Raft commit on a
# contended runner; wait_until returns as soon as the assignments settle.
wait_for_even_over_nodes 45 node-b node-c
assert_log_contains node-a 'shutting down on SIGINT'
for vip in "${held_by_a[@]}"; do
  assert_log_contains node-a "unbound ${vip//./\\.}/32 on eth0"
done
