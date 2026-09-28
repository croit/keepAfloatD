#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

held=()
for vip in "${VIPS[@]}"; do
  [[ "$(holder_for_vip "${vip}")" == node-a ]] && held+=("${vip}")
done
[[ "${#held[@]}" -ge 1 ]] || fail "node-a has no VIP to release"

signal_keepafloatd node-a HUP
wait_for_service_exit node-a 10
assert_service_exit_code node-a 1
assert_log_contains node-a 'shutting down on SIGHUP'
for vip in "${held[@]}"; do
  assert_log_contains node-a "unbound ${vip//./\\.}/32 on eth0"
done
wait_for_even_over_nodes 30 node-b node-c

start_service node-a
wait_for_steady_state
