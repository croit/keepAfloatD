#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

for signal in HUP QUIT; do
  held=()
  for vip in "${VIPS[@]}"; do
    [[ "$(holder_for_vip "${vip}")" == node-a ]] && held+=("${vip}")
  done
  [[ "${#held[@]}" -ge 1 ]] || fail "node-a has no VIP to release"

  started="$(docker inspect -f '{{.State.StartedAt}}' "$(service_container_id node-a)")"
  signal_keepafloatd node-a "${signal}"
  wait_for_service_exit node-a 10
  assert_service_exit_code node-a 1
  logs="$(compose logs --no-color --since "${started}" node-a)"
  [[ "${logs}" == *"shutting down on SIG${signal}"* ]] || fail "missing SIG${signal} shutdown"
  for vip in "${held[@]}"; do
    [[ "${logs}" == *"unbound ${vip}/32 on eth0"* ]] || fail "SIG${signal} did not unbind ${vip}"
  done
  wait_for_even_over_nodes 30 node-b node-c

  start_service node-a
  wait_for_startup_state
done
