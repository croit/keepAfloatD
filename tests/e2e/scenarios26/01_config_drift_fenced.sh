#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Nodes a/b share the canonical config. Node c differs only in health.stale_secs. The matching
# majority must form and serve every VIP; c must exit with the dedicated self-fence code before it
# can hold a VIP. Recreating c with the canonical config proves repair/rejoin.
wait_for_even_over_nodes 45 node-a node-b
wait_for_single_agreed_leader 20
wait_for_service_exit node-c 20
assert_service_exit_code node-c 4
assert_node_lacks_all_vips node-c
wait_for_log_any 10 'cluster configuration mismatch confirmed'
assert_unique_holders

export KEEPAFLOATD_E2E_CONFIG_DIR=configs
compose up -d --no-deps --force-recreate node-c >/dev/null
wait_for_service_running node-c 10
wait_for_even_over_nodes 45 "${NODES[@]}"
wait_for_single_agreed_leader 20
assert_unique_holders
