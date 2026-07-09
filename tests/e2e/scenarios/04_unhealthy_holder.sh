#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

set_node_unhealthy node-a
# node-a is unhealthy (still running); its VIPs move off onto the two healthy nodes, evenly.
wait_for_even_over_nodes 12 node-b node-c
assert_node_lacks_all_vips node-a
