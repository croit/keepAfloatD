#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

install_minority_partition
# node-a becomes the minority (1 of 3): it loses quorum and unbinds, while the majority {node-b,
# node-c} keeps serving and holds every VIP. Reaching the two-node assignment is itself proof the
# majority has a working leader; the leader id is not asserted (it is non-deterministic).
wait_for_even_over_nodes 20 node-b node-c
assert_node_lacks_all_vips node-a
