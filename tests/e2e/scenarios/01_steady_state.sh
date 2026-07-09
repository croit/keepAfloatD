#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

wait_for_even_over_nodes 20 "${NODES[@]}"
assert_unique_holders
# Any node may win formation; assert one agreed leader, not a specific id.
wait_for_single_agreed_leader 20
