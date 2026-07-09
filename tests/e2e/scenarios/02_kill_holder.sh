#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

kill_service node-a KILL
wait_for_service_exit node-a 10
# node-a is down; the three VIPs collapse evenly onto the surviving pair (one node holds two).
wait_for_even_over_nodes 30 node-b node-c
