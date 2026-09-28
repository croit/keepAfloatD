#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# All nodes run nopreempt. Nodes a/b configure failback_delay_secs=1 and stale_secs=10 while c
# configures 99 and 11. The delay is ignored under nopreempt, and a three-second probe interval
# converts both stale values to three missed rounds, so all effective identities must match.
wait_for_steady_state
assert_unique_holders

for node in "${NODES[@]}"; do
  service_is_running "${node}" || {
    dump_cluster_diagnostics
    fail "${node} exited despite an equivalent effective configuration"
    exit 1
  }
done

if cluster_logs_contain 'cluster configuration mismatch confirmed'; then
  dump_cluster_diagnostics
  fail "equivalent nopreempt configurations triggered the mismatch fence"
  exit 1
fi

log "equivalent nopreempt/staleness configurations formed one healthy three-node cluster"
