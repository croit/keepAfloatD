#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly VIP="${VIPS[0]}"
recovered="$(wait_for_stable_vip_holder "${VIP}" 15 3)" || {
  dump_cluster_diagnostics
  fail "${VIP} did not settle before the ownerless-gap check"
  exit 1
}

all_nodes_lack_vip() {
  local node
  for node in "${NODES[@]}"; do
    node_has_vip_bound "${node}" "${VIP}" && return 1
  done
  return 0
}

vip_held_by() {
  [[ "$(holder_for_vip "$1")" == "$2" ]]
}

# Make every node ineligible and prove the committed owner map reaches its ownerless state. This
# reproduces the gap that used to discard handoff history before a later assignment.
for node in "${NODES[@]}"; do
  set_node_unhealthy "${node}"
done
wait_until 20 all_nodes_lack_vip || {
  dump_cluster_diagnostics
  fail "${VIP} never became ownerless after every node failed health"
  exit 1
}

# With all other nodes still unhealthy, one recovered node must accept the orphan and converge to
# exactly one kernel holder. Unit/state-machine tests assert the one-round activation boundary;
# this scenario exercises the same transition through real processes and network I/O.
set_node_healthy "${recovered}"
wait_until 20 vip_held_by "${VIP}" "${recovered}" || {
  dump_cluster_diagnostics
  fail "recovered ${recovered} did not reacquire ownerless ${VIP}"
  exit 1
}
assert_unique_holders
runner_sh "arping -q -c 1 -w 1 -I eth0 ${VIP}" || {
  dump_cluster_diagnostics
  fail "reassigned ${VIP} is not reachable after the ownerless gap"
  exit 1
}
log "${VIP} became ownerless and then converged uniquely on recovered ${recovered}"
