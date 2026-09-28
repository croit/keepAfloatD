#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly VIP="${VIPS[0]}"
original="$(wait_for_stable_vip_holder "${VIP}" 15 3)" || {
  dump_cluster_diagnostics
  fail "${VIP} did not settle on one holder before the nopreempt check"
  exit 1
}

vip_held_by() {
  [[ "$(holder_for_vip "$1")" == "$2" ]]
}

vip_moved_from() {
  local current
  current="$(holder_for_vip "$1")"
  [[ "${current}" != "$2" && "${current}" != "none" && "${current}" != duplicate:* ]]
}

# Fail the original holder and capture the actual replacement. Holder identity is deliberately
# dynamic because cold-start leader and assignment order are not API guarantees.
set_node_unhealthy "${original}"
wait_until 15 vip_moved_from "${VIP}" "${original}" || {
  dump_cluster_diagnostics
  fail "${VIP} did not leave unhealthy original holder ${original}"
  exit 1
}
replacement="$(holder_for_vip "${VIP}")"

# Recovery must not proactively take the VIP back under failback:false.
set_node_healthy "${original}"
for _ in {1..30}; do
  vip_held_by "${VIP}" "${replacement}" || {
    dump_cluster_diagnostics
    fail "${VIP} preempted ${replacement} after ${original} recovered"
    exit 1
  }
  sleep 0.2
done

# Fence the third node first, then fail the replacement. The recovered original is now the only
# eligible fallback and must accept the orphan; old permanent-block semantics failed here.
spare=""
for node in "${NODES[@]}"; do
  [[ "${node}" == "${original}" || "${node}" == "${replacement}" ]] || spare="${node}"
done
[[ -n "${spare}" ]] || {
  fail "could not identify spare node"
  exit 1
}
set_node_unhealthy "${spare}"
sleep 8
set_node_unhealthy "${replacement}"

sleep 2
node_has_vip_bound "${replacement}" "${VIP}" || {
  dump_cluster_diagnostics
  fail "${VIP} left ${replacement} before failover_delay_secs elapsed"
  exit 1
}
wait_until 15 vip_held_by "${VIP}" "${original}" || {
  dump_cluster_diagnostics
  fail "recovered ${original} did not accept orphaned ${VIP} after ${replacement} failed"
  exit 1
}
# Each VIP has its own release fence and acknowledgement, so a holder carrying multiple VIPs can
# release them in separate commits. Require bounded convergence of the complete set instead of
# assuming every handoff completes in the same polling instant as the target VIP.
wait_until 15 all_vips_uniquely_held || {
  dump_cluster_diagnostics
  fail "VIP ownership did not converge after nopreempt fallback"
  exit 1
}
assert_unique_holders
log "${VIP} stayed on ${replacement} after recovery, then fell back to ${original} when needed"
