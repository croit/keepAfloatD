#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Work item #14: VIP ownership is STICKY -- a healthy node never has a VIP yanked off it just because
# the cluster membership changed. We prove the load-bearing half of that here: when a node dies, ONLY
# the VIP it was holding moves; the VIPs on the still-healthy survivors stay exactly where they were.
# The pre-#14 round-robin would reshuffle the survivors' VIPs whenever the eligible set changed, so
# this scenario fails on the old code and passes on the sticky code.
#
# (The complementary "a recovered node does not flap a VIP back" property is shown unconditionally by
# the 2-VIP/3-node dry-run harness and the Rust unit tests; with 3 VIPs over 3 nodes here the
# minimal-movement rebalance legitimately requires a returning node to take one VIP back to keep the
# spread even, so we only assert balance on rejoin.)
#
# The exact steady VIP->node mapping is a formation race under sticky placement, so we OBSERVE it
# rather than assume it.
wait_for_single_agreed_leader 20

# Snapshot which node holds each VIP at steady state (one VIP per node).
declare -A holder_before
for vip in "${VIPS[@]}"; do
  holder_before["${vip}"]="$(holder_for_vip "${vip}")"
done
log "steady layout: $(current_assignments_summary)"

# Kill node-a, the lowest id -- the strongest case, since the old round-robin remaps by (index mod
# eligible) and dropping the lowest id shifts every wrapped VIP, churning the survivors needlessly.
kill_service node-a KILL
wait_for_service_exit node-a 10

# node-a's VIP fails over; node-b and node-c keep serving, balanced and uniquely held.
wait_for_even_over_nodes 30 node-b node-c

# Stickiness: every VIP that was NOT on node-a must still be on the SAME node it started on -- a
# healthy survivor never gives up a VIP just because node-a left.
for vip in "${VIPS[@]}"; do
  before="${holder_before[${vip}]}"
  [[ "${before}" == "node-a" ]] && continue
  now="$(holder_for_vip "${vip}")"
  [[ "${now}" == "${before}" ]] || {
    dump_cluster_diagnostics
    fail "VIP ${vip} moved off healthy ${before} to ${now} when node-a left (expected sticky)"
    exit 1
  }
done

# Bring node-a back blank; it rejoins via replication and the cluster rebalances to one VIP per node.
restart_checkpoint="$(log_checkpoint)"
start_service node-a
wait_for_service_running node-a 10
wait_for_log_any_after "${restart_checkpoint}" 30 'joining via replication instead of forming a new one'
wait_for_even_over_nodes 45 "${NODES[@]}"
assert_unique_holders
wait_for_single_agreed_leader 15
