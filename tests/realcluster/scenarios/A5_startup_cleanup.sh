#!/usr/bin/env bash
# A5: Crash-safety startup cleanup. SIGKILL a holder (no graceful unbind, VIP stays on the
# kernel as an orphan). The cluster reassigns the VIP (so it's briefly double-present), then
# on restart vip::startup_cleanup must reclaim the orphan so the killed node holds nothing
# stale and there's no permanent double-bind.
SCENARIO_NAME=A5_startup_cleanup
source "$(dirname "$0")/../scenario.sh"
scenario_start "startup_cleanup reclaims orphan VIP after SIGKILL"

snapshot_vips
victim="$(holder_for_vip "${VIPS[2]}")"
vip="${VIPS[2]}"
evid "SIGKILL keepafloatd holding ${vip} on ${victim} (leaves orphan in kernel)"
cleanup_context="$(timed_node_command "${victim}" :)"

kafd_kill "${victim}"
# The orphan should still be on the victim's kernel immediately after kill (no graceful unbind).
sleep 2
orphan_present="$(node_sh "${victim}" "set -o pipefail; ip -o -4 addr show dev ${IFACE} | awk -v needle=' ${vip}/32 ' 'index(\$0, needle) { count++ } END { print count + 0 }'")"
evid "orphan ${vip} present on killed node kernel: ${orphan_present} (expected 1: SIGKILL skips graceful unbind)"
check_eq "SIGKILL leaves the VIP orphaned in the kernel" "${orphan_present}" "1"

# Restart the victim: startup_cleanup must reclaim the orphan before it rejoins, so the cluster
# converges to UNIQUE holders (the core invariant - no permanent double-bind). We assert the final
# converged state rather than the transient even-over-survivors window (reassignment can race the
# orphan still being on the dead node's kernel for a moment).
kafd_start "${victim}"
check "restarted victim remains active" wait_until 30 holds_for 5 node_active "${victim}"
check "cluster converges to unique holders and stays there" \
  wait_until 90 holds_for 5 all_vips_uniquely_held
check "settled ownership remains free of double-binds" holds_for 5 no_vip_is_duplicate
check "new process reclaimed the exact orphan VIP on the expected interface" \
  wait_until 15 startup_orphan_reclaimed "${victim}" "${cleanup_context}" "${vip}"
check "all VIPs remain reachable after cleanup" wait_until 15 all_vips_pingable
check "all three daemons remain active after cleanup" holds_for 5 all_daemons_active

scenario_end
