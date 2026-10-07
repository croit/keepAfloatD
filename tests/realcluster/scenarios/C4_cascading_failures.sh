#!/usr/bin/env bash
# C4: Cascading / flapping failures. Repeatedly kill and restart rotating nodes (one at a
# time, always keeping a 2/3 quorum). After each cycle the cluster must reconverge to unique
# holders with no permanent double-bind. Stresses minimal-movement + handoff fencing under
# churn.
SCENARIO_NAME=C4_cascading_failures
source "$(dirname "$0")/../scenario.sh"
scenario_start "rotating kill/restart keeps unique live service and reclaims every orphan"


rounds=6
not_served=0
for r in $(seq 1 "${rounds}"); do
  idx=$(( (r - 1) % 3 ))
  victim="${NODE_IPS[$idx]}"
  evid "round ${r}: kill ${victim}"
  kafd_kill "${victim}"
  # Every VIP must (re)appear on a live survivor within the failover window. The dead node's
  # orphan is ignored - what matters is that a healthy node also serves each VIP. After a SIGKILL
  # the dead holder is reassigned via the staleness path (it can't publish unhealthy or ack a
  # release), which takes the full stale window plus the safe-handoff holdoff - ~60-90s observed,
  # so use the same generous window as D3.
  if ! wait_until 120 vips_uniquely_served_by_live "${victim}"; then
    evid "  ✗ round ${r}: live survivor ownership was missing or duplicated"; not_served=$(( not_served + 1 ))
  fi
  evid "round ${r}: restart ${victim}"
  kafd_start "${victim}"
  if ! wait_until 30 holds_for 3 node_active "${victim}"; then
    evid "  ✗ round ${r}: restarted daemon did not remain active"; _PASS=0
  fi
  # After rejoin + startup_cleanup, the cluster must converge to unique holders (no permanent
  # double-bind; the orphan must be reclaimed).
  if ! wait_until 90 all_vips_uniquely_held; then
    evid "  ✗ round ${r}: cluster did not return to unique holders after rejoin"; _PASS=0
  fi
  check "restarted daemon completes activation before the next fault" \
    wait_for_startup_activation 30 "${victim}"
done

check "every VIP gained exactly one live holder across all ${rounds} rounds" test "${not_served}" -eq 0
check "unique ownership remains stable at end" holds_for 5 all_vips_uniquely_held
check "all restarted daemons remain active at end" holds_for 5 all_daemons_active
check "all VIPs remain reachable at end" all_vips_pingable
check "single agreed leader at end" wait_until 30 single_agreed_leader

scenario_end
