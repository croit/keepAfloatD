#!/usr/bin/env bash
# D3: Silent holder death (staleness rule). SIGKILL a holder so it never publishes
# healthy:false. Survivors keep committing probe rounds; the dead node's committed round
# stops advancing and, once it exceeds the stale window, it is removed from eligibility and
# its VIPs are reassigned. (This is the staleness path, distinct from a graceful health fail.)
SCENARIO_NAME=D3_silent_death_staleness
source "$(dirname "$0")/../scenario.sh"
scenario_start "silently-killed holder is staled out and its VIPs reassigned"

snapshot_vips
old_leader_id="$(cluster_leader_id)"
victim=""
for i in "${!NODE_RAFT_IDS[@]}"; do
  [[ "${NODE_RAFT_IDS[$i]}" == "${old_leader_id}" ]] && victim="${NODE_IPS[$i]}"
done
check "current leader maps to a cluster node" test -n "${victim}"
[[ -n "${victim}" ]] || scenario_end

held=""
for vip in "${VIPS[@]}"; do
  node_sh "${victim}" "ip -o -4 addr show dev ${IFACE} | grep -F -q ' ${vip}/32 '" \
    && held+="${vip} "
done
evid "SIGKILL current leader ${victim} (raft id ${old_leader_id}, holds ${held:-none}); it cannot publish unhealthy"
check "silent-death victim starts as a VIP holder" test -n "${held}"
[[ -n "${held}" ]] || scenario_end

inst="$(instance_for_ip "${victim}")"
stale_secs="$(node_sh "${victim}" "sed -n 's/^[[:space:]]*stale_secs:[[:space:]]*//p' /etc/keepafloatd/config-${inst}.yaml")"
if [[ -z "${stale_secs}" ]]; then
  interval_ms="$(node_sh "${victim}" "sed -n 's/^[[:space:]]*interval_ms:[[:space:]]*//p' /etc/keepafloatd/config-${inst}.yaml")"
  if [[ "${interval_ms}" =~ ^[0-9]+$ && "${interval_ms}" -ge 1 ]]; then
    interval_secs=$(( (interval_ms + 999) / 1000 ))
    stale_secs=$(( interval_secs * 3 ))
    (( stale_secs >= 3 )) || stale_secs=3
  fi
fi
if [[ "${stale_secs}" =~ ^[0-9]+$ && "${stale_secs}" -ge 2 ]]; then
  pre_stale_secs=$((stale_secs - 1))
else
  pre_stale_secs=1
  check "configured stale window is an integer of at least two seconds" false
fi

fault_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
kafd_kill "${victim}"
start=$(date +%s)

victim_vips_absent_on_survivors() {
  local vip ip
  for vip in ${held}; do
    for ip in $(nodes_except "${victim}"); do
      if node_sh "${ip}" \
        "ip -o -4 addr show dev ${IFACE} | grep -F -q ' ${vip}/32 '"; then
        return 1
      fi
    done
  done
}
check "no survivor binds the orphaned VIP before the ${stale_secs}s stale fence" \
  holds_for "${pre_stale_secs}" victim_vips_absent_on_survivors

survivors_agree_on_new_leader() {
  local ip leader agreed=""
  for ip in $(nodes_except "${victim}"); do
    leader="$(leader_seen_by_since "${ip}" "${fault_since}")"
    [[ -n "${leader}" && "${leader}" != "${old_leader_id}" ]] || return 1
    [[ -z "${agreed}" || "${agreed}" == "${leader}" ]] || return 1
    agreed="${leader}"
  done
  [[ -n "${agreed}" ]]
}
check "survivors promptly elect a leader different from ${old_leader_id}" \
  wait_until 60 survivors_agree_on_new_leader
check "dead holder's VIPs are staled out and assigned to exactly one live node" \
  wait_until 120 vips_uniquely_served_by_live "${victim}"
elapsed=$(( $(date +%s) - start ))
evid "reassignment to live nodes completed ~${elapsed}s after kill (stale window enforced)"

# Restart: startup_cleanup reclaims the orphan, node rejoins cleanly.
kafd_start "${victim}"
rejoined_cluster_stable() {
  all_daemons_active && all_vips_uniquely_held && single_agreed_leader
}
check "restarted victim rejoins without double-bind or leader churn" \
  wait_until 60 holds_for 5 rejoined_cluster_stable
check "all VIPs remain reachable after rejoin" all_vips_pingable

scenario_end
