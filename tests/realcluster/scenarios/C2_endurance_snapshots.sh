#!/usr/bin/env bash
# C2: Endurance through snapshot/purge cycles - the exact path the fixed bug lived in. We
# accelerate health churn (lower interval and a deterministic sentinel probe so openraft crosses its
# since_last:5000 snapshot threshold quickly), then run long enough to build snapshots + purges and assert
# ZERO Defensive/quit-RaftCore on any node and unique VIPs in repeated kernel samples.
SCENARIO_NAME=C2_endurance_snapshots
source "$(dirname "$0")/../scenario.sh"
scenario_start "endurance across multiple snapshot+purge cycles (no Defensive/quit)"

# Accelerate churn on all nodes. Back up the complete generated config because the real RGW probe
# and all timing values must be restored exactly after the campaign.
backup_cluster_configs c2
clear_health_sentinels
configure_sentinel_health 50 500 5
clean_reform
evid "accelerated churn (interval_ms=50, deterministic sentinel probe); original configs backed up"
check "cluster steady at accelerated rate" wait_for_even 60 "${NODE_IPS[@]}"

# Pin every node's invocation so one busy node or a restart cannot supply another node's proof.
declare -A snapshot_contexts cycles
for ip in "${NODE_IPS[@]}"; do
  snapshot_contexts["${ip}"]="$(timed_node_command "${ip}" :)"
done
evid "running endurance window until two snapshot cycles (max ~20 min)..."
deadline=$(( $(date +%s) + 1200 ))
ownership_failures=0; samples=0
while (( $(date +%s) < deadline )); do
  complete=1
  for ip in "${NODE_IPS[@]}"; do
    cycles["${ip}"]="$(node_snapshot_cycles "${ip}" "${snapshot_contexts[${ip}]}")"
    (( cycles["${ip}"] >= 2 )) || complete=0
  done
  samples=$((samples + 1))
  all_vips_uniquely_held || ownership_failures=$((ownership_failures + 1))
  (( complete )) && break
  sleep 2
done
evid "endurance: ownership-samples=${samples}, failed-samples=${ownership_failures}"

for ip in "${NODE_IPS[@]}"; do
  final_cycles="$(node_snapshot_cycles "${ip}" "${snapshot_contexts[${ip}]}")"
  evid "${ip}: completed snapshots followed by advancing purge commands=${final_cycles}"
  check "node ${ip} completed two distinct snapshot/purge cycles without Raft errors or restart" \
    test "${final_cycles}" -ge 2
  check "node ${ip} still active" node_active "${ip}"
done
check "kernel samples were collected" test "${samples}" -gt 0
check "every kernel sample had exactly one holder per VIP" test "${ownership_failures}" -eq 0

# Restore the exact generated configs.
restore_cluster_configs c2
clean_reform

scenario_end
