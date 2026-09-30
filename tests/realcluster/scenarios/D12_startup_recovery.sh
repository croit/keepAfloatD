#!/usr/bin/env bash
# D12: With failback:false, a node whose first probe is unhealthy has
# never failed as an owner and must not be marked nopreempt. Its later first healthy report must
# make it a normal rebalance receiver so the cluster returns from 2/3 to 1/1/1 placement.
SCENARIO_NAME=D12_startup_recovery
source "$(dirname "$0")/../scenario.sh"
scenario_start "unknown -> unhealthy -> healthy startup joins the balance"

tag=d12
startup_node="${NODE_IPS[0]}"
backup_cluster_configs "${tag}"
clear_health_sentinels
sentinel_fail "${startup_node}"
configure_sentinel_health 500 100 3
set_cluster_scalar failover_delay_secs 0
set_cluster_scalar failback false
set_cluster_scalar failback_delay_secs 0
startup_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
clean_reform

check "startup-unhealthy node stays VIP-less" wait_until 30 node_lacks_all_vips "${startup_node}"
check "two healthy nodes carry all VIPs" wait_for_even 45 $(nodes_except "${startup_node}")
startup_failure_log="$(kafd_log_since "${startup_node}" "${startup_since}" | grep -E 'health probe exited non-zero|effective local health became unhealthy' | tail -2)"
evid "startup-unhealthy evidence:"; evid "${startup_failure_log:-  (none)}"
check "the first startup probe was observed unhealthy" test -n "${startup_failure_log}"

evid "removing startup failure on ${startup_node}; it must join normal balancing"
sentinel_recover "${startup_node}"
check "startup-recovered node receives a VIP" wait_until 45 node_has_any_vip "${startup_node}"
check "cluster returns to one VIP per node" wait_for_even 45 "${NODE_IPS[@]}"

clear_health_sentinels
restore_cluster_configs "${tag}"
clean_reform
scenario_end
