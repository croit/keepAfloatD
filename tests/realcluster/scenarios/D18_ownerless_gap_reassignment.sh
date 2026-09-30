#!/usr/bin/env bash
# D18: Every node becomes explicitly unhealthy, so the replicated owner map becomes empty. A lone
# recovered node must then reacquire all VIPs without duplicate kernel holders. The unit and
# state-machine regressions pin the retained-generation activation holdoff for this transition.
SCENARIO_NAME=D18_ownerless_gap_reassignment
source "$(dirname "$0")/../scenario.sh"
scenario_start "ownerless health gap safely reacquires VIPs through retained generations"

tag=d18
recovered="${NODE_IPS[0]}"
backup_cluster_configs "${tag}"
clear_health_sentinels
configure_sentinel_health 500 100 3
set_cluster_scalar failover_delay_secs 0
set_cluster_scalar failback true
set_cluster_scalar failback_delay_secs 0
clean_reform

check "sentinel cluster reaches an even baseline" wait_for_even 45 "${NODE_IPS[@]}"

evid "failing health on every node; every VIP must leave every kernel"
for ip in "${NODE_IPS[@]}"; do sentinel_fail "${ip}"; done
check "all VIPs enter a genuinely ownerless state" wait_until 30 all_cluster_vips_absent
check "ownerless state remains stable" holds_for 2 all_cluster_vips_absent

evid "recovering only ${recovered}; retained generations must fence safe reacquisition"
sentinel_recover "${recovered}"
check "all VIPs converge on the sole recovered node" wait_for_even 45 "${recovered}"
check "reassignment has exactly one kernel holder per VIP" assert_unique_holders
check "reassigned VIPs are reachable" all_vips_pingable

clear_health_sentinels
restore_cluster_configs "${tag}"
clean_reform
scenario_end
