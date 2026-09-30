#!/usr/bin/env bash
# D15: Exercise the rolling compatibility gate with a supplied legacy binary. A legacy
# cluster must remain on legacy semantics while one voter is old, activate only after the last
# voter upgrades, and then fence a returning legacy process from Raft/VIP ownership.
SCENARIO_NAME=D15_mixed_version_activation
source "$(dirname "$0")/../lib.sh"
old_buildid="${LEGACY_BUILDID:?Set LEGACY_BUILDID for this compatibility experiment}"
old_binary="${LEGACY_BINARY:?Set LEGACY_BINARY to the prepared executable on every node}"
source "$(dirname "$0")/../scenario.sh"
scenario_start "mixed old/new voters gate V2 activation and fence a legacy return"

current_backup="/usr/bin/keepafloatd.d15-current"

binary_buildid() {
  running_keepafloatd_buildid "${1:?}"
}

deploy_binary() {  # deploy_binary <node> <source>
  replace_keepafloatd_binary "${1:?}" "${2:?}"
}

restore_current_binaries() {
  local ip current
  for ip in "${NODE_IPS[@]}"; do
    if node_sh "${ip}" "test -f ${current_backup}"; then
      deploy_binary "${ip}" "${current_backup}" || return 1
      node_sh "${ip}" "rm -f ${current_backup}" || return 1
    else
      current="$(binary_buildid "${ip}")" || return 1
      [[ "${current}" == "${FIXED_BUILDID}" ]] || return 1
    fi
  done
}

activation_logged_since() {
  journal_event_seen_on_any_node_since "${1:?}" \
    'activated failover semantics V2 after all voters upgraded'
}

activation_not_logged_since() {
  journal_event_absent_on_all_nodes_since "${1:?}" \
    'activated failover semantics V2 after all voters upgraded'
}

preflight=1
for ip in "${NODE_IPS[@]}"; do
  check_eq "node ${ip} begins on the fixed binary" "$(binary_buildid "${ip}")" "${FIXED_BUILDID}"
  node_sh "${ip}" "test -x ${old_binary}" || preflight=0
done
check "configured legacy binary exists on every node" test "${preflight}" -eq 1
if [[ "${preflight}" -ne 1 ]]; then
  scenario_end
fi

for ip in "${NODE_IPS[@]}"; do
  node_sh "${ip}" "cp -f /usr/bin/keepafloatd ${current_backup}"
done

evid "starting a fresh three-node cluster on the real legacy binary"
for ip in "${NODE_IPS[@]}"; do kafd_stop "${ip}"; done
for ip in "${NODE_IPS[@]}"; do node_sh "${ip}" "cp -f ${old_binary} /usr/bin/keepafloatd"; done
for ip in "${NODE_IPS[@]}"; do kafd_start "${ip}"; done
check "all-legacy cluster forms and serves VIPs" wait_for_steady_state
for ip in "${NODE_IPS[@]}"; do
  check_eq "node ${ip} is running the legacy BuildID" "$(binary_buildid "${ip}")" "${old_buildid}"
done

activation_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
evid "rolling upgrade: two voters new, final voter remains legacy"
deploy_binary "${NODE_IPS[0]}" "${current_backup}"
check "first upgraded voter catches up before the next voter stops" \
  wait_for_available_cluster
deploy_binary "${NODE_IPS[1]}" "${current_backup}"
check "second upgraded voter catches up before the final voter stops" \
  wait_for_available_cluster
sleep 8
check "V2 does not activate while one voter is legacy" \
  activation_not_logged_since "${activation_since}"

evid "upgrading final voter; the existing legacy state must activate V2"
deploy_binary "${NODE_IPS[2]}" "${current_backup}"
check "final upgraded voter rejoins" wait_until 45 node_active "${NODE_IPS[2]}"
check "all-voter upgrade commits V2 activation" \
  wait_until 45 activation_logged_since "${activation_since}"
check "all-new activated cluster is safely available" wait_for_available_cluster

evid "returning one legacy process after activation; it must be fenced"
deploy_binary "${NODE_IPS[2]}" "${old_binary}"
check_eq "returning process really is the legacy BuildID" \
  "$(binary_buildid "${NODE_IPS[2]}")" "${old_buildid}"
check "returning legacy daemon remains running while fenced" wait_until 30 node_active "${NODE_IPS[2]}"
check "activated majority serves every VIP uniquely and reachably" \
  wait_for_live_service_without 90 "${NODE_IPS[2]}"
# Sustained kernel state is the public safety contract. Depending on connection direction, the new
# peers may drop their outbound legacy links before an inbound RPC exists, so a particular warning
# line is not guaranteed and is not a valid behavioral assertion.
check "returning legacy node remains VIP-less throughout the fence window" \
  holds_for 12 node_lacks_all_vips "${NODE_IPS[2]}"
check "VIP ownership remains unique throughout mixed-version fencing" \
  holds_for 12 all_vips_uniquely_held

evid "restoring the fixed binary on all nodes"
restore_current_binaries
for ip in "${NODE_IPS[@]}"; do
  check_eq "node ${ip} restored to fixed BuildID" "$(binary_buildid "${ip}")" "${FIXED_BUILDID}"
done
check "all-current cluster returns to safe service without forced failback" \
  wait_for_available_cluster
scenario_end
