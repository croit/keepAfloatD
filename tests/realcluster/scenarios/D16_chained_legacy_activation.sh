#!/usr/bin/env bash
# D16: Preserve real nopreempt history when a VIP changes holder twice before Legacy-to-V2
# activation. Legacy records node-level blocks but only the latest previous holder, so activation
# must not infer that an older failed owner is safe to preempt.
SCENARIO_NAME=D16_chained_legacy_activation
source "$(dirname "$0")/../lib.sh"
old_buildid="${LEGACY_BUILDID:?Set LEGACY_BUILDID for this compatibility experiment}"
old_binary="${LEGACY_BINARY:?Set LEGACY_BINARY to the prepared executable on every node}"
source "$(dirname "$0")/../scenario.sh"
scenario_start "chained legacy handoffs keep every real failed owner nopreempt after V2 activation"

tag=d16
current_backup="/usr/bin/keepafloatd.d16-current"
target_vip="${VIPS[0]}"

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

restore_candidate_state() {
  restore_current_binaries
  clear_health_sentinels
  restore_cluster_configs "${tag}"
  clean_reform
}

require_step() {  # require_step <description> <command...>
  local desc="${1:?}"
  shift
  if "$@"; then
    evid "  ✓ ${desc}"
    return 0
  fi
  evid "  ✗ ${desc}"
  _PASS=0
  dump_diag >>"${_EVID}" 2>&1 || true
  restore_candidate_state
  scenario_end
}

activation_logged_since() {
  journal_event_seen_on_any_node_since "${1:?}" \
    'activated failover semantics V2 after all voters upgraded'
}

activation_not_logged_since() {
  journal_event_absent_on_all_nodes_since "${1:?}" \
    'activated failover semantics V2 after all voters upgraded'
}

holder_excludes() {  # holder_excludes <vip> <node>...
  local vip="${1:?}" holder excluded
  shift
  snapshot_vips
  holder="$(holder_for_vip "${vip}")"
  [[ "${holder}" != "none" && "${holder}" != duplicate:* ]] || return 1
  for excluded in "$@"; do [[ "${holder}" != "${excluded}" ]] || return 1; done
}

node_holds_all_vips() {
  local node="${1:?}" vip
  snapshot_vips
  for vip in "${VIPS[@]}"; do node_has_vip_bound "${node}" "${vip}" || return 1; done
}

upgraded_blocked_owners_are_caught_up() {
  local owner
  for owner in "$@"; do
    node_active "${owner}" || return 1
    node_lacks_all_vips "${owner}" || return 1
  done
  node_holds_all_vips "${third_owner}" && all_vips_uniquely_held
}

preflight=1
for ip in "${NODE_IPS[@]}"; do
  check_eq "node ${ip} begins on the candidate binary" "$(binary_buildid "${ip}")" "${FIXED_BUILDID}"
  node_sh "${ip}" "test -x ${old_binary}" || preflight=0
done
check "configured legacy binary exists on every node" test "${preflight}" -eq 1
if [[ "${preflight}" -ne 1 ]]; then
  scenario_end
fi

backup_cluster_configs "${tag}"
clear_health_sentinels
# Keep the semantic fixture at the normal three-node publication rate. A 500 ms interval injected
# six writes per second into the Ceph-loaded lab; the old leader then spent its whole RPC/submit
# budget draining probe writes and self-fenced before the chained-handoff assertion was reachable.
configure_sentinel_health 2000 1000 6
set_cluster_scalar failover_delay_secs 0
set_cluster_scalar failback false
set_cluster_scalar failback_delay_secs 0
for ip in "${NODE_IPS[@]}"; do
  node_sh "${ip}" "cp -f /usr/bin/keepafloatd ${current_backup}"
done

evid "forming a fresh Legacy cluster with deterministic sentinel health"
for ip in "${NODE_IPS[@]}"; do kafd_stop "${ip}"; done
for ip in "${NODE_IPS[@]}"; do node_sh "${ip}" "cp -f ${old_binary} /usr/bin/keepafloatd"; done
for ip in "${NODE_IPS[@]}"; do kafd_start "${ip}"; done
check "all-legacy cluster forms and serves VIPs" wait_for_steady_state

snapshot_vips
first_owner="$(holder_for_vip "${target_vip}")"
evid "${target_vip} initial owner: ${first_owner}"
check "target VIP starts with one holder" instance_for_ip "${first_owner}"
if ! instance_for_ip "${first_owner}" >/dev/null; then
  restore_candidate_state
  scenario_end
fi

evid "failing first owner ${first_owner}; target must hand off once"
sentinel_fail "${first_owner}"
check "target leaves first owner" wait_until 45 holder_excludes "${target_vip}" "${first_owner}"
snapshot_vips
second_owner="$(holder_for_vip "${target_vip}")"
evid "${target_vip} first replacement: ${second_owner}"
check "first replacement is one cluster node" instance_for_ip "${second_owner}"
if ! instance_for_ip "${second_owner}" >/dev/null; then
  restore_candidate_state
  scenario_end
fi
sentinel_recover "${first_owner}"
check "first recovered owner remains VIP-less under Legacy nopreempt" \
  holds_for 6 node_lacks_all_vips "${first_owner}"

evid "failing replacement ${second_owner}; target must hand off a second time"
sentinel_fail "${second_owner}"
check "target leaves both earlier owners" \
  wait_until 45 holder_excludes "${target_vip}" "${first_owner}" "${second_owner}"
snapshot_vips
third_owner="$(holder_for_vip "${target_vip}")"
evid "${target_vip} second replacement: ${third_owner}"
check "second replacement is one cluster node" instance_for_ip "${third_owner}"
if ! instance_for_ip "${third_owner}" >/dev/null; then
  restore_candidate_state
  scenario_end
fi
sentinel_recover "${second_owner}"
check "second recovered owner remains VIP-less under Legacy nopreempt" \
  holds_for 6 node_lacks_all_vips "${second_owner}"
check "last eligible Legacy owner carries all VIPs" wait_until 45 node_holds_all_vips "${third_owner}"

activation_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
evid "rolling the two blocked owners to the candidate while the active owner stays legacy"
deploy_binary "${first_owner}" "${current_backup}"
require_step "first upgraded owner catches up before the next voter stops" \
  wait_until 45 upgraded_blocked_owners_are_caught_up "${first_owner}"
require_step "first upgraded owner remains caught up before the next voter stops" \
  wait_until 60 holds_for 8 upgraded_blocked_owners_are_caught_up "${first_owner}"
deploy_binary "${second_owner}" "${current_backup}"
require_step "second upgraded owner catches up before the final voter stops" \
  wait_until 45 upgraded_blocked_owners_are_caught_up "${first_owner}" "${second_owner}"
require_step "both upgraded owners remain caught up before the final voter stops" \
  wait_until 60 holds_for 8 upgraded_blocked_owners_are_caught_up "${first_owner}" "${second_owner}"
sleep 8
require_step "V2 remains gated by the final legacy voter" \
  activation_not_logged_since "${activation_since}"

evid "upgrading the final owner; activation must preserve both older nopreempt blocks"
deploy_binary "${third_owner}" "${current_backup}"
require_step "final upgraded owner rejoins" wait_until 45 node_active "${third_owner}"
require_step "all-voter upgrade commits V2 activation" \
  wait_until 45 activation_logged_since "${activation_since}"
check "first chained owner remains VIP-less after activation" \
  holds_for 12 node_lacks_all_vips "${first_owner}"
check "second chained owner remains VIP-less after activation" \
  holds_for 12 node_lacks_all_vips "${second_owner}"
check "latest holder retains all VIPs without proactive failback" \
  holds_for 12 node_holds_all_vips "${third_owner}"
check "VIP ownership remains unique after activation" holds_for 12 all_vips_uniquely_held

evid "restoring candidate binaries and baseline RGW health configuration"
restore_candidate_state
for ip in "${NODE_IPS[@]}"; do
  check_eq "node ${ip} restored to candidate BuildID" "$(binary_buildid "${ip}")" "${FIXED_BUILDID}"
done
check "cluster returns to baseline steady state" wait_for_steady_state
scenario_end
