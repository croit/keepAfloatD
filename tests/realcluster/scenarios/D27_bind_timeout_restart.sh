#!/usr/bin/env bash
# D27: A holder whose `ip addr replace` for a VIP it already holds misses its deadline once stops,
# hands off, and is restarted by its supervisor; the fresh process re-enters placement. A replace
# that fails outright instead keeps the holder out without a restart.
SCENARIO_NAME=D27_bind_timeout_restart
source "$(dirname "$0")/../scenario.sh"
scenario_start "a timed-out VIP reassert restarts the holder; a failed one keeps it out"

tag=d27
target_vip="${VIPS[0]}"
backup_live=0
faults_live=0

install_ip_fault() {
  local ip="${1:?}" inst
  inst="$(instance_for_ip "${ip}")"
  node_sh "${ip}" "
    mkdir -p /run/kafd-ipfault /run/systemd/system/keepafloatd@${inst}.service.d
    printf '%s\n' \
      '#!/bin/sh' \
      'if [ \"\${1-}\" = -4 ] && [ \"\${2-}\" = addr ] && [ \"\${3-}\" = replace ] && [ \"\${4-}\" = ${target_vip}/32 ]; then' \
      '  mode=\$(cat /run/kafd-ipfault/mode 2>/dev/null)' \
      '  if [ \"\${mode}\" = timeout-once ]; then' \
      '    printf \"%s\\n\" passthrough > /run/kafd-ipfault/mode' \
      '    sleep 2' \
      '    exit 2' \
      '  fi' \
      '  if [ \"\${mode}\" = fail ]; then exit 2; fi' \
      'fi' \
      'exec /usr/sbin/ip \"\$@\"' > /run/kafd-ipfault/ip
    chmod 0755 /run/kafd-ipfault/ip
    printf '%s\n' passthrough > /run/kafd-ipfault/mode
    printf '%s\n' '[Service]' 'Environment=PATH=/run/kafd-ipfault:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin' \
      > /run/systemd/system/keepafloatd@${inst}.service.d/zz-kafd-ipfault.conf
    systemctl daemon-reload
  "
}

remove_ip_faults() {
  local ip inst failed=0
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")" || { failed=1; continue; }
    node_sh "${ip}" "
      set -e
      rm -f /run/systemd/system/keepafloatd@${inst}.service.d/zz-kafd-ipfault.conf
      rmdir /run/systemd/system/keepafloatd@${inst}.service.d 2>/dev/null || true
      rm -rf /run/kafd-ipfault
      systemctl daemon-reload
    " || failed=1
  done
  return "${failed}"
}

main_pid() {
  node_sh "${1:?}" "systemctl show --property=MainPID --value keepafloatd@$(instance_for_ip "${1}")"
}

# A duplicate seen while polling fails the scenario even if ownership is unique again later.
duplicate_seen=0
record_duplicates() {
  local vip
  for vip in "${VIPS[@]}"; do
    [[ "$(holder_for_vip "${vip}")" != duplicate:* ]] || duplicate_seen=1
  done
}
no_duplicate_was_seen() { [[ "${duplicate_seen}" -eq 0 ]]; }

replacement_ready() {
  snapshot_vips || return 1
  record_duplicates
  local holder
  holder="$(holder_for_vip "${target_vip}")"
  [[ "${holder}" != none && "${holder}" != "${victim}" && "${holder}" != duplicate:* ]]
}

journal_has() {
  kafd_log_since "${victim}" "${since}" | grep -F "${1:?}" >/dev/null
}

victim_restarted() {
  local pid
  snapshot_vips && record_duplicates
  pid="$(main_pid "${victim}")" || return 1
  [[ "${pid}" != 0 && "${pid}" != "${victim_pid}" ]] && node_active "${victim}"
}

victim_not_restarted() {
  snapshot_vips && record_duplicates
  [[ "$(main_pid "${victim}")" == "${victim_pid}" ]] && node_active "${victim}"
}

victim_stays_out() {
  node_lacks_all_vips "${victim}" || return 1
  record_duplicates
}

# Fail closed: an unreadable snapshot is no evidence that the victim holds a VIP.
victim_holds_a_vip() {
  snapshot_vips || return 1
  record_duplicates
  local vip
  for vip in "${VIPS[@]}"; do
    [[ "$(holder_for_vip "${vip}")" == "${victim}" ]] && return 0
  done
  return 1
}

concrete_holder() { [[ "${1}" != none && "${1}" != duplicate:* ]]; }

# Configs and fault injection are restored independently; reform only from a fully restored lab,
# otherwise leave it for the campaign guard or operator.
d27_restore_mutations() {
  local restored=1 mutated=0
  if [[ "${backup_live}" -eq 1 ]]; then
    mutated=1
    if restore_cluster_configs "${tag}"; then backup_live=0; else restored=0; fi
  fi
  if [[ "${faults_live}" -eq 1 ]]; then
    mutated=1
    if remove_ip_faults; then faults_live=0; else restored=0; fi
  fi
  [[ "${restored}" -eq 1 ]] || return 1
  if [[ "${mutated}" -eq 1 ]]; then clean_reform || return 1; fi
  return 0
}

restore_d27() {
  local status=$?
  trap - ERR EXIT INT TERM
  d27_restore_mutations || status=1
  exit "${status}"
}
trap restore_d27 EXIT

# Undo D27's own mutations before the harness's baseline recovery reforms the cluster.
d27_unexpected_error() {
  local status="${1:?}" command="${2:-unknown command}"
  trap - ERR
  if [[ "${BASHPID}" == "${_SCENARIO_GUARD_PID:-}" ]] && ! d27_restore_mutations; then
    evid "  ✗ unchecked command failed (${status}): ${command}; D27 restoration failed, baseline recovery skipped"
    exit 1
  fi
  scenario_unexpected_error "${status}" "${command}"
}
trap 'd27_unexpected_error "$?" "$BASH_COMMAND"' ERR

backup_cluster_configs "${tag}" || {
  evid "config capture failed; recovery was not started"
  exit 1
}
backup_live=1
# The restarted holder may only regain a VIP from healthy peers through failback.
set_cluster_scalar failback true
set_cluster_scalar failback_delay_secs 0
faults_live=1
for ip in "${NODE_IPS[@]}"; do install_ip_fault "${ip}"; done
clean_reform
check "fault-injected cluster reaches an even baseline" wait_for_even 45 "${NODE_IPS[@]}"

snapshot_vips
victim="$(holder_for_vip "${target_vip}")"
check "target begins with one concrete holder" concrete_holder "${victim}"
victim_pid="$(main_pid "${victim}")"
since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
evid "making the next reassert of ${target_vip} on ${victim} miss its deadline once"
node_sh "${victim}" "printf '%s\n' timeout-once > /run/kafd-ipfault/mode"
check "holder reports the timed-out reassert and asks for a restart" \
  wait_until 30 journal_has "VIP command timed out; restarting the daemon to prove the release"
# A timeout restart acknowledges no release, so the peer takes over after the stale-holder fence.
check "a healthy peer takes over the target VIP" wait_until 120 replacement_ready
check "the supervisor restarts the holder with a fresh process" wait_until 30 victim_restarted
check "ownership stays unique while the holder restarts" holds_for 5 no_vip_is_duplicate
check "restarted holder reaches activation" wait_for_startup_activation 30 "${victim}"
check "restarted holder holds a VIP again" wait_until 60 victim_holds_a_vip
check "ownership is unique after the holder re-entered placement" no_vip_is_duplicate
check "no duplicate ownership was seen during the timeout restart" no_duplicate_was_seen

snapshot_vips
victim="$(holder_for_vip "${target_vip}")"
check "target has one concrete holder before the failing reassert" concrete_holder "${victim}"
victim_pid="$(main_pid "${victim}")"
since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
evid "making every replace of ${target_vip} on ${victim} fail outright"
node_sh "${victim}" "printf '%s\n' fail > /run/kafd-ipfault/mode"
check "holder reports the failed reassert as a latched bind fault" \
  wait_until 30 journal_has "VIP bind failed; node remains unhealthy until restart"
check "a healthy peer takes over the target VIP" wait_until 30 replacement_ready
check "a failed bind keeps the holder running without a restart" holds_for 20 victim_not_restarted
check "the latched holder stays out of placement" holds_for 10 victim_stays_out
check "ownership stays unique with the holder latched" no_vip_is_duplicate
check "no duplicate ownership was seen during the latched failure" no_duplicate_was_seen

restore_cluster_configs "${tag}"
backup_live=0
remove_ip_faults
faults_live=0
clean_reform
trap - EXIT
scenario_end
