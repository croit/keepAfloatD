#!/usr/bin/env bash
# B3: Notify hook fires MASTER on VIP gain, FAULT on local-health-driven release, and BACKUP
# on a cluster-driven release. Hooks are installed on every node so a rebalance release cannot
# escape observation merely because a different node happened to own the moved VIP.
SCENARIO_NAME=B3_notify_hook
source "$(dirname "$0")/../scenario.sh"
scenario_start "notify hook fires MASTER/FAULT/BACKUP at correct transitions"

# The packaged unit uses PrivateTmp=yes, so /tmp written by the hook is not the
# host /tmp observed by this harness. /run is writable and shared with the host.
notify_log="/run/kafd-notify.log"
notify_script="/usr/local/bin/kafd-notify.sh"
config_tag=b3-notify
backup_live=0

notify_event_count() {
  local ip="${1:?}" vip="${2:?}" state="${3:?}"
  node_sh "${ip}" "set -o pipefail; test -r ${notify_log}; awk -v vip='${vip}' -v state='${state}' '\$2 == vip && \$3 == state { count++ } END { print count + 0 }' ${notify_log}"
}

cluster_notify_event_count() {
  local vip="${1:?}" state="${2:?}" ip count total=0
  for ip in "${NODE_IPS[@]}"; do
    count="$(notify_event_count "${ip}" "${vip}" "${state}")" || return 1
    [[ "${count}" =~ ^[0-9]+$ ]] || return 1
    total=$((total + count))
  done
  printf '%d\n' "${total}"
}

unexpected_notify_event_count() {
  local ip="${1:?}" vip="${2:?}" expected="${3:?}"
  node_sh "${ip}" "set -euo pipefail; test -r ${notify_log}; awk -v vip='${vip}' -v expected='${expected}' '
    \$2 == vip && \$3 != expected { count++ }
    END { print count + 0 }
  ' ${notify_log}"
}

master_without_local_vip_count() {
  local ip="${1:?}" vip="${2:?}"
  node_sh "${ip}" "set -o pipefail; test -r ${notify_log}; awk -v vip='${vip}' '
    \$2 == vip && \$3 == \"MASTER\" && \$4 != \"held\" { count++ }
    END { print count + 0 }
  ' ${notify_log}"
}

cluster_master_without_local_vip_count() {
  local vip="${1:?}" ip count total=0
  for ip in "${NODE_IPS[@]}"; do
    count="$(master_without_local_vip_count "${ip}" "${vip}")" || return 1
    [[ "${count}" =~ ^[0-9]+$ ]] || return 1
    total=$((total + count))
  done
  printf '%d\n' "${total}"
}

cleanup_b3_state() {
  local failed=0 ip
  [[ "${backup_live}" -eq 1 ]] || return 0
  for ip in "${NODE_IPS[@]}"; do kafd_stop "${ip}" || failed=1; done
  restore_cluster_configs "${config_tag}" || failed=1
  for ip in "${NODE_IPS[@]}"; do
    node_sh "${ip}" "rm -f ${notify_log} /tmp/kafd-notify.log ${notify_script}" || failed=1
    set_healthy "${ip}" || failed=1
  done
  [[ "${failed}" -eq 0 ]] && clean_reform || failed=1
  [[ "${failed}" -eq 0 ]] && backup_live=0
  return "${failed}"
}

restore_b3_on_exit() {
  local status=$?
  trap - ERR EXIT INT TERM
  cleanup_b3_state || status=1
  exit "${status}"
}
trap restore_b3_on_exit EXIT

evid "installing notify script + config on all nodes"
backup_cluster_configs "${config_tag}" || {
  evid "config capture failed; recovery was not started"
  exit 1
}
backup_live=1
for ip in "${NODE_IPS[@]}"; do
  inst="$(instance_for_ip "${ip}")"
  cfg="/etc/keepafloatd/config-${inst}.yaml"
  node_sh "${ip}" "set -euo pipefail
test ! -e ${notify_log}
test ! -e /tmp/kafd-notify.log
test ! -e ${notify_script}
cat > ${notify_script} <<'EOS'
#!/bin/bash
held=na
if [[ \"\$3\" == MASTER ]]; then
  if ip -o addr show | awk -v vip=\"\$2\" 'index(\$4, vip \"/\") == 1 { found=1 } END { exit !found }'; then
    held=held
  else
    held=missing
  fi
fi
echo \"\$(date +%s) \$2 \$3 \$held\" >> ${notify_log}
EOS
chmod +x ${notify_script}
: > ${notify_log}
if grep -q '^notify:' ${cfg}; then
  grep -Fxq 'notify: \"${notify_script}\"' ${cfg}
else
  echo 'notify: \"${notify_script}\"' >> ${cfg}
fi
grep -Fxq 'notify: \"${notify_script}\"' ${cfg}"
done
# BACKUP requires a healthy node to release a VIP because of a consensus reassignment. The lab's
# baseline is nopreempt, so enable failback only for this scenario and restore the exact configs.
set_cluster_scalar failback true
clean_reform

for ip in "${NODE_IPS[@]}"; do
  check "node ${ip} active after notify config" wait_until 30 node_active "${ip}"
done
check "cluster steady with notify hooks" wait_for_even 60 "${NODE_IPS[@]}"

# A repeated event for one address cannot substitute for a missing transition on another address.
sleep 3
for vip in "${VIPS[@]}"; do
  master_lines="$(cluster_notify_event_count "${vip}" MASTER)"
  invalid_master_lines="$(cluster_master_without_local_vip_count "${vip}")"
  evid "MASTER notifications for ${vip}: ${master_lines}"
  check "MASTER fired for initial gain of ${vip}" test "${master_lines}" -ge 1
  check "no MASTER without local VIP ownership for ${vip}" \
    test "${invalid_master_lines}" -eq 0
done
for ip in "${NODE_IPS[@]}"; do node_sh "${ip}" ": > ${notify_log}"; done

# Local health failure must produce FAULT on the failing node.
fault_ip="${NODE_IPS[0]}"
fault_vips=()
snapshot_vips
for vip in "${VIPS[@]}"; do
  [[ "$(holder_for_vip "${vip}")" == "${fault_ip}" ]] && fault_vips+=("${vip}")
done
check "failing node owns at least one VIP before the health fault" test "${#fault_vips[@]}" -ge 1
evid "stopping the local RGW/front end on ${fault_ip} -> expect FAULT on its release"
set_unhealthy "${fault_ip}"
check "node releases VIPs after local health fail" wait_until 60 node_lacks_all_vips "${fault_ip}"
check "survivor ownership stabilizes before recording recovery incumbents" \
  wait_for_even 60 $(nodes_except "${fault_ip}")
sleep 3
for vip in "${fault_vips[@]}"; do
  fault_lines="$(notify_event_count "${fault_ip}" "${vip}" FAULT)"
  unexpected_lines="$(unexpected_notify_event_count "${fault_ip}" "${vip}" FAULT)"
  cluster_fault_lines="$(cluster_notify_event_count "${vip}" FAULT)"
  outside_fault_lines=$((cluster_fault_lines - fault_lines))
  evid "FAULT notifications on ${fault_ip} for ${vip}: ${fault_lines}"
  check "FAULT fired for local-health-driven release of ${vip}" test "${fault_lines}" -ge 1
  check "no unexpected notify state accompanied FAULT for ${vip}" \
    test "${unexpected_lines}" -eq 0
  check "no FAULT notifications outside ${fault_ip} for ${vip}" \
    test "${outside_fault_lines}" -eq 0
done

# Recovery makes an overloaded healthy survivor give a VIP back. Its local health remains good,
# so that cluster-driven release must be BACKUP rather than FAULT.
declare -A before_recovery_holder=()
snapshot_vips
for vip in "${VIPS[@]}"; do before_recovery_holder["${vip}"]="$(holder_for_vip "${vip}")"; done
for ip in "${NODE_IPS[@]}"; do node_sh "${ip}" ": > ${notify_log}"; done
set_healthy "${fault_ip}"
check "recovery drives a cluster rebalance" wait_for_even 90 "${NODE_IPS[@]}"
sleep 3
snapshot_vips
backup_transitions=0
for ip in "${NODE_IPS[@]}"; do
  evid "notify log on ${ip}:"
  node_sh "${ip}" "tail -12 ${notify_log}" | sed 's/^/    /' | tee -a "${_EVID}"
done
for vip in "${VIPS[@]}"; do
  previous="${before_recovery_holder[${vip}]}"
  current="$(holder_for_vip "${vip}")"
  cluster_master_lines="$(cluster_notify_event_count "${vip}" MASTER)"
  if [[ "${previous}" == "${current}" ]]; then
    check "unchanged VIP ${vip} emitted no recovery MASTER" \
      test "${cluster_master_lines}" -eq 0
    continue
  fi
  case "${previous}" in
    none | duplicate:* | "")
      check "previous holder evidence is valid for moved VIP ${vip}" false
      continue
      ;;
  esac
  backup_lines="$(notify_event_count "${previous}" "${vip}" BACKUP)"
  cluster_backup_lines="$(cluster_notify_event_count "${vip}" BACKUP)"
  outside_backup_lines=$((cluster_backup_lines - backup_lines))
  unexpected_lines="$(unexpected_notify_event_count "${previous}" "${vip}" BACKUP)"
  master_lines="$(notify_event_count "${current}" "${vip}" MASTER)"
  outside_master_lines=$((cluster_master_lines - master_lines))
  current_unexpected_lines="$(unexpected_notify_event_count "${current}" "${vip}" MASTER)"
  evid "BACKUP notifications on ${previous} for moved VIP ${vip}: ${backup_lines}"
  check "BACKUP fired for healthy cluster-driven release of ${vip}" \
    test "${backup_lines}" -ge 1
  check "no unexpected notify state accompanied BACKUP for ${vip}" \
    test "${unexpected_lines}" -eq 0
  check "no BACKUP notifications outside ${previous} for ${vip}" \
    test "${outside_backup_lines}" -eq 0
  check "new holder ${current} received MASTER for ${vip}" test "${master_lines}" -ge 1
  check "no MASTER notifications outside ${current} for ${vip}" \
    test "${outside_master_lines}" -eq 0
  check "new holder ${current} received no contradictory state for ${vip}" \
    test "${current_unexpected_lines}" -eq 0
  backup_transitions=$((backup_transitions + 1))
done
check "recovery caused at least one correlated cluster-driven release" \
  test "${backup_transitions}" -ge 1

# Restore the exact nopreempt configs on every node.
cleanup_b3_state
trap - EXIT
check "cluster safely available after restoring nopreempt configs" wait_for_available_cluster

scenario_end
