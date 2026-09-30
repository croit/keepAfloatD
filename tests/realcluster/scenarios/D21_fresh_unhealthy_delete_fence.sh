#!/usr/bin/env bash
# D21: A live holder keeps publishing fresh unhealthy probes while its targeted `ip addr del`
# blocks or fails. The replacement must remain fenced until kernel absence is verified. The final
# phase removes the address but still returns a non-zero delete status, proving the exact-host probe
# can safely acknowledge an already-absent address.
SCENARIO_NAME=D21_fresh_unhealthy_delete_fence
source "$(dirname "$0")/../scenario.sh"
scenario_start "fresh-unhealthy delete failure fences replacement until verified absence"

tag=d21
target_vip="${VIPS[0]}"
backup_cluster_configs "${tag}"
clear_health_sentinels
configure_sentinel_health 500 100 3
set_cluster_scalar failover_delay_secs 0
set_cluster_scalar failback true
set_cluster_scalar failback_delay_secs 0

install_ip_fault() {
  local ip="${1:?}" inst
  inst="$(instance_for_ip "${ip}")"
  node_sh "${ip}" "
    mkdir -p /run/kafd-ipfault /run/systemd/system/keepafloatd@${inst}.service.d
    printf '%s\n' \
      '#!/bin/sh' \
      'if [ \"\${1-}\" = -4 ] && [ \"\${2-}\" = addr ] && [ \"\${3-}\" = del ] && [ \"\${4-}\" = ${target_vip}/32 ]; then' \
      '  if [ \"\$(cat /run/kafd-ipfault/mode 2>/dev/null)\" = timeout ]; then' \
      '    sleep 5' \
      '    exit 2' \
      '  fi' \
      '  if [ \"\$(cat /run/kafd-ipfault/mode 2>/dev/null)\" = absent ]; then' \
      '    /usr/sbin/ip \"\$@\" >/dev/null 2>&1 || true' \
      '    exit 2' \
      '  fi' \
      '  if [ \"\$(cat /run/kafd-ipfault/mode 2>/dev/null)\" = present ]; then exit 2; fi' \
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
  local ip inst
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    node_sh "${ip}" "
      rm -f /run/systemd/system/keepafloatd@${inst}.service.d/zz-kafd-ipfault.conf
      rmdir /run/systemd/system/keepafloatd@${inst}.service.d 2>/dev/null || true
      rm -rf /run/kafd-ipfault
      systemctl daemon-reload
    "
  done
}

for ip in "${NODE_IPS[@]}"; do install_ip_fault "${ip}"; done
clean_reform
check "fault-injected cluster reaches an even baseline" wait_for_even 45 "${NODE_IPS[@]}"

snapshot_vips
victim="$(holder_for_vip "${target_vip}")"
check "target begins with one concrete holder" test "${victim}" != none
node_sh "${victim}" "printf '%s\n' timeout > /run/kafd-ipfault/mode"
failure_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
evid "making live holder ${victim} freshly unhealthy while ${target_vip} delete first blocks"
sentinel_fail "${victim}"

timed_out_delete_logged() {
  delete_error_logged 'child command exceeded 250ms'
}
failed_delete_logged() {
  delete_error_logged 'ip addr del failed (exit status: 2) and the address is still present'
}
delete_error_logged() {
  # Fencing may use either cleanup path. The per-VIP diagnostic has no interface
  # field; unbind_all includes it. Consume the full journal so pipefail cannot see SIGPIPE.
  kafd_log_since "${victim}" "${failure_since}" \
    | awk -v vip="${target_vip}" -v iface="${IFACE}" -v expected="${1:?}" '
      {
        line = " " $0
        per_vip = " unbind " vip ": "
        cleanup = " unbind_all: " vip "/32 on " iface ": "
        pos = index(line, per_vip)
        if (pos && substr(line, pos + length(per_vip)) == expected) found = 1
        pos = index(line, cleanup)
        if (pos) {
          error = substr(line, pos + length(cleanup))
          if (error == expected || index(error, expected "; ") == 1) found = 1
        }
      }
      END { exit !found }
    '
}
target_stays_only_on_victim() {
  snapshot_vips || return 1
  [[ "$(holder_for_vip "${target_vip}")" == "${victim}" ]] && node_active "${victim}"
}

check "holder kills and reports the blocked kernel delete at its command deadline" \
  wait_until 20 timed_out_delete_logged
check "timed-out delete leaves the fresh unhealthy holder as the only kernel holder" \
  holds_for 5 target_stays_only_on_victim

evid "switching the same delete from timeout to an immediate non-zero status"
node_sh "${victim}" "printf '%s\n' present > /run/kafd-ipfault/mode"
check "holder observes the injected non-zero kernel delete" wait_until 20 failed_delete_logged
check "fresh unhealthy holder remains the only kernel holder" \
  holds_for 8 target_stays_only_on_victim
check "no other VIP becomes duplicated during the fenced handoff" no_vip_is_duplicate

evid "switching the same delete to remove the address but return status 2"
node_sh "${victim}" "printf '%s\n' absent > /run/kafd-ipfault/mode"
replacement_ready() {
  snapshot_vips || return 1
  local holder
  holder="$(holder_for_vip "${target_vip}")"
  [[ "${holder}" != none && "${holder}" != "${victim}" && "${holder}" != duplicate:* ]]
}
verified_absent_logged() {
  kafd_log_since "${victim}" "${failure_since}" \
    | grep -F "unbound ${target_vip}/32 on ${IFACE}" >/dev/null
}
check "verified absence opens the release fence and moves the VIP" wait_until 30 replacement_ready
check "non-zero delete with exact-host absence is recorded as unbound" \
  wait_until 20 verified_absent_logged
check "post-release ownership remains unique" holds_for 5 no_vip_is_duplicate

clear_health_sentinels
restore_cluster_configs "${tag}"
remove_ip_faults
clean_reform
scenario_end
