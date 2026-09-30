#!/usr/bin/env bash

RUNALL_GUARD_ACTIVE=0
RUNALL_GUARD_COMPLETE=0
RUNALL_SCENARIO_PGID=""
declare -Ag RUNALL_GUARD_CAPTURED=()
declare -Ag RUNALL_CONFIG_HASH=()
declare -Ag RUNALL_BINARY_HASH=()

verify_restored_guard_hashes() {
  local ip inst cfg hashes actual_cfg actual_binary extra
  [[ "${RUNALL_GUARD_COMPLETE}" -eq 1 ]] || return 1
  for ip in "${NODE_IPS[@]}"; do
    [[ "${RUNALL_GUARD_CAPTURED[${ip}]:-}" == 1 ]] || return 1
    inst="$(instance_for_ip "${ip}")"
    cfg="/etc/keepafloatd/config-${inst}.yaml"
    hashes="$(node_sh "${ip}" \
      "set -e; sha256sum ${cfg} /usr/bin/keepafloatd | awk '{print \$1}' | xargs")" \
      || return 1
    read -r actual_cfg actual_binary extra <<< "${hashes}" || return 1
    [[ -z "${extra}" ]] || return 1
    [[ "${actual_cfg}" == "${RUNALL_CONFIG_HASH[${ip}]}" ]] || return 1
    [[ "${actual_binary}" == "${RUNALL_BINARY_HASH[${ip}]}" ]] || return 1
  done
}

capture_scenario_guard() {
  local ip inst cfg hashes cfg_hash binary_hash guard_cfg_hash guard_binary_hash
  RUNALL_GUARD_ACTIVE=1
  RUNALL_GUARD_COMPLETE=0
  RUNALL_GUARD_CAPTURED=()
  RUNALL_CONFIG_HASH=()
  RUNALL_BINARY_HASH=()

  # These are campaign-created artifacts. Requiring an absent baseline prevents cleanup from
  # deleting operator-owned files and prevents a prior scenario leak from becoming the new guard.
  campaign_config_backups_absent || return 1
  campaign_runtime_artifacts_absent || return 1
  ownership_test_artifacts_absent || return 1
  partition_rules_absent || return 1
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    cfg="/etc/keepafloatd/config-${inst}.yaml"
    RUNALL_GUARD_CAPTURED["${ip}"]=pending
    hashes="$(node_sh "${ip}" "
      set -euo pipefail
      test ! -e ${cfg}.runall-guard
      test ! -e /usr/bin/keepafloatd.runall-guard
      test ! -e ${cfg}.runall-guard.new
      test ! -e /usr/bin/keepafloatd.runall-guard.new
      test ! -e /run/kafd-notify.log
      test ! -e /usr/local/bin/kafd-notify.sh
      test ! -e /tmp/kafd-notify.log
      cfg_hash=\$(sha256sum ${cfg} | awk '{print \$1}')
      binary_hash=\$(sha256sum /usr/bin/keepafloatd | awk '{print \$1}')
      cp -p ${cfg} ${cfg}.runall-guard.new
      cp -p /usr/bin/keepafloatd /usr/bin/keepafloatd.runall-guard.new
      mv ${cfg}.runall-guard.new ${cfg}.runall-guard
      mv /usr/bin/keepafloatd.runall-guard.new /usr/bin/keepafloatd.runall-guard
      guard_cfg_hash=\$(sha256sum ${cfg}.runall-guard | awk '{print \$1}')
      guard_binary_hash=\$(sha256sum /usr/bin/keepafloatd.runall-guard | awk '{print \$1}')
      printf '%s %s %s %s\\n' \"\$cfg_hash\" \"\$binary_hash\" \
        \"\$guard_cfg_hash\" \"\$guard_binary_hash\"
    ")" || return 1
    read -r cfg_hash binary_hash guard_cfg_hash guard_binary_hash <<< "${hashes}"
    [[ "${cfg_hash}" =~ ^[a-f0-9]{64}$ && "${binary_hash}" =~ ^[a-f0-9]{64}$ ]] || return 1
    [[ "${guard_cfg_hash}" == "${cfg_hash}" ]] || return 1
    [[ "${guard_binary_hash}" == "${binary_hash}" ]] || return 1
    RUNALL_CONFIG_HASH["${ip}"]="${cfg_hash}"
    RUNALL_BINARY_HASH["${ip}"]="${binary_hash}"
    RUNALL_GUARD_CAPTURED["${ip}"]=1
  done
  RUNALL_GUARD_COMPLETE=1
}

restore_scenario_guard() {
  local ip inst cfg tag backup_cleanup failed=0
  local -A stopped=()
  [[ "${RUNALL_GUARD_COMPLETE}" -eq 1 ]] || return 1
  for ip in "${NODE_IPS[@]}"; do
    [[ "${RUNALL_GUARD_CAPTURED[${ip}]:-}" == 1 ]] || return 1
    [[ "${RUNALL_CONFIG_HASH[${ip}]}" =~ ^[a-f0-9]{64}$ ]] || return 1
    [[ "${RUNALL_BINARY_HASH[${ip}]}" =~ ^[a-f0-9]{64}$ ]] || return 1
  done

  for ip in "${NODE_IPS[@]}"; do
    if kafd_stop "${ip}"; then
      stopped["${ip}"]=1
    else
      printf 'campaign guard stop failed on %s\n' "${ip}" >&2
      failed=1
    fi
  done
  for ip in "${NODE_IPS[@]}"; do
    [[ "${stopped[${ip}]:-}" == 1 ]] || continue
    if ! inst="$(instance_for_ip "${ip}")"; then
      printf 'campaign guard instance lookup failed on %s\n' "${ip}" >&2
      failed=1
      continue
    fi
    cfg="/etc/keepafloatd/config-${inst}.yaml"
    backup_cleanup="rm -f"
    for tag in "${REALCLUSTER_CAMPAIGN_BACKUP_TAGS[@]}"; do
      backup_cleanup+=" ${cfg}.${tag}-bak"
    done
    if ! node_sh "${ip}" "
      set -euo pipefail
      restore_file() {
        local file=\"\$1\" expected=\"\$2\"
        if test -f \"\${file}.runall-guard\"; then
          test \"\$(sha256sum \"\${file}.runall-guard\" | awk '{print \$1}')\" = \"\$expected\"
          mv -- \"\${file}.runall-guard\" \"\$file\"
        fi
        # A lost SSH response may follow a successful move; only exact content proves recovery.
        test \"\$(sha256sum \"\$file\" | awk '{print \$1}')\" = \"\$expected\"
      }
      restore_file ${cfg} '${RUNALL_CONFIG_HASH[${ip}]}'
      restore_file /usr/bin/keepafloatd '${RUNALL_BINARY_HASH[${ip}]}'
      rm -f ${cfg}.runall-guard.new /usr/bin/keepafloatd.runall-guard.new
      rm -f /usr/bin/keepafloatd.d15-current /usr/bin/keepafloatd.d16-current
      test ! -f /run/kafd-submit-blocker.pid || kill \$(cat /run/kafd-submit-blocker.pid) 2>/dev/null || true
      rm -f /run/kafd-submit-blocker.pid /run/kafd-submit-blocker.log
      rm -f /run/systemd/system/keepafloatd@${inst}.service.d/zz-kafd-ipfault.conf
      rmdir /run/systemd/system/keepafloatd@${inst}.service.d 2>/dev/null || true
      rm -rf /run/kafd-ipfault
      rm -f /run/keepafloatd-unhealthy
      rm -f /run/kafd-notify.log /usr/local/bin/kafd-notify.sh /tmp/kafd-notify.log
      ${backup_cleanup}
      systemctl daemon-reload
    "; then
      printf 'campaign guard restore failed on %s\n' "${ip}" >&2
      failed=1
    fi
  done
  (( failed == 0 )) || return 1
  cleanup_ownership_test_artifacts || return 1
  ownership_markers_absent || return 1
  verify_restored_guard_hashes || return 1
  restore_baseline || return 1
  verify_restored_guard_hashes || return 1
  ownership_markers_absent || return 1
  RUNALL_GUARD_ACTIVE=0
}

campaign_guard_on_exit() {
  local status=$?
  trap - EXIT INT TERM
  if [[ "${RUNALL_GUARD_ACTIVE}" -eq 1 ]]; then
    if ! restore_scenario_guard; then
      if declare -F log >/dev/null; then
        log "campaign exit restoration failed; inspect remaining guards and captured hashes"
      else
        printf '%s\n' "campaign exit restoration failed; inspect remaining guards and captured hashes" >&2
      fi
      status=3
    fi
  fi
  exit "${status}"
}

stop_active_scenario_group() {
  local pgid="${RUNALL_SCENARIO_PGID:-}" deadline attempt
  [[ -n "${pgid}" ]] || return 0
  [[ "${pgid}" =~ ^[1-9][0-9]*$ ]] || return 1

  # GNU timeout creates a process group unless --foreground is used. Signal that whole group so
  # an SSH or evidence child cannot outlive its scenario and race exact restoration.
  kill -TERM -- "-${pgid}" 2>/dev/null || kill -TERM "${pgid}" 2>/dev/null || true
  deadline=$((SECONDS + ${RUNALL_SCENARIO_STOP_GRACE:-30}))
  while kill -0 "${pgid}" 2>/dev/null || kill -0 -- "-${pgid}" 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      kill -KILL -- "-${pgid}" 2>/dev/null || true
      kill -KILL "${pgid}" 2>/dev/null || true
      break
    fi
    sleep 1
  done
  wait "${pgid}" 2>/dev/null || true
  # Reaping the leader does not wait for its descendants to consume SIGKILL and exit.
  for attempt in {1..20}; do
    if ! kill -0 -- "-${pgid}" 2>/dev/null; then
      RUNALL_SCENARIO_PGID=""
      return 0
    fi
    sleep 0.05
  done
  return 1
}

campaign_guard_on_signal() {
  local status="${1:?}"
  trap - INT TERM
  stop_active_scenario_group || status=3
  exit "${status}"
}

install_campaign_guard_traps() {
  trap campaign_guard_on_exit EXIT
  trap 'campaign_guard_on_signal 130' INT
  trap 'campaign_guard_on_signal 143' TERM
}
