#!/usr/bin/env bash
# Journal evidence is scoped to the service that experienced the fault.
journal_evidence() {  # <mode> <node> <boot invocation start-us> [targets...]
  local mode="${1:?}" ip="${2:?}" context="${3-}" boot invocation started extra inst records
  shift 3
  read -r boot invocation started extra <<< "${context}" || return 1
  [[ "${boot}" =~ ^[a-f0-9]{32}$ && "${invocation}" =~ ^[a-f0-9]{32}$ &&
    "${started}" =~ ^[0-9]+$ && -z "${extra}" ]] || return 1
  inst="$(instance_for_ip "${ip}")" || return 1
  records="$(node_sh "${ip}" "journalctl -q --no-pager --all -o json \
    --output-fields=MESSAGE,_BOOT_ID,_SYSTEMD_INVOCATION_ID,__MONOTONIC_TIMESTAMP \
    -u $(shell_quote "keepafloatd@${inst}") _BOOT_ID=${boot} \
    _SYSTEMD_INVOCATION_ID=${invocation}")" || return 1
  python3 "${HERE}/journal-evidence.py" "${mode}" "${boot}" "${invocation}" \
    "${started}" "$@" <<< "${records}"
}

restarted_service_context() {
  local ip="${1:?}" original="${2:?}" current old_boot old_inv old_time boot inv now
  [[ "${original}" =~ ^[a-f0-9]{32}\ [a-f0-9]{32}\ [0-9]+$ ]] || return 1
  read -r old_boot old_inv old_time <<< "${original}"
  current="$(timed_node_command "${ip}" :)" || return 1
  read -r boot inv now <<< "${current}"
  [[ "${boot}" == "${old_boot}" && "${inv}" != "${old_inv}" &&
    "${now}" -ge "${old_time}" ]] || return 1
  printf '%s %s %s\n' "${boot}" "${inv}" "${old_time}"
}

startup_orphan_reclaimed() {
  local context
  context="$(restarted_service_context "${1:?}" "${2:?}")" || return 1
  journal_evidence startup "$1" "${context}" "${IFACE}" "${3:?}"
}

incarnation_reset_observed() {
  restarted_service_context "${1:?}" "${2:?}" >/dev/null || return 1
  journal_evidence incarnation "$1" "$2"
}

config_fence_observed() {
  local ip="${1:?}" context="${2:?}"
  shift 2
  restarted_service_context "${ip}" "${context}" >/dev/null || return 1
  journal_evidence config "${ip}" "${context}" "${IFACE}" "$@"
}

node_snapshot_cycles() {
  local ip="${1:?}" original="${2:?}" current count
  current="$(timed_node_command "${ip}" :)" || return 1
  [[ "${current% *}" == "${original% *}" ]] || return 1
  count="$(journal_evidence snapshots "${ip}" "${original}")" || return 1
  current="$(timed_node_command "${ip}" :)" || return 1
  [[ "${current% *}" == "${original% *}" ]] || return 1
  printf '%s\n' "${count}"
}
