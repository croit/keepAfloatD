#!/usr/bin/env bash
# Run as root on a disposable test host with systemd and network namespaces.
set -euo pipefail

binary="$(realpath "${1:?candidate binary required}")"
unit_template="$(realpath "${2:?packaged systemd unit required}")"
evidence="$(realpath "${3:?existing evidence directory required}")"
[[ -x "${binary}" && -f "${unit_template}" && -d "${evidence}" ]]
[[ "${EUID}" == 0 ]] || { echo "root is required" >&2; exit 1; }
for tool in ip jq systemctl journalctl; do command -v "${tool}" >/dev/null; done
grep -Fxq 'ExecStopPost=/usr/bin/keepafloatd --config /etc/keepafloatd/config-%i.yaml --cleanup-only' \
  "${unit_template}" || { echo "packaged cleanup-only hook is required" >&2; exit 1; }

fixture="$(mktemp -d /var/lib/keepafloatd-signals.XXXXXX)"
name="${fixture##*/}"
unit="${name}.service"
unit_path="/run/systemd/system/${unit}"
namespace_created=false
unit_created=false

cleanup() {
  local result=$?
  trap - EXIT
  if ${unit_created}; then
    systemctl stop "${unit}" || result=1
    journalctl -u "${unit}" --no-pager >"${evidence}/journal.log" || result=1
    systemctl reset-failed "${unit}" 2>/dev/null || true
    rm -f "${unit_path}"
    systemctl daemon-reload || result=1
  fi
  if ${namespace_created}; then ip netns delete "${name}" || result=1; fi
  cp -a "${fixture}/." "${evidence}/"
  rm -f "${fixture}/keepafloatd" "${fixture}/config.yaml" \
    "${fixture}/assert-clean.sh" "${fixture}/assert-before-clean.sh" \
    "${fixture}/exits.log" "${fixture}/addresses.json" "${fixture}/markers.json"
  rmdir "${fixture}" || result=1
  exit "${result}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

ip netns add "${name}"
namespace_created=true
ip -n "${name}" link set lo up
ip -n "${name}" addr add 192.0.2.200/32 dev lo
ip -n "${name}" addr add 192.0.2.201/32 dev lo
ip -n "${name}" route add table 10245 throw 192.0.2.201/32 proto 245
install -m 0755 "${binary}" "${fixture}/keepafloatd"
cat >"${fixture}/config.yaml" <<'YAML'
node_id: 1
raft_listen: "127.0.0.1:17100"
client_submit_listen: "127.0.0.1:17101"
peers:
  - id: 1
    raft_address: "127.0.0.1:17100"
    client_submit_address: "127.0.0.1:17101"
vips:
  - address: "192.0.2.100"
    interface: lo
health:
  command: ["/bin/sh", "-c", "exit 0"]
  interval_ms: 100
  timeout_ms: 200
  stale_secs: 6
raft:
  heartbeat_interval_ms: 100
  election_timeout_min_ms: 300
  election_timeout_max_ms: 600
cluster_secret: "systemd-signal-regression-fixture-012345"
dry_run: false
YAML
cat >"${fixture}/assert-clean.sh" <<'SH'
#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
ip -j addr show dev lo > addresses.json
jq -e '[.[].addr_info[] | select(.local == "192.0.2.100")] | length == 0' \
  addresses.json >/dev/null
jq -e '[.[].addr_info[] | select(.local == "192.0.2.200" or .local == "192.0.2.201")] | length == 2' \
  addresses.json >/dev/null
ip -N -j -4 route show table all > markers.json
jq -e '[.[] | select((.type | tostring) == "9" or .type == "throw") |
  select((.protocol | tostring) == "246" and (.table | tostring) == "10246")] | length == 0' \
  markers.json >/dev/null
jq -e '[.[] | select((.type | tostring) == "9" or .type == "throw") |
  select(.dst == "192.0.2.201" and (.protocol | tostring) == "245" and
    (.table | tostring) == "10245")] | length == 1' \
  markers.json >/dev/null
printf '%s %s %s\n' "${SERVICE_RESULT}" "${EXIT_CODE}" "${EXIT_STATUS}" >>exits.log
SH
chmod 0755 "${fixture}/assert-clean.sh"
cat >"${fixture}/assert-before-clean.sh" <<'SH'
#!/bin/bash
set -euo pipefail
expected=0
printf 'Before cleanup: %s %s %s\n' "${SERVICE_RESULT}" "${EXIT_CODE}" "${EXIT_STATUS}" >&2
if [[ "${EXIT_CODE}" == killed && "${EXIT_STATUS}" == KILL ]]; then expected=1; fi
ip -j addr show dev lo |
  jq -e --argjson expected "${expected}" \
    '[.[].addr_info[] | select(.local == "192.0.2.100")] | length == $expected' >/dev/null
SH
chmod 0755 "${fixture}/assert-before-clean.sh"
sed \
  -e "s|^ExecStart=.*|ExecStart=${fixture}/keepafloatd --config ${fixture}/config.yaml|" \
  -e "s|^ExecStopPost=/usr/bin/keepafloatd --config /etc/keepafloatd/config-%i.yaml --cleanup-only$|ExecStopPost=${fixture}/assert-before-clean.sh\nExecStopPost=${fixture}/keepafloatd --config ${fixture}/config.yaml --cleanup-only|" \
  -e '/^EnvironmentFile=/d' \
  "${unit_template}" >"${unit_path}"
cat >>"${unit_path}" <<UNIT

[Unit]
# Allow six rapid restarts; bounded waits and cleanup prevent runaway retries.
StartLimitIntervalSec=0

[Service]
NetworkNamespacePath=/run/netns/${name}
ExecStopPost=${fixture}/assert-clean.sh
UNIT
unit_created=true
systemctl daemon-reload
systemctl start "${unit}"

wait_for() {
  local deadline=$((SECONDS + 10))
  until "$@"; do
    (( SECONDS < deadline )) || { echo "timeout: $*" >&2; return 1; }
    sleep 0.1
  done
}

vip_present() {
  ip -n "${name}" -j addr show dev lo |
    jq -e '[.[].addr_info[] | select(.local == "192.0.2.100")] | length == 1' >/dev/null
}

restarted() {
  local previous_pid="${1}" expected_restarts="${2}" current_pid
  current_pid="$(systemctl show "${unit}" -p MainPID --value)"
  [[ "${current_pid}" != 0 && "${current_pid}" != "${previous_pid}" ]] &&
    [[ "$(systemctl show "${unit}" -p NRestarts --value)" == "${expected_restarts}" ]] &&
    vip_present
}

wait_for vip_present
attempt=0
for signal in HUP HUP QUIT QUIT KILL KILL; do
  attempt=$((attempt + 1))
  old_pid="$(systemctl show "${unit}" -p MainPID --value)"
  systemctl kill --kill-whom=main --signal="${signal}" "${unit}"
  wait_for restarted "${old_pid}" "${attempt}"
  expected='exit-code exited 1'
  if [[ "${signal}" == KILL ]]; then expected='signal killed KILL'; fi
  [[ "$(wc -l <"${fixture}/exits.log")" -eq "${attempt}" ]]
  [[ "$(tail -1 "${fixture}/exits.log")" == "${expected}" ]]
  echo "SIG${signal} ${attempt}: kernel VIP absent before restart; new process bound it"
done

systemctl stop "${unit}"
[[ "$(systemctl show "${unit}" -p ActiveState --value)" == inactive ]]
[[ "$(tail -1 "${fixture}/exits.log")" == 'success exited 0' ]]
echo "SIGTERM: clean stop with no remaining VIP"
