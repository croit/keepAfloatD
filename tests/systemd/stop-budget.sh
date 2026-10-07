#!/usr/bin/env bash
# Run only on an authorized disposable systemd guest; never on a production host.
set -euo pipefail

[[ "${KEEPAFLOATD_DISPOSABLE_SYSTEMD_TEST:-}" == 1 ]] || {
  echo 'Set KEEPAFLOATD_DISPOSABLE_SYSTEMD_TEST=1 only inside a disposable guest.' >&2
  exit 2
}
[[ "${EUID}" == 0 ]] || { echo 'root is required' >&2; exit 2; }
binary="$(realpath "${1:?candidate binary required}")"
unit_template="$(realpath "${2:?packaged systemd unit required}")"
evidence="$(realpath "${3:?existing empty evidence directory required}")"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
[[ -x "${binary}" && -f "${unit_template}" && -d "${evidence}" && ! -e "${evidence}/fixture" ]]
for tool in ip jq systemctl journalctl timeout; do command -v "${tool}" >/dev/null; done
real_ip="$(command -v ip)"
grep -Fxq 'NotifyAccess=exec' "${unit_template}"
grep -Fxq 'TimeoutStopSec=15' "${unit_template}"
grep -Fxq 'ExecStopPost=/usr/bin/keepafloatd --config /etc/keepafloatd/config-%i.yaml --cleanup-only' "${unit_template}"

fixture="$(mktemp -d /var/lib/keepafloatd-stop.XXXXXX)"
name="${fixture##*/}"
unit="${name}.service"
unit_path="/run/systemd/system/${unit}"
namespace_created=false
unit_created=false

snapshot() {
  local label=$1
  ip -n "${name}" -j addr show dev lo >"${evidence}/${label}-addresses.json" || return 1
  ip -n "${name}" -N -j -4 route show table all >"${evidence}/${label}-markers4.json" || return 1
  ip -n "${name}" -N -j -6 route show table all >"${evidence}/${label}-markers6.json" || return 1
}

owned_count() {
  ip -n "${name}" -j addr show dev lo | jq '[.[].addr_info[] |
    select((.local | startswith("192.0.2.") or startswith("2001:db8::")) and
      .local != "192.0.2.250" and .local != "2001:db8::250")] | length'
}

assert_clean() {
  local label=$1
  snapshot "${label}" || return 1
  [[ "$(owned_count)" == 0 ]] || return 1
  jq -e '[.[].addr_info[] | select(.local == "192.0.2.250" or .local == "2001:db8::250")] |
    length == 2' "${evidence}/${label}-addresses.json" >/dev/null || return 1
  for family in 4 6; do
    jq -e '[.[] | select((.protocol | tostring) == "246" and (.table | tostring) == "10246")] |
      length == 0' "${evidence}/${label}-markers${family}.json" >/dev/null || return 1
    jq -e '[.[] | select((.protocol | tostring) == "245" and (.table | tostring) == "10245")] |
      length == 1' "${evidence}/${label}-markers${family}.json" >/dev/null || return 1
  done
}

cleanup() {
  local result=$?
  trap - EXIT
  if ${unit_created}; then
    timeout 90 systemctl stop "${unit}" || echo 'Stop returned failure during harness cleanup.' >&2
    journalctl -u "${unit}" --no-pager >"${evidence}/journal.log" || result=1
    systemctl show "${unit}" >"${evidence}/final-unit-state.txt" || result=1
    cp "${unit_path}" "${evidence}/tested.service"
    rm -f -- "${unit_path}"
    systemctl daemon-reload || result=1
    systemctl reset-failed "${unit}" 2>/dev/null || echo 'No failed unit state to reset.' >&2
  fi
  if ${namespace_created}; then
    if assert_clean before-teardown; then
      ip netns delete "${name}" || result=1
    else
      echo "Namespace ${name} retained: kernel cleanup was not verified." >&2
      result=1
    fi
  fi
  mv "${fixture}" "${evidence}/fixture"
  exit "${result}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

ip netns add "${name}"
namespace_created=true
ip -n "${name}" link set lo up
ip -n "${name}" addr add 192.0.2.250/32 dev lo
ip -n "${name}" -6 addr add 2001:db8::250/128 dev lo nodad
ip -n "${name}" route add table 10245 throw 192.0.2.250/32 proto 245
ip -n "${name}" -6 route add table 10245 throw 2001:db8::250/128 proto 245
install -m 0755 "${binary}" "${fixture}/keepafloatd"
mkdir "${fixture}/bin"
install -m 0755 "${script_dir}/fixtures/stop-budget-ip.sh" "${fixture}/bin/ip"
cat >"${fixture}/config.yaml" <<'YAML'
node_id: 1
raft_listen: "127.0.0.1:17100"
client_submit_listen: "127.0.0.1:17101"
peers:
  - id: 1
    raft_address: "127.0.0.1:17100"
    client_submit_address: "127.0.0.1:17101"
health:
  command: ["/bin/sh", "-c", "exit 0"]
  interval_ms: 100
  timeout_ms: 200
  stale_secs: 60
raft:
  heartbeat_interval_ms: 100
  election_timeout_min_ms: 300
  election_timeout_max_ms: 600
cluster_secret: "systemd-stop-budget-fixture-0123456789"
vips:
YAML
for suffix in $(seq 1 48); do
  printf '  - address: "192.0.2.%s/32"\n    interface: lo\n' "${suffix}" >>"${fixture}/config.yaml"
  printf '  - address: "2001:db8::%s/128"\n    interface: lo\n' "${suffix}" >>"${fixture}/config.yaml"
done
chmod 0600 "${fixture}/config.yaml"
sed \
  -e "s|^ExecStart=.*|ExecStart=${fixture}/keepafloatd --config ${fixture}/config.yaml|" \
  -e "s|^ExecStopPost=.*|ExecStopPost=${fixture}/keepafloatd --config ${fixture}/config.yaml --cleanup-only|" \
  -e 's/^Restart=.*/Restart=no/' \
  -e '/^EnvironmentFile=/d' \
  "${unit_template}" >"${unit_path}"
cat >>"${unit_path}" <<UNIT

[Service]
NetworkNamespacePath=/run/netns/${name}
Environment=PATH=${fixture}/bin:/usr/sbin:/usr/bin:/sbin:/bin
Environment=KEEPAFLOATD_REAL_IP=${real_ip}
Environment=KEEPAFLOATD_STOP_FIXTURE=${fixture}
Environment=RUST_LOG=info,keepafloatd::stop_budget=debug
UNIT
unit_created=true
systemctl daemon-reload

wait_for() {
  local limit=$1
  shift
  local deadline=$((SECONDS + limit))
  until "$@"; do
    (( SECONDS < deadline )) || { echo "timeout: $*" >&2; return 1; }
    sleep 0.1
  done
}

all_bound() { [[ "$(owned_count)" == 96 ]]; }
stopped() {
  local state
  state="$(systemctl show "${unit}" -p ActiveState --value)"
  [[ "${state}" == inactive || "${state}" == failed ]]
}
start() {
  local state
  state="$(systemctl show "${unit}" -p ActiveState --value)" || return 1
  if [[ "${state}" == failed ]]; then
    systemctl reset-failed "${unit}" || return 1
  fi
  timeout 20 systemctl start "${unit}" || return 1
  wait_for 30 all_bound
}
record() {
  local label=$1 elapsed=$2
  systemctl show "${unit}" >"${evidence}/${label}-unit-state.txt"
  journalctl -u "${unit}" --no-pager >"${evidence}/${label}-journal.log"
  printf '%s elapsed=%ss result=%s\n' "${label}" "${elapsed}" \
    "$(systemctl show "${unit}" -p Result --value)" | tee -a "${evidence}/results.txt"
}

assert_progress_span() {
  local label=$1 pid=$2
  cp "${fixture}/delete-progress" "${evidence}/${label}-deletes.txt"
  awk -v pid="${pid}" '
    $1 == pid { if (count++ == 0) first=$2; last=$2 }
    END {
      printf "pid=%s deletes=%d measured_progress_span=%.2fs\n", pid, count, last-first
      exit !(count > 150 && last-first > 15)
    }' "${evidence}/${label}-deletes.txt" | tee -a "${evidence}/results.txt"
}

start
daemon_pid="$(systemctl show "${unit}" -p MainPID --value)"
touch "${fixture}/delay"
started=${SECONDS}
timeout 90 systemctl stop "${unit}"
elapsed=$((SECONDS - started))
record graceful-progress "${elapsed}"
(( elapsed > 15 ))
[[ "$(systemctl show "${unit}" -p Result --value)" == success ]]
assert_progress_span graceful-progress "${daemon_pid}"
assert_clean graceful-progress
rm -- "${fixture}/delay" "${fixture}/delete-progress"

start
for suffix in $(seq 101 116); do
  ip -n "${name}" addr add "192.0.2.${suffix}/32" dev lo
  ip -n "${name}" -6 addr add "2001:db8::${suffix}/128" dev lo nodad
  ip -n "${name}" route add table 10246 throw "192.0.2.${suffix}/32" proto 246
  ip -n "${name}" -6 route add table 10246 throw "2001:db8::${suffix}/128" proto 246
done
snapshot before-crash
[[ "$(owned_count)" == 128 ]]
daemon_pid="$(systemctl show "${unit}" -p MainPID --value)"
touch "${fixture}/delay"
started=${SECONDS}
systemctl kill --kill-whom=main --signal=KILL "${unit}"
wait_for 90 stopped
elapsed=$((SECONDS - started))
record post-stop-progress "${elapsed}"
(( elapsed > 15 ))
[[ "$(systemctl show "${unit}" -p Result --value)" == signal ]]
helper_pid="$(awk 'NR == 1 { print $1 }' "${fixture}/delete-progress")"
[[ "${helper_pid}" != "${daemon_pid}" ]]
assert_progress_span post-stop-progress "${helper_pid}"
grep -q 'cleanup-only VIP cleanup complete' "${evidence}/post-stop-progress-journal.log"
assert_clean post-stop-progress
rm -- "${fixture}/delay" "${fixture}/delete-progress"

start
daemon_pid="$(systemctl show "${unit}" -p MainPID --value)"
touch "${fixture}/stall-once"
started=${SECONDS}
systemctl stop --no-block "${unit}"
wait_for 75 stopped
elapsed=$((SECONDS - started))
record graceful-stall "${elapsed}"
[[ "$(<"${fixture}/stalled-pid")" == "${daemon_pid}" ]]
[[ "$(systemctl show "${unit}" -p Result --value)" == timeout ]]
(( elapsed >= 15 && elapsed < 75 ))
assert_clean graceful-stall-recovered-by-post-stop
rm -- "${fixture}/stalled-pid" "${fixture}/stall-used"

start
touch "${fixture}/stall-once"
started=${SECONDS}
systemctl kill --kill-whom=main --signal=KILL "${unit}"
wait_for 10 test -s "${fixture}/stalled-pid"
[[ "$(<"${fixture}/stalled-pid")" == "$(systemctl show "${unit}" -p ControlPID --value)" ]]
wait_for 75 stopped
elapsed=$((SECONDS - started))
record post-stop-stall "${elapsed}"
(( elapsed >= 15 && elapsed < 75 ))
grep -Eq 'stop-post.*timed out|[Ss]top-post.*timeout' "${evidence}/post-stop-stall-journal.log"
snapshot post-stop-stall-residue
(( $(owned_count) > 0 ))
[[ "$(systemctl show "${unit}" -p MainPID --value)" == 0 ]]
[[ "$(systemctl show "${unit}" -p ControlPID --value)" == 0 ]]
timeout 30 ip netns exec "${name}" env -u NOTIFY_SOCKET \
  "PATH=${fixture}/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
  "KEEPAFLOATD_REAL_IP=${real_ip}" "KEEPAFLOATD_STOP_FIXTURE=${fixture}" \
  "${fixture}/keepafloatd" --config "${fixture}/config.yaml" --cleanup-only \
  >"${evidence}/manual-recovery.log" 2>&1
assert_clean post-stop-stall-manual-recovery

for suffix in $(seq 101 148); do
  ip -n "${name}" addr add "192.0.2.${suffix}/32" dev lo
  ip -n "${name}" -6 addr add "2001:db8::${suffix}/128" dev lo nodad
  ip -n "${name}" route add table 10246 throw "192.0.2.${suffix}/32" proto 246
  ip -n "${name}" -6 route add table 10246 throw "2001:db8::${suffix}/128" proto 246
done
snapshot startup-stop-before-start
[[ "$(owned_count)" == 96 ]]
[[ ! -e "${fixture}/delete-progress" ]]
touch "${fixture}/delay"
systemctl reset-failed "${unit}"
timeout 20 systemctl start "${unit}"
daemon_pid="$(systemctl show "${unit}" -p MainPID --value)"
wait_for 10 test -s "${fixture}/delete-progress"
[[ "$(awk 'NR == 1 { print $1 }' "${fixture}/delete-progress")" == "${daemon_pid}" ]]
started=${SECONDS}
timeout 90 systemctl stop "${unit}"
elapsed=$((SECONDS - started))
record startup-stop "${elapsed}"
(( elapsed > 15 ))
[[ "$(systemctl show "${unit}" -p Result --value)" == success ]]
assert_progress_span startup-stop "${daemon_pid}"
assert_clean startup-stop
echo 'Progress and finite no-progress cases passed; kernel absence verified before teardown.'
