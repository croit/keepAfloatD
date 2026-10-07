#!/usr/bin/env bash
# Kernel regressions stay inside a disposable container, including after daemon exit.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly aux_name="${COMPOSE_PROJECT_NAME}-missing-interface"
readonly evidence="${ARTIFACT_DIR}/18_missing_interface"
readonly config="${ROOT_DIR}/tests/e2e/configs/missing-interface.yaml"
mkdir -p "${evidence}"
created=false
cleanup() {
  local status=$?
  trap - EXIT
  fixture_watchdog_stop
  if ${created}; then
    timeout -k 2s 15s docker cp "${aux_name}:/tmp/evidence/." "${evidence}/" || status=1
    timeout -k 2s 15s docker rm -f "${aux_name}" >/dev/null || status=1
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker run -d --name "${aux_name}" --init --network none --user 0 \
  --cap-add NET_ADMIN --cap-add NET_RAW \
  --sysctl net.ipv6.conf.all.disable_ipv6=0 --entrypoint /bin/sleep \
  "${KEEPAFLOATD_IMAGE:?set KEEPAFLOATD_IMAGE to the exact candidate image}" infinity >/dev/null
created=true
fixture_watchdog_start "${aux_name}" 180
docker cp "${config}" "${aux_name}:/tmp/config.yaml"
aux() { docker exec "${aux_name}" "$@"; }
aux mkdir /tmp/evidence
aux ip link set lo up
# Subnet siblings must not be mistaken for the absent VIPs.
aux ip -4 addr add 192.0.2.31/24 dev lo
aux ip -6 addr add 2001:db8::31/64 dev lo nodad
aux ip -4 route add table 10246 throw 192.0.2.30/32 proto 246
aux ip -6 route add table 10246 throw 2001:db8::30/128 proto 246

start_daemon() {
  local case_name="${1:?case name required}"
  docker exec -d -e RUST_LOG=keepafloatd=debug "${aux_name}" /bin/sh -c '
    /usr/local/bin/keepafloatd -c /tmp/config.yaml >"/tmp/evidence/$1.log" 2>&1 &
    pid=$!
    echo "$pid" >/tmp/daemon.pid
    wait "$pid"
    echo "$?" >"/tmp/evidence/$1.exit"
  ' sh "${case_name}"
}

daemon_ready() {
  local listeners
  listeners="$(aux ss -H -ltn 'sport = :27101')" || return 1
  [[ -n "${listeners}" ]]
}

markers_absent() {
  local family routes
  for family in -4 -6; do
    routes="$(aux ip "${family}" route show table all)" || return 1
    [[ "${routes}" != *'table 10246'* ]] || return 1
  done
}

address_present() {
  local family="${1}" address="${2}" output
  output="$(aux ip "${family}" -o addr show to "${address}")" || return 1
  [[ "${output}" == *" ${address}/"* ]]
}

signal_daemon() {
  aux /bin/sh -c 'kill -"$1" "$(cat /tmp/daemon.pid)"' sh "${1}"
}

assert_exit() {
  local case_name="${1}" expected="${2}" actual
  wait_until 10 aux test -f "/tmp/evidence/${case_name}.exit" || {
    fail "${case_name}: daemon did not finish"; return 1;
  }
  actual="$(aux cat "/tmp/evidence/${case_name}.exit")"
  [[ "${actual}" == "${expected}" ]] || {
    aux cat "/tmp/evidence/${case_name}.log"
    fail "${case_name}: expected exit ${expected}, got ${actual}"; return 1;
  }
}

start_daemon startup
wait_until 10 daemon_ready || {
  aux cat /tmp/evidence/startup.log
  fail "startup aborted because the configured interface is missing"; exit 1;
}
markers_absent || { fail "startup retained orphan ownership markers"; exit 1; }
signal_daemon TERM
assert_exit startup 0
log "missing-device startup cleared orphan markers despite subnet siblings"

for change in delete rename; do
  aux ip link add vip-test0 type dummy
  aux ip link set vip-test0 up
  aux touch /tmp/healthy
  start_daemon "${change}"
  wait_until 5 aux grep -q 'runtime admission timing' "/tmp/evidence/${change}.log"
  startup_budget="$(aux cat "/tmp/evidence/${change}.log" | startup_budget_from_log 15)"
  fixture_watchdog_add_startup <<<"$(aux cat "/tmp/evidence/${change}.log")"
  wait_until "${startup_budget}" address_present -4 192.0.2.30
  wait_until 15 address_present -6 2001:db8::30
  # Freeze reconciliation so the mutation is tested by shutdown's tracked-VIP cleanup.
  signal_daemon STOP
  if [[ "${change}" == delete ]]; then
    aux ip link delete vip-test0
  else
    aux ip link set vip-test0 name renamed0
  fi
  signal_daemon TERM
  signal_daemon CONT
  if [[ "${change}" == delete ]]; then
    assert_exit "${change}" 0
    markers_absent || { fail "unbind retained missing-device markers"; exit 1; }
    log "deleted-device shutdown proved IPv4/IPv6 absence and cleared markers"
  else
    assert_exit "${change}" 1
    address_present -4 192.0.2.30
    address_present -6 2001:db8::30
    for family in -4 -6; do
      routes="$(aux ip "${family}" route show table all)"
      [[ "${routes}" == *'table 10246'* ]] || {
        fail "rename lost ownership evidence for ${family}"; exit 1;
      }
    done
    aux grep -q 'address is still present' /tmp/evidence/rename.log
    rename_log="$(aux cat /tmp/evidence/rename.log)"
    [[ "${rename_log}" != *'shutdown unhealthy report committed'* &&
       "${rename_log}" != *'shutdown VIP release committed'* ]] || {
      fail "failed cleanup published a shutdown handoff"; exit 1;
    }
    aux ip link delete renamed0
    log "renamed-device shutdown rejected release and retained both VIP markers"
  fi
done
# Recover the retained markers on the next start without recreating the old device.
aux rm /tmp/healthy
start_daemon recovery
wait_until 10 daemon_ready
markers_absent
signal_daemon TERM
assert_exit recovery 0
address_present -4 192.0.2.31
address_present -6 2001:db8::31
log "restart cleared retained markers and preserved unrelated addresses"
