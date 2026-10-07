#!/usr/bin/env bash
# Rename only a container's VIP interface; its peer address stays reachable.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

vip="${VIPS[0]}"
holder="$(holder_for_vip "${vip}")"
case "${holder}" in
  none|duplicate:*) fail "expected one initial holder"; exit 1 ;;
esac
survivors=()
for node in "${NODES[@]}"; do
  [[ "${node}" == "${holder}" ]] || survivors+=("${node}")
done
paused=false
cleanup() {
  local status=$?
  trap - EXIT
  if ${paused}; then signal_keepafloatd "${holder}" CONT || status=1; fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

address_commands_finished() {
  node_sh "${holder}" 'sh /opt/keepafloatd-tests/address-commands-finished.sh'
}

all_local_vips_absent() {
  local addresses candidate
  addresses="$(node_sh "${holder}" 'ip -o -4 addr show')" || return 1
  for candidate in "${VIPS[@]}"; do
    [[ "${addresses}" != *" ${candidate}/"* ]] || return 1
  done
}

bind_fault_logged() {
  local logs
  logs="$(compose logs --no-color "${holder}")" || return 1
  [[ "${logs}" == *'VIP bind failed; node remains unhealthy until restart'* &&
    "${logs}" == *'bind fault committed as unhealthy'* ]]
}

signal_keepafloatd "${holder}" STOP
paused=true
wait_until 5 address_commands_finished || {
  node_sh "${holder}" 'cat /proc/[0-9]*/status' >&2 || true
  fail "address commands did not finish before interface fault injection"
  exit 1
}
node_sh "${holder}" 'ip link set eth0 down && ip link set eth0 name vip-broken0 && ip link set vip-broken0 up'
# Remove only the test VIP, retaining the original peer IP and live TCP sockets.
node_sh "${holder}" "ip -4 addr del ${vip}/32 dev vip-broken0"
signal_keepafloatd "${holder}" CONT
paused=false
wait_until 10 bind_fault_logged || { fail "bind failure was not reported as a local fault"; exit 1; }
wait_for_even_over_nodes 20 "${survivors[@]}"
all_local_vips_absent || { fail "faulted holder retained a VIP on its renamed interface"; exit 1; }
service_is_running "${holder}" || { fail "bind failure stopped the daemon"; exit 1; }
node_sh "${holder}" "/bin/sh /opt/keepafloatd-tests/health.sh /shared/${holder}.unhealthy"
log "successful service health did not prevent bind-fault failover"

node_sh "${holder}" 'ip link set vip-broken0 down && ip link set vip-broken0 name eth0 && ip link set eth0 up'
all_local_vips_absent
signal_keepafloatd "${holder}" TERM
wait_for_service_exit "${holder}" 10
assert_service_exit_code "${holder}" 0
start_service "${holder}"
wait_for_startup_state
log "repaired and restarted holder rejoined normal VIP placement"
