#!/usr/bin/env bash
# Passive observation must prove NA-driven cache repair, not solicited replies.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly evidence="${ARTIFACT_DIR}/22_ipv6_announcement"
readonly vip="2001:db8:50::100"
mkdir -p "${evidence}"
compose down -v --remove-orphans
export KEEPAFLOATD_E2E_CONFIG_DIR=configs6
compose() {
  docker compose -f "${COMPOSE_FILE}" -f "${ROOT_DIR}/tests/e2e/ipv6.compose.yml" \
    -p "${COMPOSE_PROJECT_NAME}" "$@"
}
capture_pid=""
cleanup() {
  local status=$?
  trap - EXIT
  if [[ -n "${capture_pid}" ]]; then
    wait "${capture_pid}" || status=1
  fi
  capture_cluster_artifacts 22_ipv6_announcement/cluster || status=1
  compose down -v --remove-orphans || status=1
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
compose up --build -d

ipv6_holder() {
  local node addresses holder=""
  for node in "${NODES[@]}"; do
    addresses="$(node_sh "${node}" "ip -6 -o addr show to ${vip}")" || return 1
    if [[ -n "${addresses}" ]]; then
      [[ -z "${holder}" ]] || { fail "duplicate IPv6 VIP"; return 1; }
      [[ "${addresses}" != *tentative* && "${addresses}" != *dadfailed* ]] || return 1
      holder="${node}"
    fi
  done
  [[ -n "${holder}" ]] || return 1
  printf '%s\n' "${holder}"
}
initial_ready() {
  local node addresses count
  for node in "${NODES[@]}"; do
    addresses="$(node_sh "${node}" 'ip -6 -o addr show dev eth0')" || return 1
    count="$(printf '%s\n' "${addresses}" | grep -c ' inet6 2001:db8:50::10[0-2]/128 ')" || return 1
    [[ "${count}" == 1 ]] || return 1
  done
  old_holder="$(ipv6_holder)"
}
startup_budget="$(startup_budget_seconds 30 "${NODES[@]}")"
wait_until "${startup_budget}" initial_ready || { fail "IPv6 VIP did not become ready"; exit 1; }
old_mac="$(node_sh "${old_holder}" 'cat /sys/class/net/eth0/address')"
node_sh "${old_holder}" "ip -6 -o addr show to ${vip}" >"${evidence}/initial-address.txt"
grep -qw nodad "${evidence}/initial-address.txt" || { fail "IPv6 VIP lacks nodad"; exit 1; }
grep -q 'preferred_lft 0sec' "${evidence}/initial-address.txt" || {
  fail "IPv6 VIP lost source-address deprecation"; exit 1;
}

# Seed a dynamic cache entry without sending any packet to the VIP.
compose exec -T e2e-runner ip -6 neigh replace "${vip}" lladdr "${old_mac}" nud stale dev eth0
compose exec -T e2e-runner ip -6 neigh show to "${vip}" >"${evidence}/neighbor-before.txt"
grep -q "lladdr ${old_mac}" "${evidence}/neighbor-before.txt"
capture_ready() { grep -q 'listening on eth0' "${evidence}/capture.err"; }
new_holder_ready() {
  new_holder="$(ipv6_holder)" || return 1
  [[ "${new_holder}" != "${old_holder}" ]]
}

# Match this VIP's target, all-nodes destination, S=0, O=1 and a new sender.
compose exec -T e2e-runner timeout 30 tcpdump -nn -e -vv -l -i eth0 -c 1 \
  "icmp6 and dst host ff02::1 and ip6[40] = 136 and ip6[44] & 0x60 = 0x20 and
   ip6[48:4] = 0x20010db8 and ip6[52:4] = 0x00500000 and
   ip6[56:4] = 0 and ip6[60:4] = 0x100 and not ether src ${old_mac}" \
  >"${evidence}/packets.txt" 2>"${evidence}/capture.err" &
capture_pid=$!
wait_until 5 capture_ready || { fail "NA observer did not start"; exit 1; }
set_node_unhealthy "${old_holder}"
wait_until 20 new_holder_ready || { fail "IPv6 VIP did not move"; exit 1; }
new_mac="$(node_sh "${new_holder}" 'cat /sys/class/net/eth0/address')"
if wait "${capture_pid}"; then
  capture_pid=""
else
  capture_pid=""
  cat "${evidence}/packets.txt" "${evidence}/capture.err"
  fail "missing unsolicited NA for the moved IPv6 VIP"; exit 1
fi
grep -q "${new_mac}" "${evidence}/packets.txt"
cache_updated() {
  compose exec -T e2e-runner ip -6 neigh show to "${vip}" >"${evidence}/neighbor-after.txt" || return 1
  grep -q "lladdr ${new_mac}" "${evidence}/neighbor-after.txt"
}
wait_until 5 cache_updated || { fail "NA did not replace the old neighbor MAC"; exit 1; }
[[ "$(ipv6_holder)" == "${new_holder}" ]]
cat "${evidence}/packets.txt" "${evidence}/neighbor-before.txt" "${evidence}/neighbor-after.txt"
log "IPv6 nodad handoff and unsolicited neighbor-cache repair passed"
