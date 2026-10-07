#!/usr/bin/env bash
# Observe both unsolicited ARP requests from the new holder without probing the VIP.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly evidence="${ARTIFACT_DIR}/17_gratuitous_arp"
mkdir -p "${evidence}"
vip="${VIPS[0]}"
old_holder="$(holder_for_vip "${vip}")"
case "${old_holder}" in
  none|duplicate:*) fail "expected one initial holder for ${vip}"; exit 1 ;;
esac
old_mac="$(node_sh "${old_holder}" 'cat /sys/class/net/eth0/address')"
capture_pid=""

cleanup() {
  local status=$?
  trap - EXIT
  if [[ -n "${capture_pid}" ]]; then
    wait "${capture_pid}" || status=1
  fi
  exit "${status}"
}
trap cleanup EXIT

capture_ready() {
  grep -q 'listening on eth0' "${evidence}/capture.err"
}

has_new_holder() {
  new_holder="$(holder_for_vip "${vip}")"
  [[ "${new_holder}" != "${old_holder}" && "${new_holder}" != none &&
    "${new_holder}" != duplicate:* ]]
}

# The timeout bounds both capture failure and EXIT cleanup. No active ARP query is allowed here.
compose exec -T e2e-runner timeout 30 tcpdump -nn -e -l -i eth0 -c 2 \
  "arp and arp[6:2] = 1 and arp src host ${vip} and arp dst host ${vip} and not ether src ${old_mac}" \
  >"${evidence}/packets.txt" 2>"${evidence}/capture.err" &
capture_pid=$!
wait_until 5 capture_ready || { fail "ARP observer did not start"; exit 1; }

set_node_unhealthy "${old_holder}"
wait_until 15 has_new_holder || { fail "VIP did not move to one new holder"; exit 1; }
new_mac="$(node_sh "${new_holder}" 'cat /sys/class/net/eth0/address')"
if wait "${capture_pid}"; then
  capture_pid=""
else
  capture_pid=""
  cat "${evidence}/packets.txt" "${evidence}/capture.err"
  fail "new holder did not send both gratuitous ARP packets"
  exit 1
fi

cat "${evidence}/packets.txt"
count="$(awk -v mac="${new_mac}" '$2 == mac { count++ } END { print count+0 }' \
  "${evidence}/packets.txt")"
[[ "${count}" == 2 ]] || { fail "expected two packets from ${new_mac}, got ${count}"; exit 1; }
assert_unique_holders
log "both gratuitous ARP packets observed for ${vip} from ${new_holder} (${new_mac})"
