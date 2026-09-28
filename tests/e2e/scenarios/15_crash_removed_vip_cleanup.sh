#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly aux_name="${COMPOSE_PROJECT_NAME}-removed-vip-cleanup"
readonly aux_image="${KEEPAFLOATD_IMAGE:?KEEPAFLOATD_IMAGE must name the exact image under test}"
readonly before_config="${ROOT_DIR}/tests/e2e/configs30/before.yaml"
readonly after_config="${ROOT_DIR}/tests/e2e/configs30/after.yaml"
readonly removed_vip="192.0.2.30"
readonly admin_address="192.0.2.31"
readonly stable_vip="192.0.2.32"
readonly foreign_instance_vip="192.0.2.33"
readonly removed_vip6="2001:db8::30"
readonly stable_vip6="2001:db8::32"
readonly marker_table_base=10000
runner_cid="$(service_container_id e2e-runner)"

marker_table_for_protocol() {
  printf '%d\n' "$((marker_table_base + ${1:?protocol required}))"
}

cleanup_aux() {
  docker rm -f "${aux_name}" >/dev/null 2>&1 || true
}
trap cleanup_aux EXIT

docker run -d --name "${aux_name}" --user 0 --cap-add NET_ADMIN \
  --entrypoint /bin/sleep \
  -v "${before_config}:/tmp/before.yaml:ro" \
  -v "${after_config}:/tmp/after.yaml:ro" \
  "${aux_image}" 300 >/dev/null

docker exec "${aux_name}" ip addr add "${admin_address}/32" dev lo
docker exec "${aux_name}" ip -4 route replace table "$(marker_table_for_protocol 245)" \
  throw "${foreign_instance_vip}/32" proto 245
docker exec "${aux_name}" ip addr add "${foreign_instance_vip}/32" dev lo

start_aux_daemon() {
  local config="${1:?config required}"
  docker exec -d "${aux_name}" /bin/sh -c \
    "echo \$\$ >/tmp/keepafloatd.pid; exec /usr/local/bin/keepafloatd -c '${config}' >>/tmp/keepafloatd.log 2>&1"
}

aux_daemon_running() {
  docker exec "${aux_name}" /bin/sh -c \
    'test -r /tmp/keepafloatd.pid && test -d "/proc/$(cat /tmp/keepafloatd.pid)"'
}

aux_address_present() {
  local address="${1:?address required}"
  docker exec "${aux_name}" ip -o addr show dev lo to "${address}" | grep -q "${address}/"
}

aux_route_protocol() {
  local address="${1:?address required}" protocol="${2:?protocol required}" marker_table family host_prefix
  marker_table="$(marker_table_for_protocol "${protocol}")"
  if [[ "${address}" == *:* ]]; then
    family=-6
    host_prefix=128
  else
    family=-4
    host_prefix=32
  fi
  docker exec "${aux_name}" ip -N -j "${family}" route show table all |
    docker exec -i "${runner_cid}" jq -r --arg address "${address}" \
      --arg suffix "/${host_prefix}" --arg table "${marker_table}" \
      '[.[] | select(((.type | tostring) == "9" or .type == "throw") and
        (.table | tostring) == $table and
        (.dst == $address or .dst == ($address + $suffix))) | (.protocol // "")] |
        if length == 0 then ""
        elif length == 1 then .[0]
        else error("duplicate ownership markers")
        end'
}

address_has_marker() {
  local address="${1:?address required}" expected="${2:?protocol required}" expected_hex
  printf -v expected_hex '0x%02x' "${expected}"
  local observed
  aux_address_present "${address}" || return 1
  observed="$(aux_route_protocol "${address}" "${expected}")" || return 1
  [[ "${observed}" == "${expected}" || "${observed}" == "${expected_hex}" ]]
}

address_is_marked() {
  address_has_marker "${1:?address required}" 246
}

address_has_foreign_instance_marker() {
  address_has_marker "${1:?address required}" 245
}

address_marker_absent() {
  local address="${1:?address required}" protocol="${2:?protocol required}" observed
  observed="$(aux_route_protocol "${address}" "${protocol}")" || return 1
  [[ -z "${observed}" ]]
}

address_has_no_known_marker() {
  address_marker_absent "${1:?address required}" 246 &&
    address_marker_absent "${1:?address required}" 245
}

address_has_prefix() {
  local address="${1:?address required}" expected="${2:?prefix required}"
  docker exec "${aux_name}" ip -N -j addr show dev lo |
    docker exec -i "${runner_cid}" jq -e --arg address "${address}" --argjson prefix "${expected}" \
      '[.[].addr_info[] | select(.local == $address and .prefixlen == $prefix)] | length == 1' \
      >/dev/null
}

kill_aux_daemon() {
  local signal="${1:?signal required}"
  docker exec "${aux_name}" /bin/sh -c \
    "pid=\$(cat /tmp/keepafloatd.pid); kill -${signal} \"\${pid}\"; for _ in \$(seq 1 100); do test ! -d \"/proc/\${pid}\" && exit 0; sleep 0.05; done; exit 1"
}

start_aux_daemon /tmp/before.yaml
wait_until 10 aux_daemon_running
wait_until 15 address_is_marked "${removed_vip}"
wait_until 15 address_is_marked "${stable_vip}"
wait_until 15 address_is_marked "${removed_vip6}"
wait_until 15 address_is_marked "${stable_vip6}"
aux_address_present "${admin_address}"
address_has_foreign_instance_marker "${foreign_instance_vip}"
address_has_no_known_marker "${admin_address}" || {
  fail "unmarked administrator address unexpectedly carries a protocol marker"
  exit 1
}

kill_aux_daemon KILL
aux_address_present "${removed_vip}"
address_is_marked "${removed_vip}"
aux_address_present "${removed_vip6}"
address_is_marked "${removed_vip6}"

start_aux_daemon /tmp/after.yaml
wait_until 10 aux_daemon_running
wait_until 15 address_is_marked "${stable_vip}"
wait_until 15 address_has_prefix "${stable_vip}" 24
wait_until 15 address_is_marked "${stable_vip6}"
wait_until 15 address_has_prefix "${stable_vip6}" 64
if aux_address_present "${removed_vip}"; then
  docker exec "${aux_name}" cat /tmp/keepafloatd.log >&2 || true
  fail "marked VIP removed from config survived startup cleanup"
  exit 1
fi
address_marker_absent "${removed_vip}" 246 || {
  fail "removed VIP ownership marker survived startup cleanup"
  exit 1
}
if aux_address_present "${removed_vip6}"; then
  fail "marked IPv6 VIP removed from config survived startup cleanup"
  exit 1
fi
address_marker_absent "${removed_vip6}" 246 || {
  fail "removed IPv6 VIP ownership marker survived startup cleanup"
  exit 1
}
aux_address_present "${admin_address}"
address_has_foreign_instance_marker "${foreign_instance_vip}" || {
  fail "startup cleanup deleted another keepafloatd instance's marked address"
  exit 1
}
address_has_no_known_marker "${admin_address}" || {
  fail "startup cleanup altered the unmarked administrator address"
  exit 1
}

kill_aux_daemon TERM
if aux_address_present "${stable_vip}"; then
  fail "graceful shutdown left the stable marked VIP bound"
  exit 1
fi
address_marker_absent "${stable_vip}" 246 || {
  fail "graceful shutdown left the stable VIP ownership marker"
  exit 1
}
if aux_address_present "${stable_vip6}"; then
  fail "graceful shutdown left the stable marked IPv6 VIP bound"
  exit 1
fi
address_marker_absent "${stable_vip6}" 246 || {
  fail "graceful shutdown left the stable IPv6 VIP ownership marker"
  exit 1
}
aux_address_present "${admin_address}"
address_has_foreign_instance_marker "${foreign_instance_vip}"
