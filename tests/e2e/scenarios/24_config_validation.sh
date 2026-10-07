#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

export KEEPAFLOATD_E2E_CONFIG_DIR=configs-secret-file
reset_cluster
wait_for_startup_state
log "inline, relative-file and absolute-file secrets formed one cluster"
kill_service node-b KILL
wait_for_service_exit node-b 10
wait_for_even_over_nodes "$(cleanup_budget_seconds 30)" node-a node-c
compose start node-b
wait_for_startup_state
log "file-backed member rejoined with unique VIP ownership"

readonly aux_name="${COMPOSE_PROJECT_NAME}-config-preflight"
readonly evidence="${ARTIFACT_DIR}/24_config_validation/preflight"
mkdir -p "${evidence}"
created=false
cleanup() {
  local status=$?
  trap - EXIT
  if ${created}; then
    docker cp "${aux_name}:/tmp/evidence/." "${evidence}/" || status=1
    docker rm -f "${aux_name}" >/dev/null || status=1
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker run -d --name "${aux_name}" --init --network none --user 0 \
  --cap-add NET_ADMIN --entrypoint /bin/sleep \
  "${KEEPAFLOATD_IMAGE:?set the exact candidate image}" 120 >/dev/null
created=true
docker cp "${ROOT_DIR}/tests/e2e/configs/missing-interface.yaml" "${aux_name}:/tmp/base.yaml"
aux() { docker exec "${aux_name}" "$@"; }
aux mkdir /tmp/evidence
aux ip link add vip-test0 type dummy
aux ip link set vip-test0 up
aux ip addr add 192.0.2.30/24 dev vip-test0
aux ip route add table 10246 throw 192.0.2.30/32 proto 246
aux mkfifo /tmp/secret-pipe

for case_name in short-secret conflict invalid-interface fifo-secret; do
  aux sh -ec '
    case "$1" in
      short-secret) sed "s/^cluster_secret:.*/cluster_secret: short/" /tmp/base.yaml ;;
      conflict) sed "/^health:/i\  - address: 192.0.2.30/32\n    interface: vip-test0" /tmp/base.yaml ;;
      invalid-interface) sed "s/interface: vip-test0/interface: bad:name/" /tmp/base.yaml ;;
      fifo-secret) sed "s|^cluster_secret:.*|cluster_secret_file: /tmp/secret-pipe|" /tmp/base.yaml ;;
    esac >/tmp/config.yaml
    status=0
    timeout 3 /usr/local/bin/keepafloatd -c /tmp/config.yaml >"/tmp/evidence/$1.log" 2>&1 || status=$?
    test "$status" = 1
    grep -q "load config" "/tmp/evidence/$1.log"
  ' sh "${case_name}"
  case "${case_name}" in
    short-secret) expected='32 to 256' ;;
    conflict) expected='conflicting VIP' ;;
    invalid-interface) expected='Linux name' ;;
    fifo-secret) expected='regular file' ;;
  esac
  aux grep -q "${expected}" "/tmp/evidence/${case_name}.log"
  addresses="$(aux ip -4 -o addr show dev vip-test0)"
  [[ "${addresses}" == *' 192.0.2.30/24 '* ]] || { fail "${case_name}: startup changed VIP"; exit 1; }
  routes="$(aux ip -4 route show table 10246)"
  [[ "${routes}" == *'throw 192.0.2.30'* ]] || { fail "${case_name}: startup changed marker"; exit 1; }
  listeners="$(aux ss -H -ltn)"
  [[ -z "${listeners}" ]] || { fail "${case_name}: startup opened a listener"; exit 1; }
done
log "invalid configs left real addresses, ownership markers and listeners untouched"
