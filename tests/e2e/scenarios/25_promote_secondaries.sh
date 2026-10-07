#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly aux_name="${COMPOSE_PROJECT_NAME}-secondary-warning"
readonly evidence="${ARTIFACT_DIR}/25_promote_secondaries"
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

aux() { docker exec "${aux_name}" "$@"; }

start_fixture() {
  docker run -d --name "${aux_name}" --init --network none --user 0 \
    --cap-add NET_ADMIN --cap-add NET_RAW \
    --sysctl "net.ipv4.conf.all.promote_secondaries=$1" \
    --sysctl "net.ipv4.conf.default.promote_secondaries=$2" \
    --entrypoint /bin/sleep \
    "${KEEPAFLOATD_IMAGE:?set the exact candidate image}" 30 >/dev/null
  created=true
  docker cp "${ROOT_DIR}/tests/e2e/configs/missing-interface.yaml" \
    "${aux_name}:/tmp/base.yaml"
  aux mkdir /tmp/evidence
  aux ip link set lo up
  aux ip link add vip-test0 type dummy
  aux ip link set vip-test0 up
}

daemon_ready() {
  local listeners
  listeners="$(aux ss -H -ltn 'sport = :27101')" || return 1
  [[ -n "${listeners}" ]]
}

run_case() {
  local name="$1" all="$2" interface="$3" mode="$4" expected="$5"
  start_fixture "${all}" "${interface}"
  aux /bin/sh -c '
    cp /tmp/base.yaml /tmp/config.yaml
    case "$1" in
      ipv6) sed -i "/address: 192.0.2.30\/24/,+1d" /tmp/config.yaml ;;
      dry-run) printf "\ndry_run: true\n" >>/tmp/config.yaml ;;
      missing-device) sed -i "s/interface: vip-test0/interface: missing0/" /tmp/config.yaml ;;
    esac
  ' sh "${mode}"
  docker exec -d -e RUST_LOG=keepafloatd=debug "${aux_name}" /bin/sh -c '
    /usr/local/bin/keepafloatd -c /tmp/config.yaml >"/tmp/evidence/$1.log" 2>&1 &
    pid=$!
    echo "$pid" >/tmp/daemon.pid
    wait "$pid"
    echo "$?" >"/tmp/evidence/$1.exit"
  ' sh "${name}"
  wait_until 5 daemon_ready || {
    aux cat "/tmp/evidence/${name}.log"
    fail "${name}: daemon did not start"; return 1;
  }
  aux /bin/sh -c 'kill -TERM "$(cat /tmp/daemon.pid)"'
  wait_until 5 aux test -f "/tmp/evidence/${name}.exit"
  [[ "$(aux cat "/tmp/evidence/${name}.exit")" == 0 ]]
  [[ "$(aux cat /proc/sys/net/ipv4/conf/all/promote_secondaries)" == "${all}" ]]
  [[ "$(aux cat /proc/sys/net/ipv4/conf/vip-test0/promote_secondaries)" == "${interface}" ]]
  local output
  output="$(aux cat "/tmp/evidence/${name}.log")"
  if [[ "${expected}" == warn ]]; then
    [[ "${output}" == *'IPv4 secondary address promotion is disabled'* ]] || {
      fail "${name}: disabled promotion was not reported"; return 1;
    }
    [[ "${output}" == *'iface=vip-test0'* ]]
    aux awk '
      /IPv4 secondary address promotion is disabled/ { warning++; seen=1 }
      /startup_cleanup:/ { if (!seen) exit 1 }
      END { if (warning != 1) exit 1 }
    ' "/tmp/evidence/${name}.log"
  elif [[ "${expected}" == unknown ]]; then
    [[ "${output}" == *'could not verify IPv4 secondary address promotion'* &&
       "${output}" == *'iface=missing0'* &&
       "${output}" != *'promotion is disabled'* ]] || {
      fail "${name}: unreadable setting was not distinguished from disabled"; return 1;
    }
  else
    [[ "${output}" != *'IPv4 secondary address promotion'* ]] || {
      fail "${name}: unexpected promotion warning"; return 1;
    }
  fi
  docker cp "${aux_name}:/tmp/evidence/." "${evidence}/"
  docker rm -f "${aux_name}" >/dev/null
  created=false
  log "${name}: startup and cleanup succeeded without changing sysctls"
}

run_case disabled 0 0 ipv4 warn
run_case interface-enabled 0 1 ipv4 quiet
run_case global-enabled 1 0 ipv4 quiet
run_case missing-device 0 0 missing-device unknown
run_case ipv6-only 0 0 ipv6 quiet
run_case dry-run 0 0 dry-run quiet
