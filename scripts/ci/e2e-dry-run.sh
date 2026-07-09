#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BINARY="${BINARY:-${ROOT_DIR}/target/release/keepafloatd}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${ROOT_DIR}/e2e-artifacts}"
HEALTH_SCRIPT="${ROOT_DIR}/scripts/ci/health-gate.sh"

readonly VIP1="10.0.0.101"
readonly VIP2="10.0.0.102"
readonly CLUSTER_SECRET="ci-dry-run-secret"
readonly HEALTH_INTERVAL_MS=300
readonly HEALTH_TIMEOUT_MS=150
readonly HEALTH_STALE_SECS=2
readonly HEARTBEAT_MS=100
readonly ELECTION_MIN_MS=300
readonly ELECTION_MAX_MS=600
readonly SUBMIT_TIMEOUT_MS=500
readonly MAX_FRAME_BYTES=4194304
readonly WAIT_STEP=0.2

if [[ ! -x "${BINARY}" ]]; then
  echo "binary not found or not executable: ${BINARY}" >&2
  exit 1
fi

if [[ ! -x "${HEALTH_SCRIPT}" ]]; then
  echo "health script not found or not executable: ${HEALTH_SCRIPT}" >&2
  exit 1
fi

mkdir -p "${ARTIFACT_DIR}"

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/keepafloatd-e2e.XXXXXX")"
PIDS=()
CURRENT_PHASE=""
CURRENT_PHASE_DIR=""
CURRENT_CONFIG_DIR=""
CURRENT_LOG_DIR=""
CURRENT_STATE_DIR=""

cleanup() {
  local status=$?
  stop_cluster || true
  rm -rf "${WORKDIR}"
  exit "${status}"
}
trap cleanup EXIT

stop_cluster() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
    fi
  done
  for pid in "${PIDS[@]:-}"; do
    wait "${pid}" 2>/dev/null || true
  done
  PIDS=()
}

start_phase() {
  CURRENT_PHASE="${1:?phase name required}"
  CURRENT_PHASE_DIR="${ARTIFACT_DIR}/${CURRENT_PHASE}"
  CURRENT_CONFIG_DIR="${CURRENT_PHASE_DIR}/configs"
  CURRENT_LOG_DIR="${CURRENT_PHASE_DIR}/logs"
  CURRENT_STATE_DIR="${WORKDIR}/${CURRENT_PHASE}/state"

  mkdir -p "${CURRENT_CONFIG_DIR}" "${CURRENT_LOG_DIR}" "${CURRENT_STATE_DIR}"
  rm -f "${CURRENT_LOG_DIR}"/*.log
}

node_log() {
  local node_id="${1:?node id required}"
  echo "${CURRENT_LOG_DIR}/node${node_id}.log"
}

node_state_file() {
  local node_id="${1:?node id required}"
  echo "${CURRENT_STATE_DIR}/node${node_id}.ok"
}

write_node_config() {
  local node_id="${1:?node id required}"
  local bootstrap="${2:?bootstrap required}"
  local raft_port="${3:?raft port required}"
  local submit_port="${4:?submit port required}"
  local state_file
  state_file="$(node_state_file "${node_id}")"

  cat >"${CURRENT_CONFIG_DIR}/node${node_id}.yaml" <<EOF
node_id: ${node_id}
bootstrap: ${bootstrap}
raft_listen: "127.0.0.1:${raft_port}"
client_submit_listen: "127.0.0.1:${submit_port}"

peers:
  - id: 1
    raft_address: "127.0.0.1:17100"
    client_submit_address: "127.0.0.1:17101"
  - id: 2
    raft_address: "127.0.0.1:17110"
    client_submit_address: "127.0.0.1:17111"
  - id: 3
    raft_address: "127.0.0.1:17120"
    client_submit_address: "127.0.0.1:17121"

vips:
  - address: "${VIP1}"
    interface: lo
  - address: "${VIP2}"
    interface: lo

health:
  command: ["${HEALTH_SCRIPT}", "${state_file}"]
  interval_ms: ${HEALTH_INTERVAL_MS}
  timeout_ms: ${HEALTH_TIMEOUT_MS}
  stale_secs: ${HEALTH_STALE_SECS}

raft:
  election_timeout_min_ms: ${ELECTION_MIN_MS}
  election_timeout_max_ms: ${ELECTION_MAX_MS}
  heartbeat_interval_ms: ${HEARTBEAT_MS}

cluster_secret: "${CLUSTER_SECRET}"
max_frame_bytes: ${MAX_FRAME_BYTES}
submit_timeout_ms: ${SUBMIT_TIMEOUT_MS}
dry_run: true
EOF
}

start_cluster() {
  local node_id
  touch "$(node_state_file 1)" "$(node_state_file 2)" "$(node_state_file 3)"

  write_node_config 1 true 17100 17101
  write_node_config 2 false 17110 17111
  write_node_config 3 false 17120 17121

  for node_id in 1 2 3; do
    "${BINARY}" -c "${CURRENT_CONFIG_DIR}/node${node_id}.yaml" \
      >"$(node_log "${node_id}")" 2>&1 &
    PIDS+=("$!")
  done
}

assert_node_alive() {
  local node_id="${1:?node id required}"
  local idx=$((node_id - 1))
  local pid="${PIDS[idx]:-}"
  if [[ -z "${pid}" ]]; then
    return 0
  fi
  if ! kill -0 "${pid}" 2>/dev/null; then
    echo "node${node_id} is not running in phase ${CURRENT_PHASE}" >&2
    tail -n 200 "$(node_log "${node_id}")" >&2 || true
    exit 1
  fi
}

set_node_health() {
  local node_id="${1:?node id required}"
  local healthy="${2:?healthy flag required}"
  local state_file
  state_file="$(node_state_file "${node_id}")"
  if [[ "${healthy}" == "1" ]]; then
    touch "${state_file}"
  else
    rm -f "${state_file}"
  fi
}

node_has_vip_bound() {
  local node_id="${1:?node id required}"
  local vip="${2:?vip required}"
  local log_file
  log_file="$(node_log "${node_id}")"
  [[ -f "${log_file}" ]] || { echo 0; return; }

  awk -v vip="${vip}" '
    index($0, "dry-run: would bind " vip " on ") { bound = 1 }
    index($0, "dry-run: would unbind " vip " on ") { bound = 0 }
    END { print bound ? 1 : 0 }
  ' "${log_file}"
}

current_holder_for_vip() {
  local vip="${1:?vip required}"
  local holders=()
  local node_id
  for node_id in 1 2 3; do
    if [[ "$(node_has_vip_bound "${node_id}" "${vip}")" == "1" ]]; then
      holders+=("${node_id}")
    fi
  done

  case "${#holders[@]}" in
    0) echo "none" ;;
    1) echo "${holders[0]}" ;;
    *)
      echo "duplicate:${holders[*]}"
      ;;
  esac
}

current_cluster_state() {
  echo "${VIP1}=$(current_holder_for_vip "${VIP1}") ${VIP2}=$(current_holder_for_vip "${VIP2}")"
}

wait_for_state() {
  local expected_vip1="${1:?expected holder for ${VIP1} required}"
  local expected_vip2="${2:?expected holder for ${VIP2} required}"
  local timeout_secs="${3:?timeout required}"
  local max_iters=$((timeout_secs * 5))
  local iter=0

  while true; do
    local node_id
    for node_id in 1 2 3; do
      if [[ -n "${PIDS[*]:-}" ]] && [[ "${node_id}" -le "${#PIDS[@]}" ]]; then
        assert_node_alive "${node_id}"
      fi
    done

    local holder1 holder2
    holder1="$(current_holder_for_vip "${VIP1}")"
    holder2="$(current_holder_for_vip "${VIP2}")"

    if [[ "${holder1}" == duplicate:* || "${holder2}" == duplicate:* ]]; then
      echo "duplicate dry-run bind detected: ${VIP1}=${holder1} ${VIP2}=${holder2}" >&2
      exit 1
    fi

    if [[ "${holder1}" == "${expected_vip1}" && "${holder2}" == "${expected_vip2}" ]]; then
      return 0
    fi

    iter=$((iter + 1))
    if (( iter <= max_iters )); then
      sleep "${WAIT_STEP}"
      continue
    fi

    echo "timed out waiting for state ${VIP1}=${expected_vip1} ${VIP2}=${expected_vip2}" >&2
    echo "current state: $(current_cluster_state)" >&2
    exit 1
  done
}

run_handoff_phase() {
  start_phase "handoff"
  start_cluster

  wait_for_state "1" "2" 30

  set_node_health 1 0
  wait_for_state "2" "3" 30

  kill -TERM "${PIDS[1]}"
  wait "${PIDS[1]}" 2>/dev/null || true
  PIDS[1]=""
  wait_for_state "3" "3" 30

  stop_cluster
}

run_handoff_phase

echo "dry-run e2e passed"
