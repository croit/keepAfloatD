#!/usr/bin/env bash
set -euo pipefail

# Driver for the 5-node / 4-VIP minimal-movement rebalance suite. It reuses lib.sh unchanged by
# exporting the topology overrides (and the 5-node compose file + a distinct project name) BEFORE
# sourcing it, so the default 3-node suite in run.sh is unaffected. Scenarios live in scenarios5/
# so run.sh's "all" glob (scenarios/[0-9][0-9]_*.sh) never picks them up against the 3-node stack.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

export COMPOSE_FILE="${ROOT_DIR}/tests/e2e/docker-compose.5node.yml"
export COMPOSE_PROJECT_NAME="keepafloatd-e2e-5node"
export E2E_NODES="node-a node-b node-c node-d node-e"
export E2E_VIPS="10.50.0.100 10.50.0.101 10.50.0.102 10.50.0.103"

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly SCENARIO_DIR="${ROOT_DIR}/tests/e2e/scenarios5"

# lib.sh's wait_for_steady_state is property-based (even_over_nodes over ${NODES} + single leader),
# so it works unchanged for this 5-node / 4-VIP topology: it converges to four nodes holding one
# VIP each and one idle node. The scenario then reads the actual placement and asserts the exact
# minimal-movement narrative relative to it.

down_cluster() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}

run_scenario() {
  local scenario_path="${1:?scenario path required}"
  local scenario_name
  scenario_name="$(basename "${scenario_path}" .sh)"

  log "starting ${scenario_name}"
  local status=0

  local artifact_dir="${ARTIFACT_DIR}/${scenario_name}"
  local run_log="${artifact_dir}/scenario.out"
  mkdir -p "${artifact_dir}"

  if {
    reset_cluster &&
      wait_for_steady_state &&
      bash "${scenario_path}"
  } 2>&1 | tee "${run_log}"; then
    status=0
  else
    status="${PIPESTATUS[0]}"
  fi

  capture_cluster_artifacts "${scenario_name}" || true
  down_cluster

  if (( status != 0 )); then
    fail "scenario ${scenario_name} failed"
    return "${status}"
  fi

  log "scenario ${scenario_name} passed"
}

cmd="${1:-12_minimal_movement_rebalance}"

trap 'down_cluster' EXIT
if [[ -f "${SCENARIO_DIR}/${cmd}.sh" ]]; then
  run_scenario "${SCENARIO_DIR}/${cmd}.sh"
elif [[ -f "${cmd}" ]]; then
  run_scenario "${cmd}"
else
  printf 'usage: %s [scenario-name]\n' "${0##*/}" >&2
  printf 'available scenarios in %s:\n' "${SCENARIO_DIR}" >&2
  ls -1 "${SCENARIO_DIR}" >&2 || true
  exit 2
fi
