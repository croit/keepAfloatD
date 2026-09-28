#!/usr/bin/env bash
set -euo pipefail

# Dedicated one-VIP failover-timing suite. It reuses the default three-node Compose topology with a
# separate config directory and project name, keeping timing/nopreempt settings isolated from the
# general scenarios.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
export COMPOSE_PROJECT_NAME="keepafloatd-e2e-issue22"
export ARTIFACT_DIR="${ROOT_DIR}/e2e-artifacts/compose22"
export E2E_VIPS="10.50.0.100"
export KEEPAFLOATD_E2E_CONFIG_DIR="configs22"

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly SCENARIO_DIR="${ROOT_DIR}/tests/e2e/scenarios22"

down_cluster() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}

run_scenario() {
  local scenario_path="${1:?scenario path required}"
  local scenario_name status=0
  scenario_name="$(basename "${scenario_path}" .sh)"
  local artifact_dir="${ARTIFACT_DIR}/${scenario_name}"
  local run_log="${artifact_dir}/scenario.out"
  mkdir -p "${artifact_dir}"

  log "starting ${scenario_name}"
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
  ((status == 0)) || {
    fail "scenario ${scenario_name} failed"
    return "${status}"
  }
  log "scenario ${scenario_name} passed"
}

cmd="${1:-all}"
trap 'down_cluster' EXIT
if [[ "${cmd}" == "all" ]]; then
  for scenario in "${SCENARIO_DIR}"/[0-9][0-9]_*.sh; do
    run_scenario "${scenario}"
  done
elif [[ -f "${SCENARIO_DIR}/${cmd}.sh" ]]; then
  run_scenario "${SCENARIO_DIR}/${cmd}.sh"
elif [[ -f "${cmd}" ]]; then
  run_scenario "${cmd}"
else
  printf 'usage: %s [all|scenario-name]\n' "${0##*/}" >&2
  exit 2
fi
