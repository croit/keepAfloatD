#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"
# shellcheck source=tests/e2e/scripts/report.sh
. "${ROOT_DIR}/tests/e2e/scripts/report.sh"

readonly SCENARIO_DIR="${ROOT_DIR}/tests/e2e/scenarios"

down_cluster() {
  compose down -v --remove-orphans
}

run_scenario() {
  local scenario_path="${1:?scenario path required}"
  local scenario_name
  scenario_name="$(basename "${scenario_path}" .sh)"

  log "starting ${scenario_name}"
  local status=0

  # Tee the whole run (reset + steady-state wait + scenario, including any
  # dump_cluster_diagnostics output) into the scenario's artifact dir, so the
  # failure reason is persisted alongside the per-node logs and the report can
  # excerpt it. PIPESTATUS[0] keeps the run's exit code, not tee's.
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
  down_cluster || true

  if (( status != 0 )); then
    fail "scenario ${scenario_name} failed"
    return "${status}"
  fi

  log "scenario ${scenario_name} passed"
}

run_all_scenarios() {
  local -a passed=() failed=()
  local scenario name
  for scenario in "${SCENARIO_DIR}"/[0-9][0-9]_*.sh; do
    name="$(basename "${scenario}" .sh)"
    if run_scenario "${scenario}"; then
      passed+=("${name}")
    else
      failed+=("${name}")
    fi
  done

  # Always emit the report (best-effort); dynamic scoping hands PASSED/FAILED to
  # generate_report without exporting them.
  local PASSED="${passed[*]}" FAILED="${failed[*]}"
  generate_report || log "report generation failed"

  if (( ${#failed[@]} > 0 )); then
    fail "e2e scenarios failed: ${failed[*]}"
    return 1
  fi
}

usage() {
  cat <<EOF
Usage: ${0##*/} [all|up|wait-ready|run|down|SCENARIO]

Commands:
  all         reset the stack, run all scenarios, and tear everything down
  up          start the compose stack in the background
  wait-ready  wait for the steady-state 3-node cluster
  run         run all scenarios against fresh stacks
  down        tear the compose stack down with volumes
  SCENARIO    run one scenario by basename, e.g. 02_kill_holder

'all' and 'run' execute every scenario (continuing past failures) and write a
summary to e2e-artifacts/report.md; per-scenario logs land under
e2e-artifacts/compose/<scenario>/.
EOF
}

cmd="${1:-all}"

case "${cmd}" in
  all|run)
    trap 'down_cluster >/dev/null 2>&1 || true' EXIT
    run_all_scenarios
    ;;
  up)
    reset_cluster
    ;;
  wait-ready)
    wait_for_steady_state
    ;;
  down)
    down_cluster
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    if [[ -f "${SCENARIO_DIR}/${cmd}.sh" ]]; then
      trap 'down_cluster >/dev/null 2>&1 || true' EXIT
      run_scenario "${SCENARIO_DIR}/${cmd}.sh"
    else
      usage >&2
      exit 1
    fi
    ;;
esac
