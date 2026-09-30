#!/usr/bin/env bash
# Sourced by every scenario script. Provides scenario lifecycle + pass/fail recording.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${HERE}/lib.sh"

prepare_cluster_secret || { fail "could not establish a common required cluster_secret"; exit 2; }
if [[ "${REALCLUSTER_SECRET_CHANGED}" -eq 1 ]]; then
  log "repaired missing/mismatched required cluster_secret before scenario"
  clean_reform || { fail "cluster did not restart after cluster_secret repair"; exit 2; }
fi

SCENARIO_NAME="${SCENARIO_NAME:-$(basename "${0%.sh}")}"
RESULTS_DIR="${RESULTS_DIR:-${HERE}/results}"
mkdir -p "${RESULTS_DIR}"
_EVID="${RESULTS_DIR}/${SCENARIO_NAME}.log"
: > "${_EVID}"

# Evidence capture (also echoed).
evid() { printf '%s\n' "$*" | tee -a "${_EVID}"; }

_PASS=1
check() {  # check "<description>" <command...>   - records pass/fail, never aborts
  local desc="${1:?}"; shift
  if "$@"; then evid "  ✓ ${desc}"; else evid "  ✗ ${desc}"; _PASS=0; fi
}
check_eq() {  # check_eq "<desc>" <actual> <expected>
  local desc="${1:?}" actual="${2-}" expected="${3-}"
  if [[ "${actual}" == "${expected}" ]]; then evid "  ✓ ${desc} (=${actual})"
  else evid "  ✗ ${desc} (got '${actual}', want '${expected}')"; _PASS=0; fi
}
check_contains() {  # check_contains "<desc>" "<haystack>" "<needle>"
  local desc="${1:?}" hay="${2-}" needle="${3:?}"
  if [[ "${hay}" == *"${needle}"* ]]; then evid "  ✓ ${desc}"
  else evid "  ✗ ${desc} (missing '${needle}')"; _PASS=0; fi
}

scenario_start() { evid "=== ${SCENARIO_NAME}: ${1:-} ==="; evid "start $(date -u +%H:%M:%SZ)  baseline: $(current_assignments_summary)"; }
scenario_end() {
  # Always restore to baseline so the next scenario starts clean.
  evid "restoring baseline..."
  restore_baseline >>"${_EVID}" 2>&1 || { evid "  ✗ baseline restore failed"; _PASS=0; }
  if [[ "${RGW_MUTATED}" -eq 1 ]]; then
    evid "observing post-RGW controller window before the next scenario..."
    settle_after_rgw_recovery >>"${_EVID}" 2>&1 \
      || { evid "  ✗ post-RGW controller state did not stabilize"; _PASS=0; }
  fi
  if [[ "${_PASS}" == "1" ]]; then evid "RESULT ${SCENARIO_NAME}: PASS"; echo "PASS ${SCENARIO_NAME}"; exit 0
  else evid "RESULT ${SCENARIO_NAME}: FAIL"; echo "FAIL ${SCENARIO_NAME}"; exit 1; fi
}

# `check` and shell conditionals keep expected non-zero results explicit. Any other failed
# top-level command is a harness defect or failed mutation and must fail closed after restoration.
install_scenario_error_guard

# Give scenario-specific EXIT restorers a chance to run when the campaign timeout forwards a
# signal. The campaign guard remains the final recovery layer if a cleanup itself fails or wedges.
trap 'exit 130' INT
trap 'exit 143' TERM
