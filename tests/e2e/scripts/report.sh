#!/usr/bin/env bash
# Aggregated e2e report generator (Bash port of the nfs repo's
# tests/integrity/lib/generate_report.py).
#
# Sourced by tests/e2e/scripts/run.sh. After a full run it writes
# e2e-artifacts/report.md summarizing each scenario's PASS/FAIL plus a short
# failure excerpt, so a reviewer (or an AI fed the artifacts) reads one file off
# the CI job page instead of digging through every per-scenario log folder. The
# report lands inside e2e-artifacts/, which both pipelines already upload on
# success and failure, so no CI change is needed for it to ship.
#
# Inputs (shell vars set by run.sh before the call):
#   PASSED  — space-separated scenario basenames that passed
#   FAILED  — space-separated scenario basenames that failed
# Relies on ROOT_DIR / ARTIFACT_DIR (lib.sh) and SCENARIO_DIR (run.sh).

# One-line "what it checks" per scenario, kept next to the scenarios so drift is
# obvious in review. Pulled from each scenario file's header comment.
declare -A SCENARIO_META=(
  [01_steady_state]="3-node cluster converges; each VIP uniquely held; one agreed leader"
  [02_kill_holder]="Kill VIP holder node-a; VIPs fail over onto the two survivors"
  [03_kill_leader]="Kill the current Raft leader; survivors elect a new leader and re-collapse VIPs"
  [04_unhealthy_holder]="Mark node-a unhealthy via probe; its VIPs migrate to healthy nodes"
  [05_network_partition]="Minority-partition node-a; it loses quorum and unbinds, majority keeps every VIP"
  [06_sigint_graceful]="SIGINT node-a; it gracefully unbinds every VIP and exits cleanly"
  [07_restart_rejoin]="Restart blank node-a; it rejoins the live cluster via replication, not a 2nd one"
  [08_full_outage_majority_recovers]="Whole cluster down; a majority returns blank, reforms, elects a leader, binds VIPs"
  [09_concurrent_cold_start]="All three return blank at once; concurrent init converges to one cluster (no split brain)"
  [10_returning_nodes_join_survivor]="Leader + a follower die; returning blank nodes join the survivor via replication"
  [11_survivor_rejoin_after_leader_change]="Like 10, but after forcing a leadership change first, so the result can't depend on a particular node id"
  [12_sticky_vip]="Kill a VIP holder; only its VIP moves while healthy survivors keep theirs (sticky); rejoin rebalances evenly"
  [13_stale_survivor_rejected_after_reform]="Partition node-a, reform the majority with new state, heal: node-a's stale state is fenced; it resets and rejoins blank (no split brain)"
)

# The decisive failure lines from a scenario's captured output, flattened into
# one markdown table cell. Prefers the harness's own '[e2e] ERROR:' assertions
# (printed by fail()) and takes the LAST two, since the run aborts at the failing
# assertion near the end — earlier matches are usually build/daemon noise that
# tee also captured. Falls back to panic/timeout lines. Best-effort: never fails
# the caller.
scenario_excerpt() {
  local name="${1:?scenario required}"
  local out="${ARTIFACT_DIR}/${name}/scenario.out"
  [[ -f "${out}" ]] || { printf 'no captured output'; return 0; }

  local lines
  lines="$(grep -E '\[e2e\] ERROR:' "${out}" 2>/dev/null | tail -n 2 || true)"
  [[ -n "${lines}" ]] || lines="$(grep -E 'panic|timed out' "${out}" 2>/dev/null | tail -n 2 || true)"
  [[ -n "${lines}" ]] || { printf 'see scenario.out'; return 0; }

  # Strip ANSI color codes (RUST_LOG output leaks them via compose logs), trim the
  # [e2e] prefix, escape the markdown cell separator, join with ' · '.
  printf '%s' "${lines}" \
    | sed -e 's/\x1b\[[0-9;]*m//g' -e 's/^\[e2e\] //' -e 's/|/\\|/g' \
    | awk 'BEGIN { ORS = "" } NR > 1 { print " · " } { print }'
}

# Render e2e-artifacts/report.md from the PASSED/FAILED scenario lists plus each
# scenario's captured output. Best-effort: a failure here must not mask the run.
generate_report() {
  local report="${ROOT_DIR}/e2e-artifacts/report.md"
  mkdir -p "$(dirname "${report}")"

  # Scenario-name lists are space-separated and contain no spaces/globs, so word
  # splitting is the intended parse here.
  # shellcheck disable=SC2206
  local -a pass_arr=(${PASSED:-}) fail_arr=(${FAILED:-})
  local -A status_of=()
  local s
  for s in "${pass_arr[@]}"; do status_of["${s}"]="PASS"; done
  for s in "${fail_arr[@]}"; do status_of["${s}"]="FAIL"; done

  local passed_count="${#pass_arr[@]}" failed_count="${#fail_arr[@]}"

  local verdict
  if (( failed_count > 0 )); then
    verdict="❌ **${failed_count} scenario(s) failed.** See the table below."
  elif (( passed_count == 0 )); then
    verdict="⚠️ **No scenarios ran.** Inspect \`compose/*/compose.log\`."
  else
    verdict="✅ **All ${passed_count} scenarios passed.**"
  fi

  {
    printf '# keepAfloatD e2e report\n\n'
    printf '%s\n\n' "${verdict}"
    printf '| Field | Value |\n|---|---|\n'
    printf '| Scenarios: passed / failed | %s / %s |\n\n' "${passed_count}" "${failed_count}"

    printf '## Per-scenario results\n\n'
    printf '| Scenario | Status | What it checks | Outcome |\n'
    printf '|---|---|---|---|\n'

    local path name status icon desc outcome
    for path in "${SCENARIO_DIR}"/[0-9][0-9]_*.sh; do
      [[ -e "${path}" ]] || continue
      name="$(basename "${path}" .sh)"
      status="${status_of[${name}]:-SKIP}"
      desc="${SCENARIO_META[${name}]:-—}"
      case "${status}" in
        PASS) icon="✅"; outcome="ok" ;;
        FAIL) icon="❌"; outcome="$(scenario_excerpt "${name}")" ;;
        *)    icon="⏭️"; status="SKIP"; outcome="not run" ;;
      esac
      printf '| %s | %s %s | %s | %s |\n' "${name}" "${icon}" "${status}" "${desc}" "${outcome}"
    done

    printf '\nFull per-node logs for each scenario are under '
    printf '`e2e-artifacts/compose/<scenario>/` '
    printf '(`compose.log`, `node-a/b/c.log`, `scenario.out`).\n'
  } >"${report}"

  log "wrote e2e report to ${report}"
}
