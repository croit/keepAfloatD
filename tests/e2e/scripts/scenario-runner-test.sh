#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${HERE}/run.sh" help >/dev/null

mapfile -t all < <(E2E_SHARD_INDEX=0 E2E_SHARD_COUNT=1 selected_scenarios)
declare -A selected=()
for index in {0..16}; do
  selection="$(E2E_SHARD_INDEX="$index" E2E_SHARD_COUNT=17 selected_scenarios)"
  while IFS= read -r scenario; do
    [[ -n "$scenario" && -z "${selected[$scenario]:-}" ]]
    selected["$scenario"]=1
  done <<<"$selection"
done
[[ "${#selected[@]}" == "${#all[@]}" ]]
for scenario in "${all[@]}"; do [[ "${selected[$scenario]:-}" == 1 ]]; done
printf 'PASS shard selection is complete and disjoint\n'

for settings in '17 17' '-1 17' '0 0' 'nope 1' '0 999'; do
  read -r index count <<<"$settings"
  if E2E_SHARD_INDEX="$index" E2E_SHARD_COUNT="$count" selected_scenarios >/dev/null 2>&1; then
    fail "accepted invalid shard $settings"
    exit 1
  fi
done
printf 'PASS invalid shard settings fail\n'

selected_scenarios() {
  [[ "$mode" != invalid ]] || return 2
  [[ "$mode" != empty ]] || return 0
  printf '%s\n' /fixture/01_first.sh /fixture/18_second.sh /fixture/27_third.sh
}

run_scenario() {
  executed+=("$1")
  # Docker exec can consume all inherited stdin even when its command needs none.
  cat >/dev/null
  if [[ "$mode" == missing && "$1" == /fixture/27_third.sh ]]; then
    passed=()
  fi
  if [[ "$mode" == excess && "$1" == /fixture/27_third.sh ]]; then
    passed+=(unexpected_result)
  fi
  [[ "$mode" != failure || "$1" != /fixture/01_first.sh ]]
}

generate_report() { report="${PASSED}|${FAILED}"; }

run_fixture() {
  mode="$1"; executed=(); report=unwritten; status=0
  run_all_scenarios </dev/null || status=$?
}

run_fixture success
[[ "$status" == 0 && "${#executed[@]}" == 3 &&
   "$report" == '01_first 18_second 27_third|' ]] || {
  fail "stdin reader lost scenarios: selected=3 executed=${#executed[@]} status=$status report=$report"
  exit 1
}
printf 'PASS stdin consumption cannot remove selected scenarios\n'

run_fixture failure
[[ "$status" == 1 && "${#executed[@]}" == 3 &&
   "$report" == '18_second 27_third|01_first' ]]
printf 'PASS failures remain failures and later scenarios still run\n'

run_fixture missing
[[ "$status" != 0 && "$report" == *01_first* && "$report" == *18_second* ]]
printf 'PASS missing selected results fail and appear in the report\n'

run_fixture excess
[[ "$status" == 1 && "${#executed[@]}" == 3 ]]
printf 'PASS extra results cannot hide a selection mismatch\n'

for mode in invalid empty; do
  run_fixture "$mode"
  [[ "$status" != 0 && "${#executed[@]}" == 0 && "$report" == unwritten ]]
done
printf 'PASS invalid or empty selection cannot produce success\n'
