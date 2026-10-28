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

(
  fixture_dir="$(mktemp -d)"
  trap 'rm -rf -- "${fixture_dir}"' EXIT
  ARTIFACT_DIR="${fixture_dir}/artifacts"
  trace="${fixture_dir}/profile-trace"
  observe_profile() {
    printf '%s:%s\n' "$1" "${KEEPAFLOATD_E2E_CONFIG_DIR:-unset}" >>"${trace}"
  }
  reset_cluster() { observe_profile reset; }
  wait_for_startup_state() { observe_profile ready; }
  capture_cluster_artifacts() { observe_profile capture; }
  down_cluster() { observe_profile down; }
  bash() {
    command bash -c 'printf "scenario:%s\n" "${KEEPAFLOATD_E2E_CONFIG_DIR:-unset}"; exit "$1"' \
      _ "${child_status:-0}" >>"${trace}"
  }
  check_profile() {
    local scenario="$1" expected="$2" status=0 stage
    : >"${trace}"
    run_scenario "/fixture/${scenario}.sh" >/dev/null 2>&1 || status=$?
    [[ "$status" == "${child_status:-0}" ]]
    diff -u <(for stage in reset ready scenario capture down; do
      printf '%s:%s\n' "$stage" "$expected"
    done) "${trace}" || {
      fail "wrong config profile for ${scenario}"
      exit 1
    }
  }
  unset KEEPAFLOATD_E2E_CONFIG_DIR
  check_profile 03_kill_leader configs-continuity
  [[ ! -v KEEPAFLOATD_E2E_CONFIG_DIR ]]
  check_profile 01_steady_state configs
  [[ ! -v KEEPAFLOATD_E2E_CONFIG_DIR ]]
  export KEEPAFLOATD_E2E_CONFIG_DIR=configs-secret-file
  check_profile 03_kill_leader configs-continuity
  [[ "$KEEPAFLOATD_E2E_CONFIG_DIR" == configs-secret-file ]]
  child_status=7
  check_profile 03_kill_leader configs-continuity
  [[ "$KEEPAFLOATD_E2E_CONFIG_DIR" == configs-secret-file ]]
  child_status=0
  check_profile 04_unhealthy_holder configs-secret-file
  printf 'PASS continuity profile is exported only for scenario 03, including failed cleanup\n'

  cp -a "${HERE}/../configs" "${fixture_dir}/configs"
  recipe="$(awk '
    /^RUN mkdir .*\/configs-continuity/ { copying = 1 }
    copying { sub(/^RUN /, ""); print; if ($0 !~ /\\$/) exit }
  ' "${HERE}/../fixtures/Dockerfile")"
  [[ -n "$recipe" ]] || { fail 'missing continuity fixture generation'; exit 1; }
  recipe="${recipe//\/opt\/keepafloatd-tests/${fixture_dir}}"
  command bash -euc "$recipe"
  for node in a b c; do
    original="${fixture_dir}/configs/${node}.yaml"
    for setting in 'election_timeout_min_ms: 1500' 'election_timeout_max_ms: 2500' 'heartbeat_interval_ms: 500'; do
      grep -q "^  ${setting}$" "$original"
    done
    diff -u <(sed \
      -e 's/election_timeout_min_ms: 1500/election_timeout_min_ms: 400/' \
      -e 's/election_timeout_max_ms: 2500/election_timeout_max_ms: 800/' \
      -e 's/heartbeat_interval_ms: 500/heartbeat_interval_ms: 250/' "$original") \
      "${fixture_dir}/configs-continuity/${node}.yaml"
  done
  printf 'PASS actual Dockerfile continuity recipe changes only election and heartbeat settings\n'
)

command bash -euo pipefail -s -- "${HERE}" <<'TIMING_PROFILE_TEST'
HERE="$1"
fixture_dir="$(mktemp -d)"
trap 'rm -rf -- "${fixture_dir}"' EXIT
cp -a "${HERE}/../configs22" "${fixture_dir}/configs22"
recipe="$(awk '
  /^RUN mkdir .*\/configs22-continuity/ { copying = 1 }
  copying { sub(/^RUN /, ""); print; if ($0 !~ /\\$/) exit }
' "${HERE}/../fixtures/Dockerfile")"
[[ -n "$recipe" ]] || { echo 'missing nopreempt continuity fixture generation' >&2; exit 1; }
recipe="${recipe//\/opt\/keepafloatd-tests/${fixture_dir}}"
command bash -euc "$recipe"
for node in a b c; do
  diff -u <(sed \
    -e 's/election_timeout_min_ms: 1500/election_timeout_min_ms: 400/' \
    -e 's/election_timeout_max_ms: 2500/election_timeout_max_ms: 800/' \
    -e 's/heartbeat_interval_ms: 500/heartbeat_interval_ms: 250/' \
    "${fixture_dir}/configs22/${node}.yaml") \
    "${fixture_dir}/configs22-continuity/${node}.yaml"
done
printf 'PASS nopreempt continuity fixture preserves health and failover settings\n'

# Help must not touch Docker, including through an EXIT trap.
docker() { echo 'unexpected Docker invocation' >&2; exit 1; }
source "${HERE}/run22.sh" help >/dev/null
ARTIFACT_DIR="${fixture_dir}/artifacts"
trace="${fixture_dir}/profile-trace"
observe_profile() { printf '%s:%s\n' "$1" "${KEEPAFLOATD_E2E_CONFIG_DIR}" >>"${trace}"; }
reset_cluster() { observe_profile reset; }
wait_for_startup_state() { observe_profile ready; }
capture_cluster_artifacts() { observe_profile capture; }
down_cluster() { observe_profile down; }
bash() {
  command bash -c 'printf "scenario:%s\n" "$KEEPAFLOATD_E2E_CONFIG_DIR"; exit "$1"' \
    _ "${child_status:-0}" >>"${trace}"
}
check_profile() {
  local scenario="$1" expected="$2" status=0 stage
  : >"${trace}"
  run_scenario "/fixture/${scenario}.sh" >/dev/null 2>&1 || status=$?
  [[ "$status" == "${child_status:-0}" ]]
  diff -u <(for stage in reset ready scenario capture down; do
    printf '%s:%s\n' "$stage" "$expected"
  done) "${trace}"
  [[ "$KEEPAFLOATD_E2E_CONFIG_DIR" == configs22 ]]
}
check_profile 03_nopreempt_survives_leader_restart configs22-continuity
for scenario in 01_failover_delay 02_nopreempt_fallback 04_ownerless_gap; do
  check_profile "$scenario" configs22
done
child_status=7
check_profile 03_nopreempt_survives_leader_restart configs22-continuity
printf 'PASS nopreempt continuity profile is scoped through cleanup and failure\n'
TIMING_PROFILE_TEST

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
