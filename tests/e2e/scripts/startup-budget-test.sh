#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/e2e/scripts/lib.sh
. "${HERE}/lib.sh"

[[ "$(printf '%s\n' 'runtime admission timing startup_safety_wait_ms=93500 quarantine_ms=14000 vip_activation_ms=79500' | startup_budget_from_log 30)" == 124 ]]
[[ "$(printf '%s\n' 'runtime admission timing startup_safety_wait_ms=1000' 'runtime admission timing startup_safety_wait_ms=2001' | startup_budget_from_log 5)" == 8 ]]
[[ "$(printf '\033[32mruntime admission timing\033[0m \033[3mstartup_safety_wait_ms\033[0m=2001\n' | startup_budget_from_log 5)" == 8 ]]
for line in 'no timing information' 'runtime admission timing startup_safety_wait_ms=oops' 'runtime admission timing startup_safety_wait_ms=1.5'; do
  if printf '%s\n' "${line}" | startup_budget_from_log 30; then
    fail "accepted missing or malformed runtime timing"
    exit 1
  fi
done
printf 'ok - runtime startup budget uses the latest valid timing record\n'

capture_compose_logs() {
  printf '%s\n' 'raft current leader is now Some(ReplicaId { physical_id: 2, boot_nonce: [7, 8] })' >"$1"
}
[[ "$(node_last_leader node-a)" == 2 ]]
[[ "$(node_last_leader_replica node-a)" == 'ReplicaId { physical_id: 2, boot_nonce: [7, 8] }' ]]
printf 'ok - exact boot leader identity and physical service identity stay separate\n'

capture_compose_logs() {
  printf '%s\n' 'raft current leader is now Some(ReplicaId { physical_id: 2, boot_nonce: [7, 8] })' \
    'raft current leader is now None' >"$1"
}
[[ -z "$(node_last_leader_replica node-a)" ]]
capture_compose_logs() {
  printf '%s\n' 'raft current leader is now Some(ReplicaId { physical_id: 2, boot_nonce: [7, 8] })' \
    'runtime admission timing startup_safety_wait_ms=93500' >"$1"
}
[[ -z "$(node_last_leader_replica node-a)" ]]
printf 'ok - absent leader and a fresh boot invalidate old leadership evidence\n'

stop_log=$'runtime admission timing startup_safety_wait_ms=93500\nINFO bound 192.0.2.1/32 on eth0\nINFO unbound 192.0.2.1/32 on eth0\nError: runtime admission expired terminally'
printf '%s\n' "${stop_log}" | assert_admission_stop_log
for bad in \
  "${stop_log/INFO unbound/INFO skipped}" \
  "${stop_log/runtime admission expired terminally/unrelated panic}" \
  "${stop_log}"$'\nruntime admission timing startup_safety_wait_ms=93500\nError: runtime admission expired terminally'; do
  if printf '%s\n' "${bad}" | assert_admission_stop_log; then
    fail 'admission stop accepted missing cleanup, unrelated failure or old-boot evidence'
    exit 1
  fi
done
printf 'ok - terminal stop requires current-boot expiry and complete VIP release\n'

service_container_id() { printf '%s\n' "$1"; }
docker() { printf '%s\n' true; }
node_sh() {
  [[ "$1" != node-a ]] || printf '1: eth0 inet 10.50.0.100/32 scope global eth0\n'
}
assert_no_duplicate_holders
node_sh() { printf '1: eth0 inet 10.50.0.100/32 scope global eth0\n'; }
if assert_no_duplicate_holders 2>/dev/null; then fail 'overlapping owners accepted'; exit 1; fi
node_sh() { return 1; }
if assert_no_duplicate_holders; then fail 'unreadable live node accepted'; exit 1; fi
printf 'ok - overlapping ownership and failed observations are rejected\n'

(
  compose() {
    printf '%s\n' 'runtime admission timing quarantine_ms=4000' \
      'runtime admission timing quarantine_ms=130001'
  }
  [[ "$(admission_exit_budget_seconds node-a 20)" == 53 ]]
  [[ "$(admission_pause_seconds node-a)" == 34 ]]
  for value in 0 invalid 1.5; do
    compose() { printf 'runtime admission timing quarantine_ms=%s\n' "${value}"; }
    if admission_exit_budget_seconds node-a 20 >/dev/null; then
      fail "accepted malformed permission timing ${value}"; exit 1
    fi
  done
  compose() {
    printf '%s\n' 'runtime admission timing quarantine_ms=4000' \
      'runtime admission timing startup_safety_wait_ms=130000'
  }
  if admission_exit_budget_seconds node-a 20 >/dev/null; then
    fail 'reused a previous boot permission budget'; exit 1
  fi
)
printf 'ok - permission expiry uses current-boot quarantine divided by four\n'

(
  fixture_watchdog_arm() { :; }
  fixture_watchdog_start fixture 180
  start="${fixture_watchdog_started}"
  fixture_watchdog_add_startup <<<'runtime admission timing startup_safety_wait_ms=130001'
  [[ "${fixture_watchdog_budget}" == 311 && "${fixture_watchdog_started}" == "${start}" ]]
  fixture_watchdog_add_startup <<<'runtime admission timing startup_safety_wait_ms=70500'
  [[ "${fixture_watchdog_budget}" == 382 && "${fixture_watchdog_started}" == "${start}" ]]
  if fixture_watchdog_add_startup <<<'missing timing'; then exit 1; fi
  [[ "${fixture_watchdog_budget}" == 382 ]]
)
printf 'ok - fixture deadline adds only observed startup delays to its original work budget\n'

(
  trace="$(mktemp)"
  trap 'rm -f "${trace}"' EXIT
  owner="${BASHPID}"
  timeout() {
    [[ "$*" == '-k 2s 10s docker stop --time 1 fixture' ]] || exit 1
    printf 'stop\n' >>"${trace}"
  }
  kill() {
    [[ "$*" == "-TERM ${owner}" ]] || exit 1
    printf 'term\n' >>"${trace}"
  }
  fixture_watchdog_start fixture 1
  wait "${fixture_watchdog_pid}"
  [[ "$(<"${trace}")" == $'stop\nterm' ]]
)
printf 'ok - fixture expiry stops only its container and interrupts the scenario\n'

(
  fixture_watchdog_start unused-fixture 30
  old="${fixture_watchdog_pid}"
  fixture_watchdog_add_startup <<<'runtime admission timing startup_safety_wait_ms=130000'
  if kill -0 "${old}" 2>/dev/null; then fail 'old fixture watchdog survived rearm'; exit 1; fi
  current="${fixture_watchdog_pid}"
  fixture_watchdog_stop
  if kill -0 "${current}" 2>/dev/null; then fail 'fixture watchdog survived cleanup'; exit 1; fi
)
printf 'ok - rearming and cleanup cancel and reap fixture watchdogs\n'

(
  child_file="$(mktemp)"
  trap 'if [[ -s "${child_file}" ]]; then builtin kill -KILL "$(<"${child_file}")" 2>/dev/null || true; fi; rm -f "${child_file}"' EXIT
  test_owner="${BASHPID}"
  set -T
  trap 'if [[ "${BASH_COMMAND}" == '\''timer=$!'\'' && "${BASHPID}" != "${test_owner}" ]]; then printf "%s\n" "$!" >"${child_file}"; builtin kill -TERM "${BASHPID}"; fi' DEBUG
  fixture_watchdog_start unused-fixture 30
  wait "${fixture_watchdog_pid}" || true
  trap - DEBUG
  set +T
  [[ -s "${child_file}" ]] || { fail 'cancellation injection did not run'; exit 1; }
  if builtin kill -0 "$(<"${child_file}")" 2>/dev/null; then
    fail 'cancellation before recording the timer PID leaked its sleep child'; exit 1
  fi
)
printf 'ok - cancellation cannot leak a timer before its PID is recorded\n'
