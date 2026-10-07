#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${HERE}/lib.sh"
readonly VIP="${VIPS[0]}"

# Load only scenario predicates, never Docker setup or failure injection.
eval "$(awk '
  /^(restart_observation_now|restarted_effects_ready|assert_nopreempt_after_restart)\(\) \{/ { copying = 1 }
  copying { print }
  copying && /^}$/ { copying = 0 }
' "${HERE}/../scenarios22/03_nopreempt_survives_leader_restart.sh")"

old_boot="$(printf '%016x:%064x' 1 1)"
new_boot="$(printf '%016x:%064x' 1 2)"
fake_now=0
overlap_at=-1
move_at=-1
exit_at=-1
armed_at=28
guard_calls=0
restart_observation_now() { printf '%s\n' "${fake_now}"; }
sleep() { fake_now=$((fake_now + 1)); }
startup_budget_seconds() { printf '80\n'; }
compose() {
  printf 'runtime admission timing startup_safety_wait_ms=63500 vip_activation_ms=37500\n'
  if ((fake_now >= 26)); then printf 'runtime admission acquired replica=%s\n' "${new_boot}"; fi
  if ((fake_now >= armed_at)); then
    printf 'VIP effects armed after diskless replay reached the committed frontier\n'
  fi
}
service_is_running() { ((exit_at < 0 || fake_now < exit_at)); }
single_agreed_leader() { ((fake_now >= 28)); }
node_boot_replica() { printf '%s\n' "${new_boot}"; }
vip_held_by() { ((move_at < 0 || fake_now < move_at)); }
assert_no_duplicate_holders() {
  guard_calls=$((guard_calls + 1))
  ((overlap_at < 0 || fake_now < overlap_at))
}
assert_unique_holders() { assert_no_duplicate_holders && vip_held_by; }
dump_cluster_diagnostics() { :; }

declare -F assert_nopreempt_after_restart >/dev/null || {
  fail 'restart oracle does not wait for current-boot activation'; exit 1;
}
assert_nopreempt_after_restart node-a "${old_boot}" node-b
((fake_now >= 72 && guard_calls >= 65)) || {
  fail 'observation finished before the activation fence plus six seconds'; exit 1;
}
printf 'PASS: readiness and six-second observation follow the actual activation fence\n'

for failure in overlap move exit missing_armed observation_overlap observation_move observation_exit; do
  fake_now=0; overlap_at=-1; move_at=-1; exit_at=-1; armed_at=28; guard_calls=0
  case "${failure}" in
    overlap) overlap_at=40 ;;
    move) move_at=40 ;;
    exit) exit_at=40 ;;
    missing_armed) armed_at=200 ;;
    observation_overlap) overlap_at=68 ;;
    observation_move) move_at=68 ;;
    observation_exit) exit_at=68 ;;
  esac
  if assert_nopreempt_after_restart node-a "${old_boot}" node-b 2>/dev/null; then
    fail "accepted ${failure}"; exit 1
  fi
  case "${failure}" in
    observation_*) ((fake_now == 68)) ;;
    missing_armed) ((fake_now == 80)) ;;
    *) ((fake_now == 40)) ;;
  esac
  printf 'PASS: rejects %s during wait or observation\n' "${failure}"
done

fake_now=100; restart_seen_boot=''; restart_admitted_at=''
compose() {
  printf 'runtime admission timing vip_activation_ms=37500\nruntime admission acquired replica=%s\nVIP effects armed after diskless replay reached the committed frontier\nruntime admission timing vip_activation_ms=37500\n' "${old_boot}"
}
if restarted_effects_ready node-a "${old_boot}"; then
  fail 'old-boot arming satisfied the restart oracle'; exit 1
fi
printf 'PASS: prior-boot readiness cannot satisfy the new boot\n'
