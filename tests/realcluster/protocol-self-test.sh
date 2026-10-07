#!/usr/bin/env bash
# Local protocol fixtures only: no SSH, sockets, daemons or real clocks.
replica_log_fixture() {
  local physical="${1:?}" byte="${2:-1}" nonce index
  nonce="$(for index in {1..32}; do printf '%s, ' "$byte"; done)"
  printf 'raft current leader is now Some(ReplicaId { physical_id: %s, boot_nonce: [%s] })\n' \
    "$physical" "${nonce%, }"
}

replica_agreement_fixture() (
  local mode="$1" byte=1
  kafd_active() { printf 'active\n'; }
  node_sh() {
    [[ "$mode" != split || "$1" == "${NODE_IPS[0]}" ]] || byte=2
    [[ "$mode" != all-split || "$1" == "${NODE_IPS[0]}" ]] || byte=2
    [[ "$mode" != all-split || "$1" != "${NODE_IPS[2]}" ]] || byte=3
    replica_log_fixture "${NODE_RAFT_IDS[0]}" "$byte"
  }
  case "$mode" in
    same) all_nodes_agree_on_leader && [[ "$(cluster_leader_id)" == "${NODE_RAFT_IDS[0]}" ]];;
    split) ! all_nodes_agree_on_leader && [[ "$(cluster_leader_id)" == "${NODE_RAFT_IDS[0]}" ]];;
    all-split) ! cluster_leader_id;;
  esac
)
for mode in same split all-split; do
  assert "full boot identity leader agreement: ${mode}" replica_agreement_fixture "$mode"
done

startup_scope_fixture() (
  local mode="$1" calls=0 fixture_context
  fixture_context='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 100'
  local fixture
  fixture="$(mktemp)"
  trap 'rm -f "$fixture"' EXIT
  printf '0\n' > "$fixture"
  timed_node_command() {
    read -r calls < "$fixture"
    printf '%s\n' "$((calls+1))" > "$fixture"
    if [[ "$mode" == restart && "$calls" -gt 0 ]]; then
      printf '%s\n' "${fixture_context/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/cccccccccccccccccccccccccccccccc}"
    else printf '%s\n' "$fixture_context"; fi
  }
  journal_evidence() {
    [[ "$3" == "${fixture_context% *} 0" ]] || return 1
    [[ "$mode" != transport ]] || return 255
    printf '92000\n'
  }
  if [[ "$mode" == good ]]; then
    [[ "$(node_startup_budget "${NODE_IPS[0]}" 30)" == 122 ]]
  else ! node_startup_budget "${NODE_IPS[0]}" 30; fi
)
for mode in good restart transport; do
  assert "startup budget pins current invocation: ${mode}" startup_scope_fixture "$mode"
done

activation_wait_fixture() (
  local mode="$1" clock=0 observations=0
  local fixture_context='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 0'
  date() { printf '%s\n' "$clock"; }
  sleep() { clock=$((clock+1)); }
  timed_node_command() {
    [[ "$mode" != restart || "$clock" -lt 1 ]] || fixture_context="${fixture_context/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/cccccccccccccccccccccccccccccccc}"
    printf '%s\n' "$fixture_context"
  }
  node_startup_budget() {
    [[ "$mode" != policy-error ]] || return 1
    if [[ "$mode" == policy-pending || "$mode" == policy-timeout ]]; then
      (( clock >= 2 )) && [[ "$mode" != policy-timeout ]] || return 2
    fi
    printf '5\n'
  }
  node_activation_ready() {
    [[ "$mode" != error ]] || return 1
    ((clock >= 2)) || return 2
  }
  no_vip_is_duplicate() {
    observations=$((observations+1))
    [[ "$mode" != duplicate || "$clock" != 1 ]]
  }
  if [[ "$mode" == good || "$mode" == policy-pending ]]; then
    wait_for_startup_activation 3 "${NODE_IPS[0]}" && ((clock >= 2 && observations >= 3))
  else
    ! wait_for_startup_activation 3 "${NODE_IPS[0]}" || return 1
    [[ "$mode" != error && "$mode" != policy-error || "$clock" == 0 ]]
  fi
)
for mode in good restart duplicate error policy-pending policy-error policy-timeout; do
  assert "activation wait retains no-overlap and process identity: ${mode}" activation_wait_fixture "$mode"
done

activation_reader_fixture() (
  local mode="$1" fixture_context='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 99000000'
  local reads=0 fixture result=0
  fixture="$(mktemp)"
  trap 'rm -f "$fixture"' EXIT
  printf '0\n' > "$fixture"
  node_active() { [[ "$mode" != inactive ]]; }
  timed_node_command() {
    read -r reads < "$fixture"
    printf '%s\n' "$((reads+1))" > "$fixture"
    if [[ "$mode" == restart && "$reads" -gt 0 ]]; then
      printf '%s\n' "${fixture_context/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/cccccccccccccccccccccccccccccccc}"
    else printf '%s\n' "$fixture_context"; fi
  }
  journal_evidence() {
    [[ "$1" == activation && "$3" == "${fixture_context% *} 0" && "$4" == 99000000 && "$5" == "${NODE_RAFT_IDS[0]}" ]] || return 1
    [[ "$mode" != transport ]] || return 255
    [[ "$mode" != pending ]] || return 2
    printf '%016x:%s\n' "${NODE_RAFT_IDS[0]}" "$(printf '01%.0s' {1..32})"
  }
  node_activation_ready "${NODE_IPS[0]}" "$fixture_context" || result=$?
  case "$mode" in
    good) [[ "$result" == 0 ]];;
    pending) [[ "$result" == 2 ]];;
    *) [[ "$result" != 0 ]];;
  esac
)
for mode in good pending restart transport inactive; do
  assert "activation reader checks the exact live invocation: ${mode}" activation_reader_fixture "$mode"
done

restart_scenarios_wait_for_activation_before_recovery_assertions() (
  local scenario body
  for scenario in D14_silent_nopreempt_fallback C4_cascading_failures; do
    body="$(sed -n '/^ *kafd_start /,$p' "${HERE}/scenarios/${scenario}.sh")"
    [[ "$body" == *wait_for_startup_activation* ]] || return 1
    if [[ "$scenario" == D14* ]]; then
      [[ "${body%%silent-recovered node does not preempt*}" == *wait_for_startup_activation* ]] || return 1
    fi
  done
)
assert "silent recovery and cascading restarts observe post-activation behavior" \
  restart_scenarios_wait_for_activation_before_recovery_assertions

steady_wait_cannot_hide_activation_failure() (
  local calls=0
  wait_for_startup_activation() { return 1; }
  wait_until() { calls=$((calls+1)); }
  ! wait_for_available_cluster && ! wait_for_steady_state && [[ "$calls" == 0 ]]
)
assert "availability and baseline waits stop on invalid activation evidence" \
  steady_wait_cannot_hide_activation_failure

crash_rejoin_keeps_its_cleanup_window_before_activation() (
  local body scenario
  for scenario in D3_silent_death_staleness D17_nopreempt_restart_leader_churn; do
    body="$(sed -n '/^kafd_start /,$p' "${HERE}/scenarios/${scenario}.sh")"
    [[ "${body%%wait_for_startup_activation*}" == *no_vip_is_duplicate* ]] || return 1
  done
  body="$(sed -n '/^ *kafd_start /,$p' "${HERE}/scenarios/C4_cascading_failures.sh")"
  [[ "${body%%wait_for_startup_activation*}" == *'wait_until 90 all_vips_uniquely_held'* ]]
)
assert "crash orphans use the existing cleanup window before continuous activation checks" \
  crash_rejoin_keeps_its_cleanup_window_before_activation
