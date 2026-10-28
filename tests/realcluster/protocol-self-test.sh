#!/usr/bin/env bash
# Local protocol fixtures only: no SSH, sockets, daemons or real clocks.
timed_context_diagnostics_fixture() (
  local mode="$1" output status=0
  node_sh() {
    case "$mode" in
      transport) return 255;;
      remote-pending-code) return 2;;
      malformed) printf 'private-payload\n';;
      good) printf 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 100\n';;
    esac
  }
  output="$(timed_node_command "${NODE_IPS[0]}" private-command 2>&1)" || status=$?
  case "$mode" in
    good) [[ "$status" == 0 && "$output" == 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 100' ]];;
    transport) [[ "$status" == 1 && "$output" == *'predicate=timed-command.read'* && "$output" == *'status=255'* ]];;
    remote-pending-code) [[ "$status" == 1 && "$output" == *'predicate=timed-command.read'* && "$output" == *'status=2'* ]];;
    malformed) [[ "$status" == 1 && "$output" == *'predicate=timed-command.context'* ]];;
  esac || return 1
  [[ "$output" != *private-payload* && "$output" != *private-command* ]]
)
for mode in good transport remote-pending-code malformed; do
  assert "timed context identifies original observation failure: ${mode}" timed_context_diagnostics_fixture "$mode"
done

service_query_diagnostics_fixture() (
  local mode="$1" output status=0
  node_sh() {
    [[ "$mode" != transport ]] || return 255
    printf '%s\n' "$mode"
  }
  output="$(kafd_active "${NODE_IPS[0]}" 2>&1)" || status=$?
  if [[ "$mode" == transport ]]; then
    [[ "$status" == 255 && "$output" == *'predicate=service.active-read'* && "$output" == *'status=255'* ]]
  else [[ "$status" == 0 && "$output" == "$mode" ]]; fi
)
for mode in active inactive transport; do
  assert "service query retains original observation failure: ${mode}" service_query_diagnostics_fixture "$mode"
done

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
  local mode="$1" calls=0 fixture_context output status=0 expected
  fixture_context='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 100'
  local fixture
  fixture="$(mktemp)"
  trap 'rm -f "$fixture"' EXIT
  printf '0\n' > "$fixture"
  timed_node_command() {
    read -r calls < "$fixture"
    printf '%s\n' "$((calls+1))" > "$fixture"
    [[ "$mode" != context-before || "$calls" != 0 ]] || return 255
    [[ "$mode" != context-after || "$calls" != 1 ]] || return 255
    if [[ "$mode" == restart && "$calls" -gt 0 ]]; then
      printf '%s\n' "${fixture_context/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/cccccccccccccccccccccccccccccccc}"
    else printf '%s\n' "$fixture_context"; fi
  }
  journal_evidence() {
    [[ "$3" == "${fixture_context% *} 0" ]] || return 1
    [[ "$mode" != transport ]] || return 255
    [[ "$mode" != pending ]] || return 2
    if [[ "$mode" == invalid-budget ]]; then printf 'private-payload\n'; return; fi
    printf '92000\n'
  }
  local base=30
  [[ "$mode" != bad-base ]] || base=private-payload
  output="$(node_startup_budget "${NODE_IPS[0]}" "$base" 2>&1)" || status=$?
  case "$mode" in
    good) [[ "$status" == 0 && "$output" == 122 ]]; return;;
    pending) [[ "$status" == 2 && -z "$output" ]]; return;;
    restart) expected=startup-budget.invocation;;
    transport) expected=startup-budget.journal; [[ "$status" == 255 ]] || return 1;;
    invalid-budget) expected=startup-budget.value;;
    context-before) expected=startup-budget.context-before;;
    context-after) expected=startup-budget.context-after;;
    bad-base) expected=startup-budget.base;;
  esac
  [[ "$status" != 0 && "$output" == *"predicate=$expected node=${NODE_IPS[0]}"* &&
    "$output" == *'controller_s='* && "$output" != *private-payload* ]]
)
for mode in good restart transport pending invalid-budget context-before context-after bad-base; do
  assert "startup budget pins current invocation: ${mode}" startup_scope_fixture "$mode"
done

scenario_runner_budget_fixture() (
  local mode="$1" fixture body result=0 name=C4_cascading_failures
  fixture="$(mktemp -d)"
  trap 'rm -rf "$fixture"' EXIT
  body="$(sed '/^HERE=/d; /^source /d' "$HERE/run-all.sh")"
  unset REALCLUSTER_SCENARIO_TIMEOUT
  unset REALCLUSTER_BASELINE_HASHES
  [[ "$mode" != explicit ]] || export REALCLUSTER_SCENARIO_TIMEOUT=1800
  [[ "$mode" != invalid ]] || export REALCLUSTER_SCENARIO_TIMEOUT=invalid
  [[ "$mode" != normal ]] || name=A1_ip_bind_unbind
  HERE="$fixture"
  mkdir "$HERE/scenarios"
  : > "$HERE/scenarios/$name.sh"
  install_campaign_guard_traps() { :; }
  mgmt_sh() { :; }
  prepare_cluster_secret() { REALCLUSTER_SECRET_CHANGED=0; }
  wait_for_steady_state() { :; }
  node_sh() { printf '%064d\n' 1; }
  node_startup_budget() {
    [[ "$mode" != unreadable ]] || return 255
    [[ "$2" == 0 ]] || return 1
    printf '260\n'
  }
  capture_scenario_guard() { printf 'capture\n' >> "$fixture/guard"; }
  restore_scenario_guard() {
    printf 'restore\n' >> "$fixture/guard"
    [[ "$mode" != restore-failure ]]
  }
  timeout() {
    [[ "$1 $2" == '--signal=TERM --kill-after=30' ]] || return 1
    printf '%s\n' "$3" > "$fixture/timeout"
    [[ "$mode" != forced-failure ]] || return 124
    [[ "$mode" == normal || "$3" -ge 1900 ]] || return 124
  }
  (set -- "$name"; eval "$body") > "$fixture/output" 2>&1 || result=$?
  case "$mode" in
    good)
      if [[ "$result" != 0 ]]; then
        cat "$fixture/output" >&2
        printf 'C4 runner used %s seconds; simulated completion needs 1900\n' \
          "$(<"$fixture/timeout")" >&2
        return 1
      fi
      [[ "$(<"$fixture/timeout")" == 3360 ]] && grep -q "$name .*PASS" "$fixture/results/report.md";;
    normal) [[ "$result" == 0 && "$(<"$fixture/timeout")" == 1800 ]];;
    explicit|forced-failure)
      [[ "$result" == 1 ]] && grep -q "$name .*FAIL" "$fixture/results/report.md" &&
        grep -q 'exit status 124' "$fixture/output";;
    restore-failure) [[ "$result" == 3 ]] && grep -q "$name .*FAIL" "$fixture/results/report.md";;
    unreadable|invalid) [[ "$result" == 3 && ! -e "$fixture/timeout" && ! -e "$fixture/guard" ]];;
  esac || return 1
  [[ "$mode" == unreadable || "$mode" == invalid ]] ||
    [[ "$(<"$fixture/guard")" == $'capture\nrestore' ]]
)
for mode in good normal explicit forced-failure restore-failure unreadable invalid; do
  assert "real runner preserves timing, failure and recovery contracts: ${mode}" \
    scenario_runner_budget_fixture "$mode"
done

cascading_round_count_fixture() (
  local body rounds=0 starts=0 activations=0 final_checks=0
  body="$(sed '/^source /d' "$HERE/scenarios/C4_cascading_failures.sh")"
  scenario_start() { :; }; scenario_end() { :; }; evid() { :; }
  kafd_kill() { :; }
  kafd_start() { starts=$((starts + 1)); }
  wait_until() { shift; "$@"; }
  holds_for() { shift; "$@"; }
  vips_uniquely_served_by_live() { :; }; node_active() { :; }
  all_vips_uniquely_held() { :; }; all_daemons_active() { :; }
  all_vips_pingable() { :; }; single_agreed_leader() { :; }
  wait_for_unique_vips_stable() { [[ "$1 $2" == '30 5' ]]; }
  wait_for_startup_activation() { activations=$((activations + 1)); }
  check() {
    local description="$1"
    shift
    [[ "$description" == 'restarted daemon completes activation before the next fault' ]] ||
      final_checks=$((final_checks + 1))
    "$@"
  }
  eval "$body" >/dev/null || return 1
  [[ "$rounds" == 6 && "$starts" == 6 && "$activations" == 6 && "$final_checks" == 5 ]]
)
assert "cascading scenario keeps all six restarts and five final assertions" cascading_round_count_fixture

scenario_timeout_fixture() (
  local mode="$1" output status=0 scenario=C4_cascading_failures calls
  calls="$(mktemp)"
  trap 'rm -f "$calls"' EXIT
  unset REALCLUSTER_SCENARIO_TIMEOUT
  node_startup_budget() {
    printf '%s\n' "$1" >> "$calls"
    [[ "$2" == 0 ]] || return 1
    [[ "$mode" != unreadable || "$1" != "${NODE_IPS[1]}" ]] || return 255
    [[ "$mode" != pending ]] || return 2
    if [[ "$mode" == malformed ]]; then printf 'private-payload\n'; return; fi
    if [[ "$1" == "${NODE_IPS[1]}" ]]; then printf '260\n'
    else printf '233\n'; fi
  }
  case "$mode" in
    normal) scenario=A1_ip_bind_unbind;;
    explicit) export REALCLUSTER_SCENARIO_TIMEOUT=1800;;
    invalid) export REALCLUSTER_SCENARIO_TIMEOUT=private-payload;;
    zero) export REALCLUSTER_SCENARIO_TIMEOUT=0;;
    empty) export REALCLUSTER_SCENARIO_TIMEOUT=;;
    no-nodes) NODE_IPS=();;
  esac
  output="$(scenario_timeout_seconds "$scenario" 2>&1)" || status=$?
  case "$mode" in
    good) [[ "$status" == 0 && "$output" == 3360 && "$(wc -l < "$calls")" == 3 ]];;
    normal|explicit) [[ "$status" == 0 && "$output" == 1800 && ! -s "$calls" ]];;
    invalid|zero|empty) [[ "$status" != 0 && "$output" == *scenario-budget.override* && ! -s "$calls" ]];;
    no-nodes) [[ "$status" != 0 && "$output" == *scenario-budget.nodes* ]];;
    unreadable|pending) [[ "$status" != 0 && "$output" == *scenario-budget.policy* ]];;
    malformed) [[ "$status" != 0 && "$output" == *scenario-budget.value* ]];;
  esac || return 1
  [[ "$output" != *private-payload* ]]
)
for mode in good normal explicit invalid zero empty no-nodes unreadable pending malformed; do
  assert "outer scenario budget includes restart fences without changing explicit caps: ${mode}" \
    scenario_timeout_fixture "$mode"
done

activation_wait_fixture() (
  local mode="$1" clock=0 observations=0 output expected status=0 fixture counter reads=0
  local fixture_context='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 0'
  fixture="$(mktemp)"
  counter="$(mktemp)"
  trap 'rm -f "$fixture" "$counter"' EXIT
  printf '0\n' > "$counter"
  date() { printf '%s\n' "$clock"; }
  sleep() { clock=$((clock+1)); }
  timed_node_command() {
    read -r reads < "$counter"
    printf '%s\n' "$((reads+1))" > "$counter"
    [[ "$mode" != context-error ]] || return 255
    [[ "$mode" != policy-context || "$reads" != 1 ]] || return 255
    [[ "$mode" != activation-context || "$reads" != 2 ]] || return 255
    [[ "$mode" != policy-restart || "$reads" != 1 ]] || fixture_context="${fixture_context/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/cccccccccccccccccccccccccccccccc}"
    [[ "$mode" != restart || "$clock" -lt 1 ]] || fixture_context="${fixture_context/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/cccccccccccccccccccccccccccccccc}"
    printf '%s\n' "$fixture_context"
  }
  node_startup_budget() {
    [[ "$mode" != policy-error ]] || return 1
    [[ "$mode" != policy-transport ]] || return 255
    if [[ "$mode" == policy-pending || "$mode" == policy-timeout || "$mode" == policy-duplicate ]]; then
      (( clock >= 2 )) && [[ "$mode" != policy-timeout ]] || return 2
    fi
    printf '5\n'
  }
  node_activation_ready() {
    [[ "$mode" != error ]] || return 1
    [[ "$mode" != transport ]] || return 255
    [[ "$mode" != activation-timeout ]] || return 2
    ((clock >= 2)) || return 2
  }
  no_vip_is_duplicate() {
    observations=$((observations+1))
    [[ "$mode" != policy-duplicate ]] || return 1
    [[ "$mode" != duplicate-after || "$observations" != 2 ]] || return 1
    [[ "$mode" != duplicate || "$clock" != 1 ]]
  }
  case "$mode" in
    no-nodes) wait_for_startup_activation 3 2>"$fixture" || status=$?;;
    bad-base) wait_for_startup_activation private-payload "${NODE_IPS[0]}" 2>"$fixture" || status=$?;;
    *) wait_for_startup_activation 3 "${NODE_IPS[0]}" 2>"$fixture" || status=$?;;
  esac
  output="$(<"$fixture")"
  case "$mode" in
    good|policy-pending) [[ "$status" == 0 && -z "$output" ]] && ((clock >= 2 && observations >= 3)); return;;
    restart) expected=wait.activation.invocation;;
    duplicate) expected=wait.activation.uniqueness-before;;
    duplicate-after) expected=wait.activation.uniqueness-after;;
    error|transport) expected=wait.activation.ready; [[ "$clock" == 0 ]] || return 1;;
    policy-error|policy-transport) expected=wait.policy.budget; [[ "$clock" == 0 ]] || return 1;;
    policy-timeout) expected=wait.policy.deadline; [[ "$clock" == 3 ]] || return 1;;
    policy-duplicate) expected=wait.policy.uniqueness;;
    activation-timeout) expected=wait.activation.deadline; [[ "$clock" == 5 ]] || return 1;;
    context-error) expected=wait.pin-context;;
    policy-context) expected=wait.policy.context;;
    activation-context) expected=wait.activation.context;;
    policy-restart) expected=wait.policy.invocation;;
    no-nodes) [[ "$status" == 1 && "$output" == *'predicate=wait.nodes'* ]]; return;;
    bad-base) [[ "$status" == 1 && "$output" == *'predicate=wait.base'* && "$output" != *private-payload* ]]; return;;
  esac
  [[ "$status" == 1 && "$output" == *"predicate=$expected node="* && "$output" == *'deadline_s='* ]] || return 1
  [[ "$mode" != *transport && "$mode" != context-error || "$output" == *'status=255'* ]]
)
for mode in good restart duplicate duplicate-after error transport policy-pending policy-error \
  policy-transport policy-timeout policy-duplicate activation-timeout context-error \
  policy-context activation-context policy-restart no-nodes bad-base; do
  assert "activation wait retains no-overlap and process identity: ${mode}" activation_wait_fixture "$mode"
done

activation_reader_fixture() (
  local mode="$1" fixture_context='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 99000000'
  local reads=0 fixture result=0 output expected active_checks=0
  fixture="$(mktemp)"
  trap 'rm -f "$fixture"' EXIT
  printf '0\n' > "$fixture"
  node_active() {
    active_checks=$((active_checks+1))
    [[ "$mode" != inactive && "$mode" != service-transport && "$mode" != service-invalid ]] || return 1
    [[ "$mode" != inactive-after || "$active_checks" != 2 ]]
  }
  kafd_active() {
    [[ "$mode" != service-transport ]] || { printf 'fixture service read failed\n' >&2; return 255; }
    [[ "$mode" != service-invalid ]] || { printf 'private-payload\n'; return; }
    printf 'inactive\n'
  }
  timed_node_command() {
    read -r reads < "$fixture"
    printf '%s\n' "$((reads+1))" > "$fixture"
    [[ "$mode" != context-before || "$reads" != 0 ]] || return 255
    [[ "$mode" != context-after || "$reads" != 1 ]] || return 255
    if [[ "$mode" == restart && "$reads" -gt 0 || "$mode" == restart-before ]]; then
      printf '%s\n' "${fixture_context/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/cccccccccccccccccccccccccccccccc}"
    else printf '%s\n' "$fixture_context"; fi
  }
  journal_evidence() {
    [[ "$1" == activation && "$3" == "${fixture_context% *} 0" && "$4" == 99000000 && "$5" == "${NODE_RAFT_IDS[0]}" ]] || return 1
    [[ "$mode" != transport ]] || return 255
    [[ "$mode" != pending ]] || return 2
    if [[ "$mode" == invalid-replica ]]; then printf 'private-payload\n'; return; fi
    printf '%016x:%s\n' "${NODE_RAFT_IDS[0]}" "$(printf '01%.0s' {1..32})"
  }
  [[ "$mode" != unknown-node ]] || NODE_RAFT_IDS[0]=""
  output="$(node_activation_ready "${NODE_IPS[0]}" "$fixture_context" 2>&1)" || result=$?
  case "$mode" in
    good) [[ "$result" == 0 && -z "$output" ]]; return;;
    pending) [[ "$result" == 2 && -z "$output" ]]; return;;
    restart) expected=activation.invocation-after;;
    restart-before) expected=activation.invocation-before;;
    transport) expected=activation.journal; [[ "$result" == 255 ]] || return 1;;
    inactive) expected=activation.active-before; [[ "$output" == *'followup_service_state=inactive'* ]] || return 1;;
    context-before) expected=activation.context-before;;
    context-after) expected=activation.context-after;;
    invalid-replica) expected=activation.replica;;
    unknown-node) expected=activation.physical;;
    inactive-after) expected=activation.active-after;;
    service-transport) expected=activation.active-before; [[ "$output" == *'status=255'* && "$output" == *'fixture service read failed'* ]] || return 1;;
    service-invalid) expected=activation.active-before; [[ "$output" == *'followup_service_state=unrecognized'* ]] || return 1;;
  esac
  [[ "$result" != 0 && "$output" == *"predicate=$expected node=${NODE_IPS[0]}"* &&
    "$output" != *private-payload* ]]
)
for mode in good pending restart restart-before transport inactive context-before context-after \
  invalid-replica unknown-node inactive-after service-transport service-invalid; do
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

partition_recovery_fixture() (
  local mode="$1" clock=0 healed=0 activated=0 failed=0 status=0 observed=0 changed=0 body
  NODE_IPS=(192.0.2.1 192.0.2.2 192.0.2.3)
  VIPS=(198.51.100.1)
  body="$(sed '/^source /d' "${HERE}/scenarios/D10_asymmetric_partition.sh")"
  scenario_start() { :; }; scenario_end() { return "$failed"; }
  evid() { :; }; log() { :; }; dump_diag() { :; }; fail() { return 1; }
  check() { shift; "$@" || failed=1; }
  date() { printf '%s\n' "$clock"; }
  sleep() { clock=$((clock+$1)); }
  partition_input_from() { :; }
  heal_node() { healed=1; clock=0; }
  snapshot_vips() { :; }; holder_for_vip() { printf '%s\n' "${NODE_IPS[0]}"; }
  single_agreed_leader() { :; }
  wait_for_startup_activation() {
    [[ "$*" == "30 ${NODE_IPS[*]}" ]] || return 1
    activated=1
    [[ "$mode" != activation-error ]] || return 1
    clock=233
  }
  all_daemons_active() { observed=$((observed+1)); [[ "$mode" != inactive ]]; }
  all_nodes_agree_on_leader() { (( clock >= 233 )) && [[ "$mode" != leader ]]; }
  all_vips_uniquely_held() {
    (( ! healed )) || { [[ "$mode" != duplicate ]] && (( ! changed )); }
  }
  all_vips_pingable() {
    [[ "$mode" != unreachable ]] || return 1
    [[ "$mode" != ownership-change ]] || changed=1
  }
  eval "$body" >/dev/null 2>&1 || status=$?
  if [[ "$mode" == good ]]; then
    [[ "$status" == 0 && "$activated" == 1 && "$clock" == 238 ]]
  else
    [[ "$status" != 0 ]] || return 1
    [[ "$mode" != activation-error || "$observed" == 0 ]]
  fi
)
for mode in good activation-error inactive leader duplicate unreachable ownership-change; do
  assert "partition recovery waits for activation and retains service checks: ${mode}" \
    partition_recovery_fixture "$mode"
done
