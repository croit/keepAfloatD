#!/usr/bin/env bash
# Sourced by self-test.sh; every remote and timing boundary is replaced locally.
self_fence_diagnostic_fixture() (
  local fixture_mode="${1:?}" output status=0
  # A fresh shell keeps the real ERR guard active inside assert's conditional.
  output="$(bash -c '
    source "$1"
    fixture_mode="$2"
    _PASS=1
    fixture_checks=0
    fixture_healed=0
    scenario_start() { :; }; snapshot_vips() { :; }
    evid() { printf "%s\n" "$*"; }
    scenario_end() {
      printf "RESULT=%s CHECKS=%s HEALED=%s\n" \
        "$_PASS" "$fixture_checks" "$fixture_healed"
      exit "$((1-_PASS))"
    }
    check() {
      shift
      fixture_checks=$((fixture_checks+1))
      if "$@"; then :; else _PASS=0; fi
    }
    node_sh() { :; }; partition_node() { :; }
    wait_until() { shift; "$@"; }; holds_for() { shift; "$@"; }
    node_lacks_all_vips() { [[ "$fixture_mode" != release-failure ]]; }
    node_active() { [[ "$fixture_mode" != daemon-failure ]]; }
    wait_for_live_service_without() { [[ "$fixture_mode" != majority-failure ]]; }
    heal_node() { fixture_healed=1; }
    all_daemons_active() { :; }; single_agreed_leader() { :; }
    all_vips_uniquely_held() { [[ "$fixture_mode" != healed-failure ]]; }
    all_vips_pingable() { :; }
    kafd_log() {
      case "$fixture_mode" in
        empty) :;;
        noise) printf "WARN heartbeat worker: request timed out\n";;
        read-error) return 255;;
        partial-read-error) printf "INFO self-fence release\n"; return 255;;
        legacy) printf "WARN CONSENSUS not fresh\n";;
        *)
          printf "INFO removed address %s/32 on %s\n" "${VIPS[0]}" "$IFACE"
          printf "INFO unbound %s/32 on %s\n" "${VIPS[0]}" "$IFACE";;
      esac
    }
    install_scenario_error_guard
    eval "$(sed "/^source /d" "$3")"
  ' _ "${HERE}/lib.sh" "$fixture_mode" \
    "${HERE}/scenarios/D2_consensus_self_fence.sh" 2>&1)" || status=$?
  case "$fixture_mode" in
    read-error|partial-read-error)
      [[ "$status" == 1 && "$output" == *"unchecked command failed (255)"* &&
         "$output" == *"RESULT=0 CHECKS=3 HEALED=0"* ]];;
    *-failure)
      [[ "$status" == 1 && "$output" == *"RESULT=0 CHECKS=5 HEALED=1"* ]];;
    *)
      [[ "$status" == 0 && "$output" == *"RESULT=1 CHECKS=5 HEALED=1"* ]] || {
        printf '%s\n' "$output" >&2
        return 1
      }
      case "$fixture_mode" in
        empty|noise) [[ "$output" == *"(none)"* ]];;
        legacy) [[ "$output" == *"CONSENSUS not fresh"* ]];;
        current) [[ "$output" == *"removed address"* && "$output" == *"unbound"* ]];;
      esac;;
  esac
)

for mode in empty noise legacy current read-error partial-read-error \
  release-failure daemon-failure majority-failure healed-failure; do
  assert "D2 diagnostic collection preserves safety checks: ${mode}" \
    self_fence_diagnostic_fixture "$mode"
done

scenario_evidence_fixture() (
  local scenario="${1:?}" fixture_mode="${2:?}" fixture body status=0
  fixture="$(mktemp -d)"
  trap 'rm -rf "${fixture}"' EXIT
  body="$(sed '/^source /d' "${HERE}/scenarios/${scenario}.sh")"
  NODE_IPS=(192.0.2.1 192.0.2.2 192.0.2.3)
  NODE_RAFT_IDS=(1 2 3)
  VIPS=(198.51.100.1 198.51.100.2 198.51.100.3)
  IFACE=eth0
  local fixture_boot=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  local fixture_old=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb fixture_new=cccccccccccccccccccccccccccccccc
  local restarted=0 result=0 clock=0 healed=0 fixture_emit_inv=""
  local old_replica="0000000000000001:$(printf 'ab%.0s' {1..32})"
  local new_replica="0000000000000001:$(printf 'cd%.0s' {1..32})"
  printf '0\n' > "${fixture}/clock"
  printf '0\n' > "${fixture}/context-count"
  scenario_start() { :; }; scenario_end() { exit "${result}"; }
  evid() { :; }; snapshot_vips() { :; }
  holder_for_vip() { printf '%s\n' "${NODE_IPS[0]}"; }
  node_has_vip_bound() { [[ "$2" == "${VIPS[0]}" ]]; }
  check() { shift; "$@" || result=1; }
  check_eq() { [[ "$2" == "$3" ]] || result=1; }
  sleep() { :; }; wait_until() { shift; "$@"; }; holds_for() { shift; "$@"; }
  date() {
    if [[ "$1" == +%s ]]; then
      read -r clock < "${fixture}/clock"
      printf '%s\n' "$((clock*1000))"
      printf '%s\n' "$((clock+1))" > "${fixture}/clock"
    else printf '2026-01-01 00:00:00 UTC\n'; fi
  }
  timed_node_command() {
    local fixture_inv="${fixture_old}" fixture_current_boot="${fixture_boot}" fixture_calls
    read -r fixture_calls < "${fixture}/context-count"
    printf '%s\n' "$((fixture_calls+1))" > "${fixture}/context-count"
    if (( restarted )) && [[ "$fixture_mode" != same-process ]]; then fixture_inv="${fixture_new}"; fi
    if [[ "$fixture_mode" == context-restart && "$fixture_calls" -ge 3 ]]; then fixture_inv="${fixture_new}"; fi
    if [[ "$fixture_mode" == read-restart && "$fixture_calls" -ge 4 ]]; then fixture_inv="${fixture_new}"; fi
    if [[ "$fixture_mode" == boot-change && "$fixture_calls" -ge 1 ]]; then fixture_current_boot="${fixture_new}"; fi
    printf '%s %s 100\n' "${fixture_current_boot}" "${fixture_inv}"
  }
  emit_record() {
    local fixture_inv="${fixture_emit_inv:-${fixture_old}}"
    [[ "$scenario" == A5* ]] && fixture_inv="${fixture_new}"
    [[ "$fixture_mode" == old-invocation ]] && fixture_inv=dddddddddddddddddddddddddddddddd
    python3 -c 'import json,sys; print(json.dumps(dict(
        _BOOT_ID=sys.argv[1], _SYSTEMD_INVOCATION_ID=sys.argv[2],
        __MONOTONIC_TIMESTAMP="200", MESSAGE=sys.argv[3])))' "${fixture_boot}" "${fixture_inv}" "$1"
  }
  emit_cycle() {
    local index="$1" replica="0000000000000001:$(printf 'ab%.0s' {1..32})"
    emit_record "INFO openraft::core::raft_core: sm::StateMachine command done: BuildSnapshotDone: {snapshot_id: snapshot-T1-N${replica}.${index}, last_log:T1-N${replica}.${index}, last_membership: {}}"
    emit_record "INFO openraft::engine::handler::log_handler: purge log, last_purged: None, purge_upto: T1-N${replica}.$((index-1000))"
  }
  node_sh() {
    if [[ "$2" == journalctl* ]]; then
      case "$scenario" in
        A5*)
          if [[ "$fixture_mode" == cleanup-error ]]; then
            emit_record 'ERROR startup_cleanup: spawn ip del: failure'
          else emit_record "INFO keepafloatd::vip: startup_cleanup: reclaimed orphan ${VIPS[2]}/32 on eth0"; fi;;
        D7*)
          fixture_emit_inv="$fixture_old"
          [[ "$2" != *"_SYSTEMD_INVOCATION_ID=${fixture_new}"* ]] || fixture_emit_inv="$fixture_new"
          if [[ "$1" == "${NODE_IPS[0]}" ]]; then
            if [[ "$fixture_emit_inv" == "$fixture_old" ]]; then
              emit_record "INFO keepafloatd::raft::admission::runtime::driver: runtime admission acquired replica=${old_replica}"
              [[ "$fixture_mode" == missing-release ]] || emit_record "INFO keepafloatd::vip: unbound ${VIPS[0]}/32 on eth0"
              if [[ "$fixture_mode" == generic-epoch ]]; then emit_record 'WARN unrelated epoch message'
              elif [[ "$fixture_mode" == arbitrary-restart ]]; then emit_record 'Error: network task failed'
              else emit_record 'Error: runtime admission task: runtime admission expired terminally'; fi
              [[ "$fixture_mode" != rebound ]] || emit_record "INFO keepafloatd::vip: bound ${VIPS[0]}/32 on eth0"
            else
              local replica="$new_replica"
              [[ "$fixture_mode" != missing-admission ]] || return 0
              [[ "$fixture_mode" != same-boot ]] || replica="$old_replica"
              [[ "$fixture_mode" != wrong-admission ]] || replica="0000000000000002:${new_replica#*:}"
              emit_record "INFO keepafloatd::raft::admission::runtime::driver: runtime admission acquired replica=${replica}"
            fi
          elif (( healed )); then
            local promoted="$new_replica"
            [[ "$fixture_mode" != wrong-promotion ]] || promoted="$old_replica"
            [[ "$fixture_mode" != missing-promotion ]] || return 0
            [[ "$fixture_mode" != peer-read-error || "$1" != "${NODE_IPS[2]}" ]] || return 255
            [[ "$1" == "${NODE_IPS[1]}" ]] || return 0
            emit_record "INFO keepafloatd::raft::store::membership: committed learner promotion consumer=${promoted}"
          fi;;
        D19*)
          local target="${VIPS[0]}"
          [[ "$fixture_mode" == unrelated-unbind ]] && target="${VIPS[2]}"
          emit_record "INFO keepafloatd::vip: unbound ${target}/32 on eth0"
          emit_record 'ERROR keepafloatd::raft::control: cluster configuration mismatch confirmed; shutting down safely'
          if [[ "$fixture_mode" == rebound ]]; then emit_record "INFO keepafloatd::vip: bound ${target}/32 on eth0"; fi;;
        C2*)
          if [[ "$fixture_mode" == one-node-only && "$1" != "${NODE_IPS[0]}" ]]; then
            emit_record 'INFO no completed snapshots'
          else emit_cycle 4999; emit_cycle 9999; fi;;
      esac
      [[ "$fixture_mode" != partial-transport ]] || return 255
    else
      case "$2" in *interval_ms*) echo 500;; *stale_secs*) echo 2;; *) echo 1;; esac
    fi
  }
  node_active() { :; }; kafd_kill() { :; }; kafd_stop() { :; }; kafd_restart() { restarted=1; }
  kafd_start() {
    [[ "$1" != "${NODE_IPS[0]}" && "$scenario" != D7* ]] || restarted=1
  }
  partition_node() { :; }; heal_node() { restarted=1; healed=1; }
  all_vips_uniquely_held() { [[ "$fixture_mode" != duplicate-vip ]]; }
  no_vip_is_duplicate() { :; }; all_vips_pingable() { :; }; all_daemons_active() { :; }
  node_lacks_all_vips() { :; }; single_agreed_leader() { :; }
  wait_for_live_service_without() { :; }; wait_for_available_cluster() { :; }
  wait_for_startup_activation() { :; }
  backup_cluster_configs() { :; }; restore_cluster_configs() { :; }; clean_reform() { :; }
  instance_for_ip() { printf 'fixture\n'; }
  nodes_except() { printf '%s\n' "${NODE_IPS[1]}" "${NODE_IPS[2]}"; }
  leader_seen_by() {
    local physical=2
    [[ "$1" != "${NODE_IPS[0]}" ]] || return 1
    [[ "$fixture_mode" != leader-read-error || "$1" != "${NODE_IPS[2]}" ]] || return 255
    [[ "$fixture_mode" != leader-outside-subset ]] || physical=1
    [[ "$fixture_mode" != leader-unknown ]] || physical=4
    [[ "$fixture_mode" != leader-disagreement || "$1" != "${NODE_IPS[2]}" ]] || physical=3
    printf '%016x:%s\n' "$physical" "$(printf 'ef%.0s' {1..32})"
  }
  replica_is_configured() { [[ "$1" =~ ^000000000000000[123]:[a-f0-9]{64}$ ]]; }
  replica_physical_id() { printf '%s\n' "$((16#${1%%:*}))"; }
  next_behavior_changing_stale_secs() { echo 4; }; journal_event_count() { echo 0; }
  clear_health_sentinels() { :; }; configure_sentinel_health() { :; }; wait_for_even() { :; }
  (eval "${body}") >/dev/null 2>&1 || status=$?
  if [[ "$fixture_mode" == good || "$fixture_mode" == expiry-before-heal ]]; then
    [[ "$status" == 0 ]]
  else [[ "$status" != 0 ]]; fi
)

for scenario in A5_startup_cleanup D7_stale_survivor D19_config_identity C2_endurance_snapshots; do
  for mode in good old-invocation partial-transport; do
    assert "${scenario} scoped evidence: ${mode}" scenario_evidence_fixture "${scenario}" "${mode}"
  done
done
assert "D7 accepts terminal permission expiry before partition healing" \
  scenario_evidence_fixture D7_stale_survivor expiry-before-heal
for mode in same-process boot-change cleanup-error; do
  assert "A5 rejects ${mode}" scenario_evidence_fixture A5_startup_cleanup "${mode}"
done
for mode in same-process boot-change generic-epoch arbitrary-restart same-boot missing-release rebound \
  missing-promotion wrong-promotion peer-read-error leader-read-error missing-admission wrong-admission \
  leader-outside-subset leader-unknown leader-disagreement; do
  assert "D7 rejects ${mode}" scenario_evidence_fixture D7_stale_survivor "${mode}"
done
for mode in unrelated-unbind rebound same-process boot-change; do
  assert "D19 rejects ${mode}" scenario_evidence_fixture D19_config_identity "${mode}"
done
for mode in one-node-only duplicate-vip context-restart read-restart; do
  assert "C2 rejects ${mode}" scenario_evidence_fixture C2_endurance_snapshots "${mode}"
done
assert "journal parsers reject false-positive evidence" \
  env PYTHONDONTWRITEBYTECODE=1 python3 "${HERE}/journal-evidence-test.py"

snapshot_read_checks_the_invocation_after_reading() (
  local fixture_mode="$1" fixture count=0 status=0
  local fixture_context='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 100'
  fixture="$(mktemp)"
  trap 'rm -f "${fixture}"' EXIT
  printf '0\n' > "${fixture}"
  timed_node_command() {
    read -r count < "${fixture}"
    printf '%s\n' "$((count+1))" > "${fixture}"
    if (( count > 0 )); then
      case "$fixture_mode" in
        restart) printf '%s\n' "${fixture_context/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/cccccccccccccccccccccccccccccccc}"; return;;
        transport) return 255;;
      esac
    fi
    printf '%s\n' "${fixture_context}"
  }
  journal_evidence() { printf '2\n'; }
  node_snapshot_cycles fixture "${fixture_context}" >/dev/null 2>&1 || status=$?
  if [[ "$fixture_mode" == good ]]; then [[ "$status" == 0 ]]; else [[ "$status" != 0 ]]; fi
)
for mode in good restart transport; do
  assert "snapshot read final invocation: ${mode}" snapshot_read_checks_the_invocation_after_reading "$mode"
done

snapshot_empty_journal_is_not_a_transport_error() (
  local fixture_mode="$1" output status=0
  local fixture_context='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 100'
  timed_node_command() { printf '%s\n' "${fixture_context}"; }
  instance_for_ip() { printf 'fixture\n'; }
  node_sh() {
    [[ "$fixture_mode" != transport ]] || return 255
    # journalctl --grep exits one when a readable journal has no matching entries.
    [[ "$2" != *--grep* ]]
  }
  output="$(node_snapshot_cycles fixture "${fixture_context}")" || status=$?
  if [[ "$fixture_mode" == empty ]]; then [[ "$status" == 0 && "$output" == 0 ]]
  else [[ "$status" != 0 ]]; fi
)
for mode in empty transport; do
  assert "snapshot journal with no records: ${mode}" snapshot_empty_journal_is_not_a_transport_error "$mode"
done

journal_failure_diagnostics_fixture() (
  local fixture_mode="$1" output status=0 expected
  local context='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 0'
  instance_for_ip() { [[ "$fixture_mode" != mapping ]] || return 1; printf 'fixture\n'; }
  node_sh() {
    case "$fixture_mode" in
      transport) printf 'fixture journal read failed\n' >&2; return 255;;
      invalid) printf '{"MESSAGE":"private-payload"}\n';;
      *) :;;
    esac
  }
  [[ "$fixture_mode" != context ]] || context=private-payload
  output="$(journal_evidence startup-budget "${NODE_IPS[0]}" "$context" 2>&1)" || status=$?
  case "$fixture_mode" in
    pending) [[ "$status" == 2 && -z "$output" ]]; return;;
    context) expected=journal.context;;
    mapping) expected=journal.instance;;
    transport) expected=journal.read; [[ "$output" == *'status=255'* && "$output" == *'fixture journal read failed'* ]] || return 1;;
    invalid) expected=journal.parse; [[ "$output" == *'invalid scenario evidence:'* ]] || return 1;;
  esac
  [[ "$status" == 1 && "$output" == *"predicate=$expected node=${NODE_IPS[0]}"* &&
    "$output" == *'mode=startup-budget'* && "$output" != *private-payload* ]]
)
for mode in pending context mapping transport invalid; do
  assert "journal failure diagnostics preserve status and stderr: ${mode}" journal_failure_diagnostics_fixture "$mode"
done

vip_snapshot_diagnostics_fixture() (
  local mode="$1" output status=0
  NODE_IPS=(192.0.2.1 192.0.2.2)
  VIPS=(198.51.100.1)
  node_sh() {
    if [[ "$1" == "${NODE_IPS[1]}" ]]; then
      case "$mode" in
        transport) printf 'fixture address read failed\n' >&2; return 255;;
        invalid) printf 'private-payload\n'; return;;
        empty) return;;
      esac
    fi
    printf '%s\n' "${VIPS[0]}"
  }
  output="$(no_vip_is_duplicate 2>&1)" || status=$?
  case "$mode" in
    empty) [[ "$status" == 0 && -z "$output" ]]; return;;
    transport) [[ "$output" == *'predicate=vip-snapshot.read node=192.0.2.2 status=255'* &&
      "$output" == *'fixture address read failed'* ]] || return 1;;
    invalid) [[ "$output" == *'predicate=vip-snapshot.address node=192.0.2.2 status=1'* ]] || return 1;;
    duplicate) [[ "$output" == *'predicate=vip-snapshot.duplicate'* &&
      "$output" == *'vip=198.51.100.1 holders=192.0.2.1,192.0.2.2'* ]] || return 1;;
  esac
  [[ "$status" == 1 && "$output" != *private-payload* ]]
)
for mode in empty transport invalid duplicate; do
  assert "VIP snapshot distinguishes observation failure from overlap: ${mode}" vip_snapshot_diagnostics_fixture "$mode"
done

failed_ownership_sample_fixture() (
  local mode="$1" output status=0 fixture
  fixture="$(mktemp -d)"
  trap 'rm -rf "$fixture"' EXIT
  NODE_IPS=(192.0.2.1 192.0.2.2)
  VIPS=(198.51.100.1)
  date() { printf '123\n'; }
  node_sh() {
    printf 'read\n' >> "$fixture/$1"
    case "$mode:$1" in
      transport:192.0.2.2) return 255;;
      invalid:192.0.2.2) printf 'private-payload\n'; return;;
      none:*|good:192.0.2.2) return;;
    esac
    printf '%s\n' "${VIPS[0]}"
  }
  output="$(holds_unique_vips_for 0 2>&1)" || status=$?
  for ip in "${NODE_IPS[@]}"; do
    [[ -f "$fixture/$ip" && "$(wc -l < "$fixture/$ip")" == 1 ]] || return 1
  done
  if [[ "$mode" == good ]]; then
    [[ "$status" == 0 && -z "$output" ]]; return
  fi
  [[ "$status" == 1 && "$output" != *private-payload* &&
    "$output" == *'node=192.0.2.1 controller_started_s=123 controller_finished_s=123 status=0'* ]] || return 1
  case "$mode" in
    none) [[ "$output" == *'vip=198.51.100.1 holder=none'* ]];;
    duplicate) [[ "$output" == *'vip=198.51.100.1 holder=duplicate:192.0.2.1,192.0.2.2'* ]];;
    transport) [[ "$output" == *'node=192.0.2.2 controller_started_s=123 controller_finished_s=123 status=255'* &&
      "$output" == *'ownership=unreadable'* && "$output" != *'holder=none'* ]];;
    invalid) [[ "$output" == *'ownership=unreadable'* && "$output" != *'holder=duplicate'* ]];;
  esac
)
for mode in good none duplicate transport invalid; do
  assert "final ownership evidence retains the original sample without rereading: ${mode}" \
    failed_ownership_sample_fixture "$mode"
done

vip_convergence_fixture_boundaries() {
  fixture="$(mktemp -d)"
  trap 'rm -rf "$fixture"' EXIT
  NODE_IPS=(192.0.2.1 192.0.2.2 192.0.2.3)
  VIPS=(198.51.100.1 198.51.100.2 198.51.100.3)
  IFACE=eth0
  printf '0\n' > "$fixture/clock"
  local ip
  for ip in "${NODE_IPS[@]}"; do printf '0\n' > "$fixture/$ip.count"; done
  date() { printf '%s\n' "$(<"$fixture/clock")"; }
  monotonic_ns() { printf '%s\n' "$(( $(<"$fixture/clock") * 1000000000 ))"; }
  sleep() {
    local now delay="${1%%.*}"
    (( delay > 0 )) || delay=1
    read -r now < "$fixture/clock"
    printf '%s\n' "$((now+delay))" > "$fixture/clock"
  }
  node_sh() {
    local ip="${1:?}" now reads index
    [[ "$2" == *'ip -o -4 addr show dev '* ]] || return 98
    read -r now < "$fixture/clock"
    read -r reads < "$fixture/$ip.count"
    reads=$((reads+1))
    printf '%s\n' "$reads" > "$fixture/$ip.count"
    printf '%s\n' "$now" >> "$fixture/$ip.polls"
    if [[ "$ip" == "${NODE_IPS[1]}" ]]; then
      case "$fixture_mode" in
        handoff) (( now >= 2 )) || return 0;;
        reset) (( now < 2 || now >= 4 )) || return 0;;
        absent) return 0;;
        late-owner) (( now >= 28 )) || return 0;;
        c4-handoff) (( fixture_activations < 6 || now >= 2 )) || return 0;;
        duplicate) (( reads != 1 )) || printf '%s ' "${VIPS[0]}";;
        transport) (( reads != 1 )) || return 255;;
        malformed) (( reads != 1 )) || { printf 'invalid-address\n'; return 0; };;
      esac
    fi
    if [[ "$ip" == "${NODE_IPS[2]}" ]]; then
      case "$fixture_mode:$reads" in
        slow-first:1) printf '4\n' > "$fixture/next-clock";;
        read-deadline:1)
          printf '31\n' > "$fixture/next-clock"
          printf '%s\n' "$reads" > "$fixture/deadline-read";;
        final-read-deadline:*)
          if (( now >= 5 )) && [[ ! -f "$fixture/deadline-read" ]]; then
            printf '31\n' > "$fixture/next-clock"
            printf '%s\n' "$reads" > "$fixture/deadline-read"
          fi;;
      esac
      # Parallel readers must never see a partially replaced virtual clock.
      if [[ -f "$fixture/next-clock" ]]; then mv "$fixture/next-clock" "$fixture/clock"; fi
    fi
    for index in "${!NODE_IPS[@]}"; do
      [[ "$ip" != "${NODE_IPS[$index]}" ]] || printf '%s ' "${VIPS[$index]}"
    done
  }
}

unique_vips_stability_fixture() (
  local fixture_mode="${1:?}" fixture status=0 output now ip reads expected_reads="" earliest=5
  declare -F wait_for_unique_vips_stable >/dev/null || {
    printf 'missing wait_for_unique_vips_stable\n' >&2
    return 1
  }
  vip_convergence_fixture_boundaries
  output="$(wait_for_unique_vips_stable 30 5 2>&1)" || status=$?
  read -r now < "$fixture/clock"
  for ip in "${NODE_IPS[@]}"; do
    read -r reads < "$fixture/$ip.count"
    (( reads > 0 )) || return 1
    [[ -z "$expected_reads" || "$reads" == "$expected_reads" ]] || return 1
    expected_reads="$reads"
    # One node read per poll, including the terminal observation and diagnostics.
    awk 'seen[$0]++ { exit 1 }' "$fixture/$ip.polls" || return 1
  done
  case "$fixture_mode" in
    unique|handoff|reset|slow-first)
      case "$fixture_mode" in handoff) earliest=7;; reset|slow-first) earliest=9;; esac
      [[ "$status" == 0 ]] && (( now >= earliest && now <= 30 )) || {
        printf 'mode=%s status=%s clock=%s earliest=%s\n%s\n' \
          "$fixture_mode" "$status" "$now" "$earliest" "$output" >&2
        return 1
      };;
    duplicate|transport|malformed)
      (( status != 0 && now == 0 && expected_reads == 1 ));;
    absent|late-owner)
      (( status != 0 && now == 30 ));;
    read-deadline)
      (( status != 0 && now == 31 && expected_reads == 1 ));;
    final-read-deadline)
      (( status != 0 && now == 31 )) && [[ "$expected_reads" == "$(<"$fixture/deadline-read")" ]];;
  esac
)

assert "VIP convergence: immediate unique ownership stays stable for five seconds" \
  unique_vips_stability_fixture unique
assert "VIP convergence: short ownerless handoff then stable ownership succeeds" \
  unique_vips_stability_fixture handoff
assert "VIP convergence: absence resets the continuous stability window" \
  unique_vips_stability_fixture reset
assert "VIP convergence: duplicate fails before a recovering second sample" \
  unique_vips_stability_fixture duplicate
assert "VIP convergence: transport failure is terminal without a reread" \
  unique_vips_stability_fixture transport
assert "VIP convergence: malformed addresses are terminal without a reread" \
  unique_vips_stability_fixture malformed
assert "VIP convergence: persistent absence reaches the original deadline" \
  unique_vips_stability_fixture absent
assert "VIP convergence: late ownership cannot extend the deadline to finish stability" \
  unique_vips_stability_fixture late-owner
assert "VIP convergence: a slow first snapshot does not count as stable time" \
  unique_vips_stability_fixture slow-first
assert "VIP convergence: a first read crossing the deadline fails without retry" \
  unique_vips_stability_fixture read-deadline
assert "VIP convergence: a final read crossing the deadline cannot report success" \
  unique_vips_stability_fixture final-read-deadline

fractional_vip_deadline_fixture() (
  local fixture mode="${1:?}" status=0 output
  fixture="$(mktemp -d)"
  trap 'rm -rf "$fixture"' EXIT
  NODE_IPS=(192.0.2.1)
  VIPS=(198.51.100.1)
  printf '100100000000\n' > "$fixture/clock"
  printf '0\n' > "$fixture/reads"
  date() { printf '%s\n' "$(( $(<"$fixture/clock") / 1000000000 ))"; }
  monotonic_ns() {
    [[ "$mode" != clock-failure || "$(<"$fixture/reads")" == 0 ]] || return 1
    printf '%s\n' "$(<"$fixture/clock")"
  }
  sleep() { :; }
  snapshot_vips() {
    local reads=$(( $(<"$fixture/reads") + 1 )) now
    printf '%s\n' "$reads" > "$fixture/reads"
    case "$mode:$reads" in
      deadline:1) now=125100000000;;
      deadline:*) now=130900000000;;
      short-window:1) now=100900000000;;
      short-window:2) now=105100000000;;
      short-window:*) now=131000000000;;
      clock-failure:*) now=131000000000;;
    esac
    printf '%s\n' "$now" > "$fixture/clock"
    _SNAP_COMPLETE=1
    _SNAP_VIPS_ON[192.0.2.1]=" 198.51.100.1 "
  }
  output="$(wait_for_unique_vips_stable 30 5 2>&1)" || status=$?
  (( status != 0 )) || { printf '%s unexpectedly passed\n' "$mode" >&2; return 1; }
  if [[ "$mode" == clock-failure ]]; then
    [[ "$(<"$fixture/reads")" == 1 && "$output" == *'.clock'* ]]
  else
    [[ "$output" == *'.deadline'* ]]
  fi
)
assert "VIP convergence: a fractional deadline overrun cannot report success" \
  fractional_vip_deadline_fixture deadline
assert "VIP convergence: five rounded seconds are not five elapsed seconds" \
  fractional_vip_deadline_fixture short-window
assert "VIP convergence: an unreadable clock fails without another snapshot" \
  fractional_vip_deadline_fixture clock-failure

cascading_post_activation_handoff_fixture() (
  local fixture_mode=c4-handoff fixture fixture_activations=0 orphan_checks=0
  local body output _PASS=1 ip index
  local -a killed=() restarted=()
  vip_convergence_fixture_boundaries
  body="$(sed '/^source /d' "${HERE}/scenarios/C4_cascading_failures.sh")"
  eval "$(declare -f wait_until | sed '1s/wait_until/convergence_original_wait_until/')"
  wait_until() {
    if [[ "$2" == all_vips_uniquely_held ]]; then
      [[ "$1" == 90 ]] || return 1
      orphan_checks=$((orphan_checks+1))
    fi
    convergence_original_wait_until "$@"
  }
  scenario_start() { :; }; scenario_end() { :; }; evid() { :; }
  check() {
    local description="$1"; shift
    if "$@"; then :; else _PASS=0; printf 'failed check: %s\n' "$description"; fi
  }
  kafd_kill() { killed+=("$1"); }; kafd_start() { restarted+=("$1"); }
  node_active() { :; }; vips_uniquely_served_by_live() { :; }
  all_daemons_active() { :; }; all_vips_pingable() { :; }; single_agreed_leader() { :; }
  wait_for_startup_activation() {
    [[ "$1" == 30 && "$2" == "${NODE_IPS[$((fixture_activations % 3))]}" ]] || return 1
    (( orphan_checks == fixture_activations+1 )) || return 1
    fixture_activations=$((fixture_activations+1))
    if (( fixture_activations == 6 )); then printf '0\n' > "$fixture/clock"; fi
  }
  eval "$body" > "$fixture/output" 2>&1 || return 1
  output="$(<"$fixture/output")"
  [[ "${#killed[@]}" == 6 && "${#restarted[@]}" == 6 &&
    "$fixture_activations" == 6 && "$orphan_checks" == 6 ]] || return 1
  for index in "${!killed[@]}"; do
    ip="${NODE_IPS[$((index % 3))]}"
    [[ "${killed[$index]}" == "$ip" && "${restarted[$index]}" == "$ip" ]] || return 1
  done
  [[ "$_PASS" == 1 ]] || {
    printf 'C4 result=%s kills=%s restarts=%s activations=%s orphan_checks=%s\n%s\n' \
      "$_PASS" "${#killed[@]}" "${#restarted[@]}" "$fixture_activations" "$orphan_checks" "$output" >&2
    return 1
  }
)
assert "C4 convergence: six rounds and orphan checks permit a late post-activation handoff" \
  cascading_post_activation_handoff_fixture
