#!/usr/bin/env bash
# Sourced by self-test.sh; every remote and timing boundary is replaced locally.
scenario_evidence_fixture() (
  local scenario="${1:?}" fixture_mode="${2:?}" fixture body status=0
  fixture="$(mktemp -d)"
  trap 'rm -rf "${fixture}"' EXIT
  body="$(sed '/^source /d' "${HERE}/scenarios/${scenario}.sh")"
  NODE_IPS=(192.0.2.1 192.0.2.2 192.0.2.3)
  VIPS=(198.51.100.1 198.51.100.2 198.51.100.3)
  IFACE=eth0
  local fixture_boot=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  local fixture_old=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb fixture_new=cccccccccccccccccccccccccccccccc
  local restarted=0 result=0 clock=0
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
    local fixture_inv="${fixture_old}"
    [[ "$scenario" == A5* ]] && fixture_inv="${fixture_new}"
    [[ "$fixture_mode" == old-invocation ]] && fixture_inv=dddddddddddddddddddddddddddddddd
    python3 -c 'import json,sys; print(json.dumps(dict(
        _BOOT_ID=sys.argv[1], _SYSTEMD_INVOCATION_ID=sys.argv[2],
        __MONOTONIC_TIMESTAMP="200", MESSAGE=sys.argv[3])))' "${fixture_boot}" "${fixture_inv}" "$1"
  }
  emit_cycle() {
    local index="$1"
    emit_record "INFO openraft::core::raft_core: sm::StateMachine command done: BuildSnapshotDone: {snapshot_id: snapshot-T1-N1.${index}, last_log:T1-N1.${index}, last_membership: {}}"
    emit_record "INFO openraft::engine::handler::log_handler: purge log, last_purged: None, purge_upto: T1-N1.$((index-1000))"
  }
  node_sh() {
    if [[ "$2" == journalctl* ]]; then
      case "$scenario" in
        A5*)
          if [[ "$fixture_mode" == cleanup-error ]]; then
            emit_record 'ERROR startup_cleanup: spawn ip del: failure'
          else emit_record "INFO keepafloatd::vip: startup_cleanup: reclaimed orphan ${VIPS[2]}/32 on eth0"; fi;;
        D7*)
          if [[ "$fixture_mode" == generic-epoch ]]; then emit_record 'WARN unrelated epoch message'
          else emit_record 'ERROR keepafloatd::raft: stale cluster incarnation confirmed; shutting down safely before rejoining with fresh state'; fi;;
        D19*)
          local target="${VIPS[0]}"
          [[ "$fixture_mode" == unrelated-unbind ]] && target="${VIPS[2]}"
          emit_record "INFO keepafloatd::vip: unbound ${target}/32 on eth0"
          emit_record 'ERROR keepafloatd::raft: cluster configuration mismatch confirmed; shutting down safely'
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
  kafd_start() { [[ "$1" != "${NODE_IPS[0]}" ]] || restarted=1; }
  partition_node() { :; }; heal_node() { restarted=1; }
  all_vips_uniquely_held() { [[ "$fixture_mode" != duplicate-vip ]]; }
  no_vip_is_duplicate() { :; }; all_vips_pingable() { :; }; all_daemons_active() { :; }
  node_lacks_all_vips() { :; }; single_agreed_leader() { :; }
  wait_for_live_service_without() { :; }; wait_for_available_cluster() { :; }
  backup_cluster_configs() { :; }; restore_cluster_configs() { :; }; clean_reform() { :; }
  instance_for_ip() { printf 'fixture\n'; }
  nodes_except() { printf '%s\n' "${NODE_IPS[1]} ${NODE_IPS[2]}"; }
  next_behavior_changing_stale_secs() { echo 4; }; journal_event_count() { echo 0; }
  clear_health_sentinels() { :; }; configure_sentinel_health() { :; }; wait_for_even() { :; }
  (eval "${body}") >/dev/null 2>&1 || status=$?
  if [[ "$fixture_mode" == good ]]; then [[ "$status" == 0 ]]; else [[ "$status" != 0 ]]; fi
)

for scenario in A5_startup_cleanup D7_stale_survivor D19_config_identity C2_endurance_snapshots; do
  for mode in good old-invocation partial-transport; do
    assert "${scenario} scoped evidence: ${mode}" scenario_evidence_fixture "${scenario}" "${mode}"
  done
done
for mode in same-process boot-change cleanup-error; do
  assert "A5 rejects ${mode}" scenario_evidence_fixture A5_startup_cleanup "${mode}"
done
for mode in same-process boot-change generic-epoch; do
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
