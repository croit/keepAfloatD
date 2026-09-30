#!/usr/bin/env bash
# Fast, local regression checks for failure handling in the SSH harness.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh"

failures=0
assert() {
  local description="${1:?}"; shift
  if "$@"; then
    printf 'ok - %s\n' "${description}"
  else
    printf 'not ok - %s\n' "${description}" >&2
    failures=$((failures + 1))
  fi
}

# shellcheck source=safety-self-test.sh
source "${HERE}/safety-self-test.sh"
# shellcheck source=evidence-self-test.sh
source "${HERE}/evidence-self-test.sh"

arp_failover_observation_never_sends_an_active_probe() (
  local calls result=0 body
  calls=$(mktemp)
  body=$(sed '/^source /d' "${HERE}/scenarios/A2_arp.sh")
  (
    stopped=0
    test_failed=0
    scenario_start() { :; }
    scenario_end() { exit "$test_failed"; }
    evid() { :; }
    check() { shift; "$@" || test_failed=1; }
    check_eq() { [[ "$2" == "$3" ]] || test_failed=1; }
    wait_until() { shift; "$@"; }
    holds_for() { shift; "$@"; }
    kafd_stop() { stopped=1; }
    kafd_start() { :; }
    node_sh() {
      if [[ "$1" == "${NODE_IPS[0]}" ]]; then
        printf '02:00:00:00:00:01\n'
      else
        printf '02:00:00:00:00:02\n'
      fi
    }
    mgmt_sh() {
      if (( stopped )) && [[ "$*" == *'ping '* ]]; then
        printf 'active probe\n' >> "$calls"
      fi
      if (( stopped )); then printf '02:00:00:00:00:02\n'
      else printf '02:00:00:00:00:01\n'; fi
    }
    snapshot_vips() {
      _SNAP_VIPS_ON=()
      for node in "${NODE_IPS[@]}"; do _SNAP_VIPS_ON["$node"]=" "; done
      if (( stopped )); then
        _SNAP_VIPS_ON["${NODE_IPS[1]}"]=" ${VIPS[*]} "
      else
        _SNAP_VIPS_ON["${NODE_IPS[0]}"]=" ${VIPS[*]} "
      fi
    }
    eval "$body"
  ) >/dev/null 2>&1 || result=1
  [[ ! -s "$calls" ]] || result=1
  rm -f "$calls"
  return "$result"
)

inner_ssh_ignores_disposable_node_host_keys() {
  [[ " ${NODE_SSH_OPTS} " == *" -o UserKnownHostsFile=/dev/null "* ]] &&
    [[ " ${NODE_SSH_OPTS} " == *" -o GlobalKnownHostsFile=/dev/null "* ]]
}

secret_read_failure_never_attempts_a_write() {
  local calls result=0
  calls="$(mktemp)"
  REALCLUSTER_SECRET=""
  REALCLUSTER_SECRET_CHANGED=0
  node_sh() {
    if [[ "$*" == *"sed -i"* ]]; then
      printf 'write\n' >> "${calls}"
    fi
    return 255
  }

  prepare_cluster_secret >/dev/null 2>&1 && result=1
  [[ ! -s "${calls}" && -z "${REALCLUSTER_SECRET}" ]] || result=1
  rm -f "${calls}"
  return "${result}"
}

secret_audit_is_read_only_and_rejects_mismatch() {
  local calls mode=matching result=0
  calls="$(mktemp)"
  node_sh() {
    local ip="${1:?}"
    shift
    if [[ "$*" == *"sed -i"* ]]; then
      printf 'write\n' >> "${calls}"
    fi
    if [[ "${mode}" == "mismatch" && "${ip}" == "${NODE_IPS[2]}" ]]; then
      printf 'different-secret-123456\n'
    else
      printf 'matching-secret-123456\n'
    fi
  }

  assert_cluster_secret_consistent >/dev/null 2>&1 || result=1
  mode=mismatch
  assert_cluster_secret_consistent >/dev/null 2>&1 && result=1
  [[ ! -s "${calls}" ]] || result=1
  rm -f "${calls}"
  return "${result}"
}

live_holder_check_rejects_survivor_duplicates() (
  VIPS=(192.0.2.10 192.0.2.11 192.0.2.12)
  snapshot_vips() {
    _SNAP_VIPS_ON["${NODE_IPS[0]}"]=" ${VIPS[0]} ${VIPS[1]} ${VIPS[2]} "
    _SNAP_VIPS_ON["${NODE_IPS[1]}"]=" ${VIPS[0]} "
    _SNAP_VIPS_ON["${NODE_IPS[2]}"]=" "
  }

  ! vips_uniquely_served_by_live "${NODE_IPS[2]}" || return 1
  snapshot_vips() {
    _SNAP_VIPS_ON["${NODE_IPS[0]}"]=" ${VIPS[0]} ${VIPS[1]} ${VIPS[2]} "
    _SNAP_VIPS_ON["${NODE_IPS[1]}"]=" "
    _SNAP_VIPS_ON["${NODE_IPS[2]}"]=" "
  }
  vips_uniquely_served_by_live "${NODE_IPS[2]}"
)

live_service_accepts_safe_skew_and_rechecks_after_reachability() (
  local calls=0
  node_lacks_all_vips() { return 0; }
  all_vips_uniquely_held() {
    calls=$((calls + 1))
    return 0
  }
  all_vips_pingable() { return 0; }

  live_service_without dead && [[ "${calls}" -eq 2 ]]
)

nopreempt_fault_scenarios_do_not_require_even_redistribution() {
  local name scenario
  for name in \
    A1_ip_bind_unbind B1_rgw_failover_s3 D2_consensus_self_fence \
    D6_leader_kill D7_stale_survivor D9_cluster_secret \
    D15_mixed_version_activation D19_config_identity D22_startup_endpoint_failures
  do
    scenario="${HERE}/scenarios/${name}.sh"
    grep -q 'wait_for_live_service_without\|wait_for_available_cluster' "${scenario}" || return 1
    ! grep -q 'wait_for_even\|even_over_nodes' "${scenario}" || return 1
  done
}

cold_reform_and_leader_rejoin_use_their_exact_oracles() {
  local d6="${HERE}/scenarios/D6_leader_kill.sh"
  local d8="${HERE}/scenarios/D8_full_outage_recovery.sh"

  grep -Fq 'wait_for_available_cluster' "${d6}" &&
    grep -Fq 'wait_for_even 90 "${NODE_IPS[0]}" "${NODE_IPS[1]}"' "${d8}" &&
    grep -Fq 'wait_for_available_cluster' "${d8}"
}

no_duplicate_check_allows_a_safe_handoff_gap() (
  VIPS=(192.0.2.10 192.0.2.11 192.0.2.12)
  snapshot_vips() {
    _SNAP_VIPS_ON["${NODE_IPS[0]}"]=" ${VIPS[0]} "
    _SNAP_VIPS_ON["${NODE_IPS[1]}"]=" "
    _SNAP_VIPS_ON["${NODE_IPS[2]}"]=" ${VIPS[2]} "
  }
  no_vip_is_duplicate || return 1

  snapshot_vips() {
    _SNAP_VIPS_ON["${NODE_IPS[0]}"]=" ${VIPS[0]} ${VIPS[1]} "
    _SNAP_VIPS_ON["${NODE_IPS[1]}"]=" ${VIPS[1]} "
    _SNAP_VIPS_ON["${NODE_IPS[2]}"]=" ${VIPS[2]} "
  }
  ! no_vip_is_duplicate
)

behavior_changing_stale_window_skips_equivalent_raw_values() {
  [[ "$(next_behavior_changing_stale_secs 6000 6)" == "12" ]] \
    && [[ "$(next_behavior_changing_stale_secs 500 5)" == "6" ]]
}

behaviorally_equivalent_stale_window_reports_when_none_exists() {
  [[ "$(next_behaviorally_equivalent_stale_secs 2500 3)" == "4" ]] \
    && ! next_behaviorally_equivalent_stale_secs 1000 3 >/dev/null
}

arbitrary_interface_holder_query_is_batched() {
  node_sh() {
    local ip="${1:?}"
    if [[ "${ip}" == "${NODE_IPS[1]}" ]]; then printf '1\n'; else printf '0\n'; fi
  }

  [[ "$(ipv4_holder_on_interface 198.51.100.10 32 test0.200)" == "${NODE_IPS[1]}" ]]
}

rgw_discovery_does_not_assume_a_cluster_specific_daemon_name() {
  ! grep -q 'ceph-rgw\.ceph-[0-9]' "${HERE}/lib.sh"
}

unreadable_node_invalidates_unique_holder_evidence() (
  VIPS=(192.0.2.10)
  node_sh() {
    local ip="${1:?}"
    case "${ip}" in
      "${NODE_IPS[0]}") return 255 ;;
      "${NODE_IPS[1]}") printf '%s ' "${VIPS[0]}" ;;
      *) return 0 ;;
    esac
  }

  ! all_vips_uniquely_held
)

latest_leader_none_invalidates_old_some_evidence() {
  kafd_active() { printf 'active\n'; }
  node_sh() {
    printf '%s\n' \
      'raft current leader is now Some(7)' \
      'raft current leader is now None'
  }

  ! single_agreed_leader
}

current_configured_leader_requires_a_majority() (
  local leader="${NODE_RAFT_IDS[0]}"
  kafd_active() { printf 'active\n'; }
  node_sh() {
    local ip="${1:?}"
    if [[ "${ip}" == "${NODE_IPS[2]}" ]]; then
      printf 'raft current leader is now None\n'
    else
      printf 'raft current leader is now Some(%s)\n' "${leader}"
    fi
  }

  [[ "$(cluster_leader_id)" == "${leader}" ]]
)

inactive_nodes_cannot_supply_leader_evidence() (
  local leader="${NODE_RAFT_IDS[0]}"
  kafd_active() { printf 'inactive\n'; }
  node_sh() { printf 'raft current leader is now Some(%s)\n' "${leader}"; }

  ! cluster_leader_id
)

malformed_address_count_evidence_is_rejected() {
  node_sh() { printf 'not-a-count\n'; }

  ! ipv4_bound_count 198.51.100.10 32 test0
}

s3_fixture_setup_cannot_ignore_backend_failure() (
  _s3() { return 1; }

  ! s3_ensure_bucket 198.51.100.10
)

real_service_failure_controls_the_probed_frontend_and_backend() (
  local observed unhealthy healthy
  node_sh() { shift; observed="$*"; }

  set_unhealthy "${NODE_IPS[0]}" || return 1
  unhealthy="${observed}"
  [[ "${unhealthy}" == *"systemctl stop haproxy"* ]] || return 1
  [[ "${unhealthy}" == *"systemctl stop \"\$unit\""* ]] || return 1

  set_healthy "${NODE_IPS[0]}" || return 1
  healthy="${observed}"
  [[ "${healthy}" == *"systemctl start \"\$unit\""* ]] &&
    [[ "${healthy}" == *"systemctl start haproxy"* ]] &&
    [[ "${healthy}" == *"systemctl is-active --quiet haproxy"* ]]
)

real_service_recovery_propagates_backend_start_failure() (
  node_sh() {
    shift
    find() { printf '%s\n' '/var/lib/ceph/radosgw/ceph-rgw.fixture'; }
    pkill() { return 0; }
    systemctl() {
      [[ "$1" == start && "$2" == ceph-radosgw@rgw.fixture ]] && return 1
      return 0
    }
    export -f find pkill systemctl
    bash -c "$*"
  }

  ! set_healthy "${NODE_IPS[0]}"
)

real_service_failure_propagates_frontend_stop_failure() (
  node_sh() {
    shift
    find() { printf '%s\n' '/var/lib/ceph/radosgw/ceph-rgw.fixture'; }
    systemctl() {
      [[ "$1" == stop && "$2" == haproxy ]] && return 1
      return 0
    }
    export -f find systemctl
    bash -c "$*"
  }

  ! set_unhealthy "${NODE_IPS[0]}"
)

post_rgw_stability_requires_the_real_data_path() (
  all_daemons_active() { return 0; }
  all_nodes_agree_on_leader() { return 0; }
  all_vips_uniquely_held() { return 0; }
  all_service_paths_healthy() { return 1; }

  ! post_rgw_stable
)

post_rgw_stability_requires_every_node_in_the_cluster() (
  all_daemons_active() { return 0; }
  all_nodes_agree_on_leader() { return 1; }
  all_vips_uniquely_held() { return 0; }
  all_service_paths_healthy() { return 0; }

  ! post_rgw_stable
)

service_path_probes_the_rgw_backend_itself() {
  local implementation
  implementation="$(declare -f service_path_healthy)"
  [[ "${implementation}" == *"127.0.0.1:80/"* ]] &&
    [[ "${implementation}" == *"radosgw"* ]]
}

campaign_timeout_terminates_the_scenario_process_group() {
  local guard="${HERE}/run-all-guard.sh" runner="${HERE}/run-all.sh"
  grep -q 'RUNALL_SCENARIO_PGID' "${guard}" &&
    grep -q 'stop_active_scenario_group' "${guard}" &&
    grep -q 'RUNALL_SCENARIO_PGID=.*!' "${runner}" &&
    grep -q 'wait .*RUNALL_SCENARIO_PGID' "${runner}"
}

campaign_group_stop_waits_for_descendant_exit() (
  source "${HERE}/run-all-guard.sh"
  RUNALL_SCENARIO_PGID=12345
  RUNALL_SCENARIO_STOP_GRACE=0
  local killed=0 probes=0 sleeps=0
  kill() {
    case "$1" in
      -KILL) killed=1; return 0 ;;
      -TERM) return 0 ;;
      -0)
        (( killed )) || return 0
        probes=$((probes + 1))
        (( probes < 3 ))
        ;;
      *) return 2 ;;
    esac
  }
  wait() { return 137; }
  sleep() { sleeps=$((sleeps + 1)); }
  stop_active_scenario_group && [[ -z "${RUNALL_SCENARIO_PGID}" ]] &&
    (( probes == 3 && sleeps == 2 ))
)

campaign_group_stop_rejects_a_surviving_group() (
  source "${HERE}/run-all-guard.sh"
  RUNALL_SCENARIO_PGID=12345
  RUNALL_SCENARIO_STOP_GRACE=0
  local sleeps=0
  kill() { return 0; }
  wait() { return 137; }
  sleep() { sleeps=$((sleeps + 1)); }
  ! stop_active_scenario_group && [[ "${RUNALL_SCENARIO_PGID}" == 12345 ]] &&
    (( sleeps > 0 && sleeps <= 20 ))
)

campaign_group_stop_kills_a_term_ignoring_descendant() (
  source "${HERE}/run-all-guard.sh"
  local marker child deadline process_stat process_pgid
  marker="$(mktemp)"
  timeout 30 bash -c '
    trap "" TERM
    (trap "" TERM; while true; do sleep 1; done) &
    printf "%s\n" "$!" > "$1"
    wait
  ' _ "${marker}" &
  RUNALL_SCENARIO_PGID=$!
  deadline=$((SECONDS + 3))
  while [[ ! -s "${marker}" && ${SECONDS} -lt ${deadline} ]]; do sleep 0.1; done
  [[ -s "${marker}" ]] || { rm -f "${marker}"; return 1; }
  child="$(<"${marker}")"
  IFS= read -r process_stat < "/proc/${RUNALL_SCENARIO_PGID}/stat" || {
    rm -f "${marker}"
    return 1
  }
  process_stat="${process_stat##*) }"
  read -r _ _ process_pgid _ <<< "${process_stat}"
  [[ "${process_pgid}" == "${RUNALL_SCENARIO_PGID}" ]] || {
    rm -f "${marker}"
    return 1
  }
  RUNALL_SCENARIO_STOP_GRACE=1
  stop_active_scenario_group || { rm -f "${marker}"; return 1; }
  deadline=$((SECONDS + 3))
  while kill -0 "${child}" 2>/dev/null && (( SECONDS < deadline )); do sleep 0.1; done
  rm -f "${marker}"
  ! kill -0 "${child}" 2>/dev/null
)

final_audit_captures_mutation_queries_before_testing_absence() {
  local audit="${HERE}/final-audit.sh"
  grep -q 'partition_rules_absent' "${audit}" &&
    grep -q 'campaign_config_backups_absent' "${audit}" &&
    grep -q 'campaign_runtime_artifacts_absent' "${audit}" &&
    grep -q 'ownership_test_artifacts_absent' "${audit}" &&
    grep -q "grep -Fq '127.0.0.1:9400/healthz'" "${audit}" &&
    ! grep -q '! iptables .*|' "${audit}"
}

central_cleanup_covers_vlan_and_cidr_artifacts() {
  local implementation audit
  implementation="$(declare -f cleanup_ownership_test_artifacts)"
  audit="$(declare -f ownership_markers_absent)"
  [[ "${implementation}" == *'VLAN_TEST_VIP'* ]] &&
    [[ "${implementation}" == *'CIDR_TEST_VIP'* ]] &&
    [[ "${implementation}" == *'ip link del'* ]] &&
    [[ "${audit}" == *'VLAN_TEST_VIP'* ]] &&
    [[ "${audit}" == *'CIDR_TEST_VIP'* ]]
}

b3_rejects_notify_states_on_the_wrong_node() {
  local scenario="${HERE}/scenarios/B3_notify_hook.sh"
  grep -q 'FAULT notifications outside' "${scenario}" &&
    grep -q 'BACKUP notifications outside' "${scenario}" &&
    grep -q 'MASTER notifications outside' "${scenario}" &&
    grep -q 'MASTER without local VIP' "${scenario}"
}

d17_holds_liveness_with_assignment_invariants() {
  local scenario="${HERE}/scenarios/D17_nopreempt_restart_leader_churn.sh"
  grep -q 'recovered_restart_stable' "${scenario}" &&
    grep -q 'leader_restart_stable' "${scenario}" &&
    grep -q 'holds_for 8 recovered_restart_stable' "${scenario}" &&
    grep -q 'holds_for 8 leader_restart_stable' "${scenario}" &&
    [[ "$(grep -c 'all_nodes_agree_on_leader' "${scenario}")" -ge 2 ]]
}

missing_config_backup_is_a_restore_failure() (
  local fixture command
  fixture="$(mktemp)"
  printf 'current\n' > "${fixture}"
  NODE_IPS=(fixture-node)
  instance_for_ip() { printf 'fixture\n'; }
  node_sh() {
    shift
    command="$*"
    command="${command//\/etc\/keepafloatd\/config-fixture.yaml/${fixture}}"
    bash -c "${command}"
  }

  ! restore_cluster_configs selftest
  local result=$?
  rm -f "${fixture}" "${fixture}.selftest-bak"
  return "${result}"
)

config_restore_fixture() {
  fixture="$(mktemp -d)" || exit 1
  trap 'rm -rf "${fixture}"' EXIT
  restore_fault=none
  NODE_IPS=(first second third)
  NODE_INSTANCES=(first second third)
  for node in "${NODE_IPS[@]}"; do
    printf 'changed-%s\n' "${node}" > "${fixture}/config-${node}.yaml"
    printf 'original-%s\n' "${node}" > "${fixture}/config-${node}.yaml.selftest-bak"
    printf 'operator-%s\n' "${node}" > "${fixture}/config-${node}.yaml.operator-bak"
  done
  instance_for_ip() {
    [[ "${restore_fault}:$1" != mapping:second ]] || return 1
    printf '%s\n' "$1"
  }
  node_sh() {
    local command="$2" quoted_fixture
    printf '%s\n' "$1" >> "${fixture}/calls"
    [[ "${restore_fault}:$1" != transport:second ]] || return 255
    [[ "${command}" == 'test -f '* ]] || return 64
    quoted_fixture="$(shell_quote "${fixture}")"
    command="${command//\/etc\/keepafloatd/"${quoted_fixture}"}"
    if [[ "${restore_fault}:$1" == move:second ]]; then
      command="mv() { return 17; }; ${command}"
    fi
    bash -c "${command}"
  }
}

config_restore_checks_files() {
  local unrestored="${1-}" node
  for node in "${NODE_IPS[@]}"; do
    [[ "$(<"${fixture}/config-${node}.yaml.operator-bak")" == "operator-${node}" ]] || return 1
    if [[ "${node}" == "${unrestored}" ]]; then
      [[ "$(<"${fixture}/config-${node}.yaml")" == "changed-${node}" ]] || return 1
    else
      if [[ "$(<"${fixture}/config-${node}.yaml")" != "original-${node}" ]]; then
        printf 'config restore left %s modified\n' "${node}" >&2
        return 1
      fi
      [[ ! -e "${fixture}/config-${node}.yaml.selftest-bak" ]] || return 1
    fi
  done
}

config_restore_fixture_aborts_without_a_temp_directory() (
  local body output status=0
  body="$(declare -f config_restore_fixture)"
  # Stop before file writes if the fixture continues after the injected allocation failure.
  body+=$'\nmktemp() { return 1; }\ntrap() { printf "unsafe continuation\\n"; exit 42; }\nconfig_restore_fixture'
  output="$(timeout 5 bash -c "${body}")" || status=$?
  [[ "${status}" -eq 1 && -z "${output}" ]]
)

config_restore_fixture_supports_spaced_paths() (
  local scratch
  scratch="$(mktemp -d)" || exit 1
  trap 'rm -rf "${scratch}"' EXIT
  mkdir "${scratch}/with space" || exit 1
  export TMPDIR="${scratch}/with space"
  config_restore_succeeds_for_every_node
)

config_restore_succeeds_for_every_node() (
  local fixture node restore_fault
  config_restore_fixture
  restore_cluster_configs selftest >"${fixture}/out" 2>"${fixture}/err" || return 1
  config_restore_checks_files && [[ ! -s "${fixture}/err" &&
    "$(<"${fixture}/calls")" == $'first\nsecond\nthird' ]]
)

config_restore_continues_after_failure() (
  local fixture node restore_fault mode="${1:?}" expected_calls=$'first\nsecond\nthird'
  config_restore_fixture
  restore_fault="${mode}"
  if [[ "${mode}" == missing ]]; then rm "${fixture}/config-second.yaml.selftest-bak"; fi
  ! restore_cluster_configs selftest >"${fixture}/out" 2>"${fixture}/err" || return 1
  config_restore_checks_files second || return 1
  [[ "${mode}" != mapping ]] || expected_calls=$'first\nthird'
  [[ "$(<"${fixture}/calls")" == "${expected_calls}" ]] || return 1
  grep -q 'config restore failed on second' "${fixture}/err" || return 1
  if [[ "${mode}" != missing ]]; then
    [[ "$(<"${fixture}/config-second.yaml.selftest-bak")" == original-second ]] || return 1
  fi
)

config_restore_retry_reaches_unrestored_nodes() (
  local fixture node restore_fault
  config_restore_fixture
  restore_fault=transport
  ! restore_cluster_configs selftest >"${fixture}/out" 2>"${fixture}/first-error" || return 1
  restore_fault=none
  : > "${fixture}/calls"
  ! restore_cluster_configs selftest >"${fixture}/out" 2>"${fixture}/retry-error" || return 1
  config_restore_checks_files || return 1
  [[ "$(<"${fixture}/calls")" == $'first\nsecond\nthird' ]] || return 1
  grep -q 'config restore failed on first' "${fixture}/retry-error" &&
    grep -q 'config restore failed on third' "${fixture}/retry-error" &&
    [[ "$(grep -c 'config restore failed' "${fixture}/retry-error")" -eq 2 ]]
)

config_restore_continues_under_errexit() (
  local fixture node restore_fault body status=0 mode="${1:?}"
  config_restore_fixture
  restore_fault="${mode}"
  export fixture restore_fault
  body="$(declare -f restore_cluster_configs instance_for_ip node_sh shell_quote)"
  body+=$'\nset -Eeuo pipefail\nNODE_IPS=(first second third)\nrestore_cluster_configs selftest\nprintf "unexpected success\\n"'
  timeout 5 bash -c "${body}" >"${fixture}/out" 2>"${fixture}/err" || status=$?
  [[ "${status}" -eq 1 && ! -s "${fixture}/out" ]] || return 1
  config_restore_checks_files second && grep -q 'config restore failed on second' "${fixture}/err"
)

config_restore_rejects_invalid_tags_without_effects() (
  local fixture node restore_fault tag
  config_restore_fixture
  for tag in '../other' 'bad;command' 'has space'; do
    ! restore_cluster_configs "${tag}" || return 1
  done
  [[ ! -e "${fixture}/calls" ]] || return 1
  for node in "${NODE_IPS[@]}"; do
    [[ "$(<"${fixture}/config-${node}.yaml")" == "changed-${node}" &&
      "$(<"${fixture}/config-${node}.yaml.selftest-bak")" == "original-${node}" ]] || return 1
  done
)

campaign_guard_requires_exact_backups_and_hash_verification() {
  local guard="${HERE}/run-all-guard.sh"
  grep -q 'test -f .*runall-guard' "${guard}" &&
    grep -q 'sha256sum' "${guard}" &&
    ! grep -q 'if test -f .*runall-guard; then mv' "${guard}"
}

guard_hash_verification_propagates_remote_failure() (
  source "${HERE}/run-all-guard.sh"
  NODE_IPS=(fixture-node)
  RUNALL_GUARD_COMPLETE=1
  RUNALL_GUARD_CAPTURED[fixture-node]=1
  RUNALL_CONFIG_HASH[fixture-node]="$(printf 'a%.0s' {1..64})"
  RUNALL_BINARY_HASH[fixture-node]="$(printf 'b%.0s' {1..64})"
  instance_for_ip() { printf 'fixture\n'; }
  node_sh() {
    printf '%s %s\n' \
      "${RUNALL_CONFIG_HASH[fixture-node]}" "${RUNALL_BINARY_HASH[fixture-node]}"
    return 17
  }

  ! verify_restored_guard_hashes
)

campaign_cleanup_preserves_unknown_config_backups() {
  local guard="${HERE}/run-all-guard.sh"
  local audit="${HERE}/final-audit.sh"
  local implementation
  implementation="$(declare -f backup_cluster_configs)"

  ! grep -q "config-.*yaml.\\*-bak.*-delete" "${guard}" &&
    grep -q 'REALCLUSTER_CAMPAIGN_BACKUP_TAGS' "${guard}" &&
    grep -q 'campaign_config_backups_absent' "${audit}" &&
    ! grep -q -- "-name '\*.bak'" "${audit}" &&
    [[ "${implementation}" == *'test ! -e'* ]]
}

campaign_guard_rejects_every_artifact_it_removes() {
  local guard="${HERE}/run-all-guard.sh"
  local capture
  capture="$(sed -n '/^capture_scenario_guard()/,/^}/p' "${guard}")"

  [[ "${capture}" == *'campaign_runtime_artifacts_absent'* ]] &&
    [[ "${capture}" == *'ownership_test_artifacts_absent'* ]] &&
    [[ "${capture}" == *'partition_rules_absent'* ]]
}

partition_cleanup_deletes_only_harness_rules() {
  local partition heal d10
  partition="$(declare -f partition_node)"
  heal="$(declare -f heal_node)"
  d10="${HERE}/scenarios/D10_asymmetric_partition.sh"

  [[ "${partition}" == *'keepafloatd-realcluster'* ]] &&
    [[ "${heal}" == *'keepafloatd-realcluster'* ]] &&
    [[ "${heal}" != *'iptables -F'* ]] &&
    grep -q 'partition_input_from' "${d10}" &&
    grep -q 'heal_node' "${d10}" &&
    ! grep -q 'iptables -F' "${d10}"
}

mixed_version_deploy_checks_the_running_process() {
  local scenario body implementation
  implementation="$(declare -f running_keepafloatd_buildid; \
    declare -f replace_keepafloatd_binary)"
  [[ "${implementation}" == *'set -euo pipefail'* ]] || return 1
  [[ "${implementation}" == *'MainPID'* ]] || return 1
  [[ "${implementation}" == *'/proc/'*'/exe'* ]] || return 1
  [[ "${implementation}" == *'systemctl is-active --quiet'* ]] || return 1
  [[ "${implementation}" == *'reset-failed'*'|| true'* ]] || return 1
  for scenario in \
    "${HERE}/scenarios/D15_mixed_version_activation.sh" \
    "${HERE}/scenarios/D16_chained_legacy_activation.sh"
  do
    body="$(sed -n '/^binary_buildid()/,/^}/p; /^deploy_binary()/,/^}/p' "${scenario}")"
    [[ "${body}" == *'running_keepafloatd_buildid'* ]] || return 1
    [[ "${body}" == *'replace_keepafloatd_binary'* ]] || return 1
  done
}

campaign_timeout_cleanup_is_signal_safe() {
  grep -q -- '--kill-after' "${HERE}/run-all.sh" &&
    grep -q "trap .* INT" "${HERE}/scenario.sh" &&
    grep -q "trap .* TERM" "${HERE}/scenario.sh"
}

ownership_cleanup_and_audit_are_fail_closed() {
  local d23="${HERE}/scenarios/D23_crash_removed_vip_cleanup.sh"
  local d24="${HERE}/scenarios/D24_ipv6_crash_removed_vip_cleanup.sh"
  grep -q 'ownership_markers_absent' "${HERE}/final-audit.sh" &&
    ! sed -n '/^remove_disposable_addresses()/,/^}/p' "${d23}" | grep -q '|| true' &&
    ! sed -n '/^remove_disposable_ipv6()/,/^}/p' "${d24}" | grep -q '|| true'
}

b3_notify_checks_are_fail_closed_and_stable() {
  local scenario="${HERE}/scenarios/B3_notify_hook.sh"
  grep -q 'set -euo pipefail' "${scenario}" &&
    grep -q 'wait_for_even 60' "${scenario}" &&
    grep -q 'nodes_except "${fault_ip}"' "${scenario}" &&
    grep -q 'unexpected notify state' "${scenario}" &&
    ! sed -n '/evid "installing notify script/,/^done$/p' "${scenario}" | grep -q '  true'
}

b3_nopreempt_cleanup_requires_availability_not_even_spread() {
  local scenario="${HERE}/scenarios/B3_notify_hook.sh" cleanup
  cleanup="$(sed -n '/# Restore the exact nopreempt configs/,/scenario_end/p' "${scenario}")" || return 1
  [[ "${cleanup}" == *'wait_for_available_cluster'* ]] &&
    [[ "${cleanup}" != *'wait_for_even'* ]]
}

soak_uses_even_spread_only_for_a_clean_full_reform() {
  local soak="${HERE}/soak.sh" recovery
  recovery="$(sed -n '/^soak_recovered()/,/^}/p' "${soak}")" || return 1
  [[ "${recovery}" == *'full-reform) wait_for_steady_state'* ]] &&
    [[ "${recovery}" == *'*)           wait_for_available_cluster'* ]]
}

soak_disruption_failures_abort_before_recovery_commands() (
  local soak="${HERE}/soak.sh" body calls="" result=0 cycle=1
  body="$(sed -n '/^run_soak_disruption()/,/^}/p' "${soak}")" || return 1
  [[ -n "${body}" ]] || return 1
  eval "${body}"
  say() { :; }
  sleep() { :; }

  kafd_kill() { return 23; }
  kafd_start() { calls+=" start"; }
  run_soak_disruption sigkill node-a >/dev/null 2>&1 && result=1
  [[ "${calls}" != *" start"* ]] || result=1

  partition_node() { return 24; }
  heal_node() { calls+=" heal"; }
  run_soak_disruption partition node-b >/dev/null 2>&1 && result=1
  [[ "${calls}" != *" heal"* ]] || result=1
  return "${result}"
)

soak_arms_restore_only_after_a_successful_backup() {
  local soak="${HERE}/soak.sh" startup backup_line armed_line secret_line
  startup="$(sed -n '/^say "SOAK START/,/^clear_health_sentinels/p' "${soak}")" || return 1
  backup_line="$(grep -n '^backup_cluster_configs ' <<<"${startup}" | cut -d: -f1)"
  armed_line="$(grep -n '^SOAK_CONFIG_BACKED_UP=1$' <<<"${startup}" | cut -d: -f1)"
  secret_line="$(grep -n '^prepare_cluster_secret ' <<<"${startup}" | cut -d: -f1)"
  [[ "${backup_line}" =~ ^[0-9]+$ && "${armed_line}" =~ ^[0-9]+$ &&
    "${secret_line}" =~ ^[0-9]+$ ]] || return 1
  (( backup_line < armed_line && armed_line < secret_line ))
}

restart_and_no_majority_checks_are_sustained() {
  grep -q 'holds_for 5 rejoined_cluster_stable' \
    "${HERE}/scenarios/D3_silent_death_staleness.sh" &&
    grep -q 'holds_for 5 only_restarted_holder_is_active' \
      "${HERE}/scenarios/D23_crash_removed_vip_cleanup.sh" &&
    grep -q 'holds_for 5 only_restarted_holder_is_active' \
      "${HERE}/scenarios/D24_ipv6_crash_removed_vip_cleanup.sh"
}

realcluster_marker_queries_reject_duplicate_routes() {
  local scenario body
  for scenario in \
    "${HERE}/scenarios/D23_crash_removed_vip_cleanup.sh" \
    "${HERE}/scenarios/D24_ipv6_crash_removed_vip_cleanup.sh"
  do
    body="$(sed -n '/^[a-z0-9_]*_has_marker()/,/^}/p' "${scenario}")" || return 1
    [[ "${body}" == *'ip -N -j'* ]] || return 1
    [[ "${body}" == *'length == 1'* ]] || return 1
  done
}

ssh_commands_have_an_execution_deadline() {
  declare -f mgmt_sh | grep -q 'timeout .*ssh'
}

baseline_restore_retries_a_failed_clean_reform() (
  local checks=0 reforms=0
  heal_all() { return 0; }
  prepare_cluster_secret() { return 0; }
  set_healthy() { return 0; }
  kafd_active() { printf 'active\n'; }
  kafd_start() { return 1; }
  wait_for_steady_state() {
    checks=$((checks + 1))
    [[ "${checks}" -ge 3 ]]
  }
  clean_reform() {
    reforms=$((reforms + 1))
    return 0
  }

  restore_baseline >/dev/null && [[ "${reforms}" -eq 2 && "${checks}" -eq 3 ]]
)

even_wait_rechecks_ownership_after_reachability() (
  local calls=0
  even_over_nodes() {
    calls=$((calls + 1))
    (( calls == 1 ))
  }
  all_vips_pingable() { return 0; }
  assert_unique_holders() { return 0; }
  dump_diag() { :; }
  current_assignments_summary() { printf 'transient\n'; }
  fail() { :; }

  ! wait_for_even 0 "${NODE_IPS[@]}"
)

zero_duration_hold_still_checks_the_predicate() (
  ! holds_for 0 false
)

positive_hold_rechecks_at_the_window_end() (
  local calls=0
  first_observation_only() {
    calls=$((calls + 1))
    (( calls == 1 )) && sleep 2
    (( calls == 1 ))
  }

  ! holds_for 1 first_observation_only
)

count_oracles_do_not_mask_source_failures() (
  local violations
  violations="$(
    grep -RInE 'grep .* -c|grep -[^ ]*c' \
      "${HERE}/scenarios" "${HERE}/soak.sh" "${HERE}/final-audit.sh" || true
  )"
  [[ -z "${violations}" ]]
)

event_counters_distinguish_zero_from_unreadable() (
  local fixture result=0
  fixture="$(mktemp)"
  printf '%s\n' clean MASTER clean > "${fixture}"
  instance_for_ip() { printf 'fixture\n'; }
  node_sh() {
    shift
    journalctl() { printf '%s\n' clean 'push snapshot building command' clean; }
    export -f journalctl
    bash -c "$*"
  }

  [[ "$(journal_event_count fixture-node '2026-09-01 00:00:00 UTC' \
    'push snapshot building command|build_snapshot')" == 1 ]] || result=1
  [[ "$(journal_event_count fixture-node '2026-09-01 00:00:00 UTC' Defensive)" == 0 ]] \
    || result=1
  [[ "$(file_event_count fixture-node "${fixture}" MASTER)" == 1 ]] || result=1
  [[ "$(file_event_count fixture-node "${fixture}" FAULT)" == 0 ]] || result=1
  ! file_event_count fixture-node "${fixture}.missing" MASTER >/dev/null || result=1
  rm -f "${fixture}"
  return "${result}"
)

journal_absence_rejects_an_unreadable_node() (
  NODE_IPS=(node-a node-b node-c)
  journal_event_count() {
    [[ "${1}" != node-b ]] || return 1
    printf '0\n'
  }

  declare -F journal_event_absent_on_all_nodes_since >/dev/null &&
    ! journal_event_absent_on_all_nodes_since now activation
)

journal_readers_propagate_journalctl_failure() (
  instance_for_ip() { printf 'fixture\n'; }
  node_sh() {
    shift
    journalctl() {
      printf '%s\n' 'raft current leader is now Some(17)'
      return 17
    }
    systemctl() { printf '%s\n' fixture-invocation; }
    export -f journalctl systemctl
    bash -c "$*"
  }

  ! kafd_log fixture 1 &&
    ! kafd_log_since fixture now &&
    ! leader_seen_by fixture &&
    ! leader_seen_by_since fixture now
)

leader_readers_are_scoped_to_the_current_service_invocation() {
  local current since
  current="$(declare -f leader_seen_by)"
  since="$(declare -f leader_seen_by_since)"

  [[ "${current}" == *'InvocationID'*'_SYSTEMD_INVOCATION_ID'* ]] &&
    [[ "${since}" == *'InvocationID'*'_SYSTEMD_INVOCATION_ID'* ]]
}

leader_reader_keeps_old_transition_after_heavy_noise() (
  instance_for_ip() { printf 'fixture\n'; }
  node_sh() {
    shift
    journalctl() {
      local argument filtered=0
      for argument in "$@"; do
        [[ "${argument}" == --grep=* ]] && filtered=1
      done
      if [[ "${filtered}" -eq 1 ]]; then
        printf '%s\n' 'raft current leader is now Some(17)'
      else
        # Model journalctl's bounded tail after the sole transition was pushed out by noisy logs.
        printf '%s\n' noise-{1..1200}
      fi
    }
    systemctl() { printf '%s\n' fixture-invocation; }
    export -f journalctl systemctl
    bash -c "$*"
  }

  [[ "$(leader_seen_by fixture)" == 17 ]]
)

d22_proves_failed_startup_has_no_daemon_or_listeners() {
  local scenario="${HERE}/scenarios/D22_startup_endpoint_failures.sh"
  grep -q 'node_process_absent' "${scenario}" &&
    grep -q 'node_tcp_listener_absent' "${scenario}" &&
    grep -q 'node_tcp_listener_owned_only_by_pid' "${scenario}" &&
    grep -q 'holds_for 5 bind_failure_closed' "${scenario}" &&
    grep -q 'holds_for 5 mapped_boundary_closed' "${scenario}"
}

process_absence_rejects_an_observer_error() (
  node_sh() {
    shift
    pgrep() { return 17; }
    export -f pgrep
    bash -c "$*"
  }

  declare -F node_process_absent >/dev/null &&
    ! node_process_absent fixture keepafloatd
)

listener_absence_rejects_an_observer_error() (
  node_sh() {
    shift
    ss() { return 17; }
    export -f ss
    bash -c "$*"
  }

  declare -F node_tcp_listener_absent >/dev/null &&
    ! node_tcp_listener_absent fixture 9210
)

listener_ownership_rejects_an_empty_observation() (
  node_sh() {
    shift
    ss() { return 0; }
    export -f ss
    bash -c "$*"
  }

  ! node_tcp_listener_owned_only_by_pid fixture 9211 12345
)

sentinel_health_replaces_inline_and_block_commands() (
  local fixture result=0 style
  fixture="$(mktemp)"
  NODE_IPS=(fixture-node)
  instance_for_ip() { printf 'fixture\n'; }
  node_sh() {
    shift
    local command="$*"
    command="${command//\/etc\/keepafloatd\/config-fixture.yaml/${fixture}}"
    bash -c "${command}"
  }

  for style in inline block; do
    printf '%s\n' 'health:' > "${fixture}"
    if [[ "${style}" == inline ]]; then
      printf '%s\n' '  command: ["/usr/bin/pgrep", "-x", "radosgw"]' >> "${fixture}"
    else
      printf '%s\n' \
        '  command:' \
        '    - "/usr/bin/pgrep"' \
        '    - "-x"' \
        '    - "radosgw"' >> "${fixture}"
    fi
    printf '%s\n' \
      '  interval_ms: 2000' \
      '  timeout_ms: 5000' \
      '  stale_secs: 10' >> "${fixture}"

    configure_sentinel_health 500 100 3 || result=1
    [[ "$(grep -c '^  command:' "${fixture}")" -eq 1 ]] || result=1
    grep -qx '  command:' "${fixture}" || result=1
    grep -qx '    - "/bin/bash"' "${fixture}" || result=1
    grep -qx '    - "-c"' "${fixture}" || result=1
    grep -qx '    - "test ! -e /run/keepafloatd-unhealthy"' "${fixture}" || result=1
    grep -qx '  interval_ms: 500' "${fixture}" || result=1
    grep -qx '  timeout_ms: 100' "${fixture}" || result=1
    grep -qx '  stale_secs: 3' "${fixture}" || result=1
  done
  rm -f "${fixture}"
  return "${result}"
)

endurance_uses_the_shared_health_mutator() {
  local scenario="${HERE}/scenarios/C2_endurance_snapshots.sh"
  grep -qx 'configure_sentinel_health 50 500 5' "${scenario}" &&
    ! grep -qE 'sed -i .*command:|/bin/true|timeout -k 1 28' "${scenario}"
}

cluster_mutators_reject_a_middle_node_failure() (
  NODE_IPS=(first broken last)
  instance_for_ip() { printf '%s\n' "$1"; }
  node_sh() { [[ "$1" != broken ]]; }
  sentinel_recover() { [[ "$1" != broken ]]; }

  ! backup_cluster_configs selftest &&
    ! restore_cluster_configs selftest &&
    ! configure_sentinel_health 500 100 3 &&
    ! set_cluster_scalar failover_delay_secs 0 &&
    ! clear_health_sentinels
)

d9_parallel_restart_checks_every_child() {
  grep -q 'wait_for_all "${pids\[@\]}"' \
    "${HERE}/scenarios/D9_cluster_secret.sh"
}

hard_kill_verifies_the_service_stayed_down() {
  declare -f kafd_kill | grep -q 'systemctl is-active'
}

steady_state_rechecks_ownership_after_leader_observation() (
  wait_for_even() { return 0; }
  wait_until() { shift; "$@"; }
  all_daemons_active() { return 0; }
  all_nodes_agree_on_leader() { return 0; }
  even_and_pingable_over_nodes() { return 1; }
  dump_diag() { :; }
  fail() { :; }
  log() { :; }

  ! wait_for_steady_state
)

available_cluster_accepts_safe_uneven_ownership() (
  all_daemons_active() { return 0; }
  single_agreed_leader() { return 0; }
  all_nodes_agree_on_leader() { return 0; }
  all_vips_uniquely_held() { return 0; }
  all_vips_pingable() { return 0; }

  available_cluster_ready
)

available_cluster_rejects_an_active_isolated_rejoin() (
  all_daemons_active() { return 0; }
  single_agreed_leader() { return 0; }
  all_nodes_agree_on_leader() { return 1; }
  all_vips_uniquely_held() { return 0; }
  all_vips_pingable() { return 0; }

  ! available_cluster_ready
)

nonpreempt_rejoins_use_availability_not_even_spread() {
  local d15="${HERE}/scenarios/D15_mixed_version_activation.sh"
  local d22="${HERE}/scenarios/D22_startup_endpoint_failures.sh"
  grep -q 'all-current cluster returns to safe service' "${d15}" &&
    grep -q 'wait_for_available_cluster' "${d15}" &&
    grep -q 'node rejoins after the submit port is released' "${d22}" &&
    grep -q 'wait_for_available_cluster' "${d22}"
}

config_identity_repair_accepts_safe_nopreempt_skew() {
  local scenario="${HERE}/scenarios/D19_config_identity.sh" repair
  repair="$(sed -n '/restoring exact baseline configs/,/scenario_end/p' "${scenario}")"
  [[ "${repair}" == *'wait_for_available_cluster'* ]] &&
    [[ "${repair}" != *'wait_for_steady_state'* ]]
}

d21_arms_delete_fault_only_after_startup_cleanup() {
  local scenario="${HERE}/scenarios/D21_fresh_unhealthy_delete_fence.sh"
  grep -Fq "printf '%s\\n' passthrough > /run/kafd-ipfault/mode" "${scenario}" &&
    grep -q 'printf .* timeout > /run/kafd-ipfault/mode' "${scenario}"
}

d21_load_journal_predicates() {
  local scenario="${HERE}/scenarios/D21_fresh_unhealthy_delete_fence.sh"
  eval "$(sed -n '/^timed_out_delete_logged()/,/^target_stays_only_on_victim()/p' "${scenario}" | sed '$d')"
  eval "$(sed -n '/^verified_absent_logged()/,/^}/p' "${scenario}")"
}

d21_delete_diagnostics_accept_both_cleanup_paths() (
  d21_load_journal_predicates
  local target_vip=192.0.2.210 IFACE=ens5 victim=fixture failure_since=now line prefix
  kafd_log_since() { printf '%s\n' "$line"; }
  for prefix in 'unbind 192.0.2.210:' 'unbind_all: 192.0.2.210/32 on ens5:'; do
    line="WARN keepafloatd::vip: ${prefix} child command exceeded 250ms"
    [[ "$prefix" != unbind_all:* ]] || line+='; retrying (1/3)'
    timed_out_delete_logged || return 1
    line="WARN keepafloatd::vip: ${prefix} ip addr del failed (exit status: 2) and the address is still present"
    [[ "$prefix" != unbind_all:* ]] || line+='; exhausted 3 attempts'
    failed_delete_logged || return 1
  done
)

d21_delete_diagnostics_reject_other_targets_and_errors() (
  d21_load_journal_predicates
  local target_vip=192.0.2.210 IFACE=ens5 victim=fixture failure_since=now line prefix error
  kafd_log_since() { printf '%s\n' "$line"; }
  for prefix in 'unbind 192.0.2.211:' 'unbind 192x0x2x210:' \
    'unbind_all: 192.0.2.211/32 on ens5:' 'unbind_all: 192.0.2.210/24 on ens5:' \
    'unbind_all: 192.0.2.210/32 on ens6:'; do
    line="WARN keepafloatd::vip: ${prefix} child command exceeded 250ms"
    timed_out_delete_logged && return 1
    line="WARN keepafloatd::vip: ${prefix} ip addr del failed (exit status: 2) and the address is still present"
    failed_delete_logged && return 1
  done
  for prefix in 'unbind 192.0.2.210:' 'unbind_all: 192.0.2.210/32 on ens5:'; do
    line="WARN keepafloatd::vip: ${prefix} child command exceeded 500ms"
    timed_out_delete_logged && return 1
    for error in 'ip addr add failed' \
      'ip addr del failed (exit status: 1) and the address is still present' \
      'ip addr del failed (exit status: 2) and presence verification failed'; do
      line="WARN keepafloatd::vip: ${prefix} ${error}"
      failed_delete_logged && return 1
    done
  done
  return 0
)

d21_journal_predicates_consume_large_output() (
  d21_load_journal_predicates
  local target_vip=192.0.2.210 IFACE=ens5 victim=fixture failure_since=now line prefix
  kafd_log_since() {
    awk -v first="$line" 'BEGIN { print first; for (i=0; i<20000; i++) print "unrelated journal noise padding to exceed the pipe buffer" }'
  }
  for prefix in 'unbind 192.0.2.210:' 'unbind_all: 192.0.2.210/32 on ens5:'; do
    line="WARN keepafloatd::vip: ${prefix} child command exceeded 250ms"
    timed_out_delete_logged || return 1
    line="WARN keepafloatd::vip: ${prefix} ip addr del failed (exit status: 2) and the address is still present"
    failed_delete_logged || return 1
  done
  line='INFO keepafloatd::vip: unbound 192.0.2.210/32 on ens5'
  verified_absent_logged
)

d21_journal_predicates_propagate_reader_failure() (
  d21_load_journal_predicates
  local target_vip=192.0.2.210 IFACE=ens5 victim=fixture failure_since=now
  kafd_log_since() {
    printf '%s\n' 'WARN keepafloatd::vip: unbind 192.0.2.210: child command exceeded 250ms' \
      'WARN keepafloatd::vip: unbind 192.0.2.210: ip addr del failed (exit status: 2) and the address is still present' \
      'INFO keepafloatd::vip: unbound 192.0.2.210/32 on ens5'
    return 42
  }
  ! timed_out_delete_logged && ! failed_delete_logged && ! verified_absent_logged
)

d22_matches_current_multiline_startup_diagnostics() {
  local scenario="${HERE}/scenarios/D22_startup_endpoint_failures.sh"
  grep -q 'Error: submit server' "${scenario}" &&
    grep -q 'Address in use' "${scenario}" &&
    grep -q 'raft_address and client_submit_address must differ' "${scenario}"
}

scenario_error_guard_rejects_an_unchecked_failure() {
  timeout 2 bash -c '
    source "$1"
    _PASS=1
    evid() { :; }
    scenario_end() {
      [[ "${_PASS}" -eq 0 ]] || exit 41
      exit 42
    }
    install_scenario_error_guard
    false
    exit 43
  ' _ "${HERE}/lib.sh"
  [[ "$?" -eq 42 ]]
}

scenario_error_guard_rejects_a_nested_unchecked_failure() {
  timeout 2 bash -c '
    source "$1"
    _PASS=1
    evid() { :; }
    scenario_end() {
      [[ "${_PASS}" -eq 0 ]] || exit 41
      exit 42
    }
    silently_recovering_helper() { false; true; }
    install_scenario_error_guard
    silently_recovering_helper
    exit 43
  ' _ "${HERE}/lib.sh"
  [[ "$?" -eq 42 ]]
}

scenario_error_guard_restores_only_from_the_parent() {
  local marker rc lines
  marker="$(mktemp)"
  timeout 2 bash -c '
    source "$1"
    marker="$2"
    _PASS=1
    evid() { :; }
    scenario_end() {
      printf "restore\n" >> "${marker}"
      exit 42
    }
    failing_worker() {
      (false) &
      local pid=$!
      wait "${pid}"
    }
    install_scenario_error_guard
    failing_worker
    exit 43
  ' _ "${HERE}/lib.sh" "${marker}"
  rc=$?
  lines="$(wc -l < "${marker}")"
  rm -f "${marker}"
  [[ "${rc}" -eq 42 && "${lines}" -eq 1 ]]
}

campaign_guard_traps_restore_once_and_preserve_status() {
  local marker rc lines
  marker="$(mktemp)"
  timeout 2 bash -c '
    source "$1"
    marker="$2"
    restore_scenario_guard() {
      printf "restore\n" >> "${marker}"
      RUNALL_GUARD_ACTIVE=0
    }
    RUNALL_GUARD_ACTIVE=1
    install_campaign_guard_traps
    exit 17
  ' _ "${HERE}/run-all-guard.sh" "${marker}"
  rc=$?
  lines="$(wc -l < "${marker}")"
  rm -f "${marker}"

  [[ "${rc}" -eq 17 && "${lines}" -eq 1 ]]
}

campaign_guard_signal_traps_restore() {
  local signal expected marker rc lines
  for signal in INT TERM; do
    case "${signal}" in INT) expected=130 ;; TERM) expected=143 ;; esac
    marker="$(mktemp)"
    timeout 2 bash -c '
      source "$1"
      marker="$2"
      restore_scenario_guard() {
        printf "restore\n" >> "${marker}"
        RUNALL_GUARD_ACTIVE=0
      }
      RUNALL_GUARD_ACTIVE=1
      install_campaign_guard_traps
      kill -"$3" "$$"
    ' _ "${HERE}/run-all-guard.sh" "${marker}" "${signal}"
    rc=$?
    lines="$(wc -l < "${marker}")"
    rm -f "${marker}"
    [[ "${rc}" -eq "${expected}" && "${lines}" -eq 1 ]] || return 1
  done
}

assert "inner SSH accepts PXE host-key rotation" inner_ssh_ignores_disposable_node_host_keys
assert "secret read failure cannot trigger writes" secret_read_failure_never_attempts_a_write
assert "secret audit is read-only and rejects mismatch" \
  secret_audit_is_read_only_and_rejects_mismatch
assert "live-holder check rejects survivor duplicates" \
  live_holder_check_rejects_survivor_duplicates
assert "live service accepts safe skew and rechecks after reachability" \
  live_service_accepts_safe_skew_and_rechecks_after_reachability
assert "nopreempt fault scenarios do not require even redistribution" \
  nopreempt_fault_scenarios_do_not_require_even_redistribution
assert "no-duplicate check permits an ownerless handoff gap only" \
  no_duplicate_check_allows_a_safe_handoff_gap
assert "config mismatch fixture changes effective staleness" \
  behavior_changing_stale_window_skips_equivalent_raw_values
assert "config equivalence fixture detects absent raw alternatives" \
  behaviorally_equivalent_stale_window_reports_when_none_exists
assert "arbitrary-interface holder lookup uses one batched snapshot" \
  arbitrary_interface_holder_query_is_batched
assert "RGW discovery is independent of cluster-specific daemon names" \
  rgw_discovery_does_not_assume_a_cluster_specific_daemon_name
assert "unreadable nodes invalidate unique-holder evidence" \
  unreadable_node_invalidates_unique_holder_evidence
assert "latest leader=None invalidates stale Some evidence" \
  latest_leader_none_invalidates_old_some_evidence
assert "a current configured leader needs a voter majority" \
  current_configured_leader_requires_a_majority
assert "inactive daemons cannot supply stale leader evidence" \
  inactive_nodes_cannot_supply_leader_evidence
assert "malformed address-count evidence is rejected" \
  malformed_address_count_evidence_is_rejected
assert "S3 fixture setup cannot ignore backend failure" \
  s3_fixture_setup_cannot_ignore_backend_failure
assert "real service failure controls the probed front end and backend" \
  real_service_failure_controls_the_probed_frontend_and_backend
assert "real service recovery propagates an RGW start failure" \
  real_service_recovery_propagates_backend_start_failure
assert "real service failure propagates a HAProxy stop failure" \
  real_service_failure_propagates_frontend_stop_failure
assert "post-RGW stability requires the real data path" \
  post_rgw_stability_requires_the_real_data_path
assert "post-RGW stability requires every node in the cluster" \
  post_rgw_stability_requires_every_node_in_the_cluster
assert "service-path health probes the RGW backend itself" \
  service_path_probes_the_rgw_backend_itself
assert "campaign timeout owns and terminates the scenario process group" \
  campaign_timeout_terminates_the_scenario_process_group
assert "campaign stop kills a TERM-ignoring scenario descendant" \
  campaign_group_stop_kills_a_term_ignoring_descendant
assert "campaign stop waits for descendant exit after leader reap" \
  campaign_group_stop_waits_for_descendant_exit
assert "campaign stop fails closed if the group survives forced termination" \
  campaign_group_stop_rejects_a_surviving_group
assert "final audit captures mutation queries before absence checks" \
  final_audit_captures_mutation_queries_before_testing_absence
assert "central cleanup covers VLAN and CIDR artifacts" \
  central_cleanup_covers_vlan_and_cidr_artifacts
assert "B3 rejects notify states on the wrong node" \
  b3_rejects_notify_states_on_the_wrong_node
assert "D17 holds daemon liveness with assignment invariants" \
  d17_holds_liveness_with_assignment_invariants
assert "config restore requires every backup" missing_config_backup_is_a_restore_failure
assert "config restore succeeds on every node and preserves unrelated backups" \
  config_restore_succeeds_for_every_node
assert "config restore fixture aborts without a temporary directory" \
  config_restore_fixture_aborts_without_a_temp_directory
assert "config restore fixture supports temporary paths with spaces" \
  config_restore_fixture_supports_spaced_paths
for restore_mode in missing transport move mapping; do
  assert "config restore continues after ${restore_mode} failure" \
    config_restore_continues_after_failure "${restore_mode}"
done
assert "config restore retries reach remaining backups after partial success" \
  config_restore_retry_reaches_unrestored_nodes
for restore_mode in transport mapping; do
  assert "config restore continues after ${restore_mode} failure under errexit" \
    config_restore_continues_under_errexit "${restore_mode}"
done
assert "config restore rejects invalid tags without effects" \
  config_restore_rejects_invalid_tags_without_effects
assert "campaign guard requires exact backups and verifies restored hashes" \
  campaign_guard_requires_exact_backups_and_hash_verification
assert "guard hash verification propagates remote failure" \
  guard_hash_verification_propagates_remote_failure
assert "campaign cleanup preserves unknown config backups" \
  campaign_cleanup_preserves_unknown_config_backups
assert "campaign guard rejects every artifact it removes" \
  campaign_guard_rejects_every_artifact_it_removes
assert "partition cleanup deletes only harness-owned rules" \
  partition_cleanup_deletes_only_harness_rules
assert "mixed-version deployment checks the running process" \
  mixed_version_deploy_checks_the_running_process
assert "campaign timeout cleanup handles INT/TERM before forced kill" \
  campaign_timeout_cleanup_is_signal_safe
assert "ownership cleanup and final marker audit fail closed" \
  ownership_cleanup_and_audit_are_fail_closed
assert "B3 notify installation and transition checks fail closed" \
  b3_notify_checks_are_fail_closed_and_stable
assert "B3 nopreempt cleanup accepts safe unique ownership skew" \
  b3_nopreempt_cleanup_requires_availability_not_even_spread
assert "soak requires even spread only after a clean full reform" \
  soak_uses_even_spread_only_for_a_clean_full_reform
assert "soak aborts failed disruptions before recovery commands" \
  soak_disruption_failures_abort_before_recovery_commands
assert "soak arms config restore only after a successful backup" \
  soak_arms_restore_only_after_a_successful_backup
assert "restart and no-majority evidence is sustained" \
  restart_and_no_majority_checks_are_sustained
assert "real-cluster marker queries reject duplicate routes" \
  realcluster_marker_queries_reject_duplicate_routes
assert "SSH commands have an execution deadline" ssh_commands_have_an_execution_deadline
assert "baseline restore retries a failed clean reform" \
  baseline_restore_retries_a_failed_clean_reform
assert "even-spread wait rechecks ownership after reachability" \
  even_wait_rechecks_ownership_after_reachability
assert "zero-duration hold still checks its predicate" \
  zero_duration_hold_still_checks_the_predicate
assert "positive hold rechecks at the window end" \
  positive_hold_rechecks_at_the_window_end
assert "count oracles do not mask source failures" \
  count_oracles_do_not_mask_source_failures
assert "event counters distinguish zero from unreadable" \
  event_counters_distinguish_zero_from_unreadable
assert "journal absence rejects an unreadable node" \
  journal_absence_rejects_an_unreadable_node
assert "journal readers propagate journalctl failure" \
  journal_readers_propagate_journalctl_failure
assert "leader readers use only the current service invocation" \
  leader_readers_are_scoped_to_the_current_service_invocation
assert "leader reader retains an old transition after heavy log noise" \
  leader_reader_keeps_old_transition_after_heavy_noise
assert "D22 proves failed startup has no daemon or listeners" \
  d22_proves_failed_startup_has_no_daemon_or_listeners
assert "process absence rejects an observer error" \
  process_absence_rejects_an_observer_error
assert "listener absence rejects an observer error" \
  listener_absence_rejects_an_observer_error
assert "listener ownership rejects an empty observation" \
  listener_ownership_rejects_an_empty_observation
assert "sentinel health replaces inline and block commands" \
  sentinel_health_replaces_inline_and_block_commands
assert "endurance uses the shared health mutator" \
  endurance_uses_the_shared_health_mutator
assert "cluster mutators reject a middle-node failure" \
  cluster_mutators_reject_a_middle_node_failure
assert "D9 parallel restart checks every child" \
  d9_parallel_restart_checks_every_child
assert "hard kill verifies the service stayed down" \
  hard_kill_verifies_the_service_stayed_down
assert "steady state rechecks ownership after leader observation" \
  steady_state_rechecks_ownership_after_leader_observation
assert "available cluster accepts safe uneven ownership" \
  available_cluster_accepts_safe_uneven_ownership
assert "available cluster rejects an active isolated rejoin" \
  available_cluster_rejects_an_active_isolated_rejoin
assert "nonpreempt rejoins require availability rather than redistribution" \
  nonpreempt_rejoins_use_availability_not_even_spread
assert "config identity repair accepts safe nopreempt ownership skew" \
  config_identity_repair_accepts_safe_nopreempt_skew
assert "cold reform redistributes but nopreempt rejoins require availability" \
  cold_reform_and_leader_rejoin_use_their_exact_oracles
assert "D21 arms its delete fault only after startup cleanup" \
  d21_arms_delete_fault_only_after_startup_cleanup
assert "D21 recognizes exact diagnostics from both cleanup paths" \
  d21_delete_diagnostics_accept_both_cleanup_paths
assert "D21 rejects other delete targets and errors" \
  d21_delete_diagnostics_reject_other_targets_and_errors
assert "D21 journal predicates consume output larger than the pipe buffer" \
  d21_journal_predicates_consume_large_output
assert "D21 journal predicates propagate reader failures" \
  d21_journal_predicates_propagate_reader_failure
assert "D22 matches current multiline startup diagnostics" \
  d22_matches_current_multiline_startup_diagnostics
assert "scenario error guard rejects unchecked failures" \
  scenario_error_guard_rejects_an_unchecked_failure
assert "scenario error guard rejects nested unchecked failures" \
  scenario_error_guard_rejects_a_nested_unchecked_failure
assert "scenario error guard restores only from the parent" \
  scenario_error_guard_restores_only_from_the_parent
assert "campaign exit traps restore once and preserve failure status" \
  campaign_guard_traps_restore_once_and_preserve_status
assert "campaign INT and TERM traps restore exactly once" \
  campaign_guard_signal_traps_restore

assert "ARP failover observation never sends an active probe" \
  arp_failover_observation_never_sends_an_active_probe

(( failures == 0 ))
