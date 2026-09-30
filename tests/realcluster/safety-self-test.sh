#!/usr/bin/env bash
# Sourced by self-test.sh. Every remote operation is replaced with a local fixture.

api_auth_fixture() {
  _TOKEN=""
  CROIT_USER="fixture'user&+name"
  CROIT_PASS="fixture'password&+value"
  CROIT_URL="https://example.invalid"
  fixture_auth_mode=success
  fixture_auth_calls="$(mktemp)"
  trap 'rm -f "${fixture_auth_calls}"' EXIT
  export CROIT_USER CROIT_PASS CROIT_URL fixture_auth_mode fixture_auth_calls
  mgmt_sh() { bash -c "$1"; }
  curl() {
    local arg user="" password="" input="" endpoint="" grant=""
    printf 'curl\n' >> "${fixture_auth_calls}"
    [[ "${fixture_auth_mode}" != transport ]] || return 22
    while (( $# )); do
      arg="$1"; shift
      case "${arg}" in
        --data-urlencode)
          case "${1-}" in
            username=*) user="${1#username=}" ;;
            password@-) input="$(</dev/stdin)"; password="${input}" ;;
            grant_type=password) grant=password ;;
            *) return 64 ;;
          esac
          shift ;;
        --data) return 64 ;;
        https://*) endpoint="${arg}" ;;
      esac
    done
    [[ "${user}" == "${CROIT_USER}" && "${password}" == "${CROIT_PASS}" &&
      "${grant}" == password && "${endpoint}" == "${CROIT_URL}/api/auth/login-form" ]] || return 65
    printf 'fixture response\n'
    [[ "${fixture_auth_mode}" != partial ]]
  }
  python3() {
    local response
    response="$(</dev/stdin)"
    [[ "${fixture_auth_mode}" != parser && "${response}" == 'fixture response' ]] || return 1
    [[ "${fixture_auth_mode}" != empty ]] || return 0
    printf 'fixture-token\n'
  }
  export -f curl python3
}

api_credentials_survive_shell_and_form_encoding() (
  api_auth_fixture
  [[ "$(croit_token)" == fixture-token ]]
)

api_auth_failures_block_the_api_request() (
  api_auth_fixture
  for fixture_auth_mode in transport parser empty partial; do
    : > "${fixture_auth_calls}"
    ! croit_api GET /cluster/status >/dev/null 2>&1 || return 1
    [[ "$(wc -l < "${fixture_auth_calls}")" -eq 1 ]] || return 1
  done
)

api_failed_response_is_not_cached() (
  api_auth_fixture
  fixture_auth_mode=partial
  ! croit_token >/dev/null 2>&1 || return 1
  [[ -z "${_TOKEN}" ]] || return 1
  ! croit_token >/dev/null 2>&1
)

api_auth_uses_bash_on_the_management_node() (
  local fixture_shims
  api_auth_fixture
  fixture_shims="$(mktemp -d)"
  trap 'rm -rf "${fixture_shims}"; rm -f "${fixture_auth_calls}"' EXIT
  # POSIX shells may discard exported Bash functions, so use executable mocks at this boundary.
  printf '#!/usr/bin/env bash\n%s\ncurl "$@"\n' "$(declare -f curl)" > "${fixture_shims}/curl"
  printf '#!/usr/bin/env bash\n%s\npython3 "$@"\n' "$(declare -f python3)" > "${fixture_shims}/python3"
  chmod +x "${fixture_shims}/curl" "${fixture_shims}/python3"
  export PATH="${fixture_shims}:${PATH}"
  mgmt_sh() {
    [[ "$1" == 'bash -c '* ]] || return 2
    sh -c "$1"
  }
  [[ "$(croit_token)" == fixture-token ]]
)

api_request_arguments_remain_separate() (
  _TOKEN=fixture-token
  CROIT_URL='https://example.invalid'
  mgmt_sh() { bash -c "$1"; }
  curl() {
    local header=0 body=0 endpoint=0
    while (( $# )); do
      case "$1" in
        -H) [[ "$2" == 'Authorization: Bearer fixture-token' ]] || return 1; header=1; shift ;;
        --data) [[ "$2" == "value with ' & + spaces" ]] || return 1; body=1; shift ;;
        https://example.invalid/api/example) endpoint=1 ;;
      esac
      shift
    done
    (( header && body && endpoint ))
  }
  export -f curl
  croit_api POST /example --data "value with ' & + spaces"
)

example_preserves_environment_credentials() (
  CROIT_PASS="fixture'password&+"
  S3_ACCESS=fixture-access
  S3_SECRET=fixture-secret
  source "${HERE}/env.example.sh"
  [[ "${CROIT_PASS}" == "fixture'password&+" && "${S3_ACCESS}" == fixture-access &&
    "${S3_SECRET}" == fixture-secret ]] || return 1
  unset CROIT_PASS S3_ACCESS S3_SECRET
  source "${HERE}/env.example.sh"
  [[ -z "${CROIT_PASS}${S3_ACCESS}${S3_SECRET}" ]]
)

hard_kill_is_scoped_and_propagates_failure() (
  local fixture mode=success
  fixture="$(mktemp -d)"
  trap 'rm -rf "${fixture}"' EXIT
  NODE_IPS=(fixture-node)
  NODE_INSTANCES=(selected)
  node_sh() { eval "$2"; }
  pkill() { printf 'broad kill\n' >> "${fixture}/bad"; }
  systemctl() {
    [[ "${*: -1}" == keepafloatd@selected ]] || { touch "${fixture}/bad"; return 1; }
    printf '%s\n' "$1" >> "${fixture}/calls"
    case "$1" in
      kill) [[ "${mode}" != kill-failure ]] ;;
      stop) [[ "${mode}" != stop-failure ]] ;;
      is-active) [[ "${mode}" == still-active ]] ;;
      *) return 1 ;;
    esac
  }
  kafd_kill fixture-node || return 1
  [[ ! -e "${fixture}/bad" && "$(<"${fixture}/calls")" == $'kill\nstop\nis-active' ]] || return 1
  for mode in kill-failure stop-failure still-active; do
    ! kafd_kill fixture-node || return 1
  done
  : > "${fixture}/calls"
  ! kafd_kill unknown-node || return 1
  [[ ! -s "${fixture}/calls" ]]
)

d16_activation_evidence_fails_closed() (
  local mode=absent
  NODE_IPS=(first second third)
  eval "$(sed -n '/^activation_logged_since()/,/^holder_excludes()/p' \
    "${HERE}/scenarios/D16_chained_legacy_activation.sh" | sed '$d')"
  journal_event_count() {
    case "${mode}:$1" in
      present:*|mixed:first) printf '1\n' ;;
      unreadable:*|mixed:second) return 255 ;;
      partial:*) printf '0\n'; return 255 ;;
      *) printf '0\n' ;;
    esac
  }
  activation_not_logged_since fixture-time && ! activation_logged_since fixture-time || return 1
  mode=present
  activation_logged_since fixture-time && ! activation_not_logged_since fixture-time || return 1
  for mode in unreadable partial mixed; do
    ! activation_logged_since fixture-time && ! activation_not_logged_since fixture-time || return 1
  done
)

final_audit_requires_a_campaign_start() (
  local preflight
  preflight="$(sed '/^assert_cluster_secret_consistent/,$d' "${HERE}/final-audit.sh" \
    | sed '/^source /d; /^HERE=/d')"
  unset REALCLUSTER_AUDIT_SINCE
  ! (eval "${preflight}") >/dev/null 2>&1 || return 1
  REALCLUSTER_AUDIT_SINCE='2026-09-29 08:00:00 UTC'
  eval "${preflight}"
  [[ "${campaign_since}" == "${REALCLUSTER_AUDIT_SINCE}" ]]
)

config_backup_fixture() {
  fixture="$(mktemp -d)"
  trap 'rm -rf "${fixture}"' EXIT
  NODE_IPS=(first second third)
  NODE_INSTANCES=(first second third)
  for node in "${NODE_IPS[@]}"; do printf '%s\n' "${node}" > "${fixture}/config-${node}.yaml"; done
  node_sh() {
    local command="$2"
    command="${command//\/etc\/keepafloatd/${fixture}}"
    bash -c "${command}"
  }
}

failed_backup_removes_only_its_own_captures() (
  local fixture node
  config_backup_fixture
  printf 'operator backup\n' > "${fixture}/config-second.yaml.soak-bak"
  ! backup_cluster_configs soak || return 1
  [[ ! -e "${fixture}/config-first.yaml.soak-bak" &&
    "$(<"${fixture}/config-second.yaml.soak-bak")" == 'operator backup' &&
    "$(<"${fixture}/config-first.yaml")" == first ]] || return 1
  rm "${fixture}/config-second.yaml.soak-bak" "${fixture}/config-second.yaml"
  ! backup_cluster_configs soak || return 1
  [[ -z "$(find "${fixture}" -name '*.soak*' -print)" ]]
)

successful_backup_restores_every_original() (
  local fixture node
  config_backup_fixture
  backup_cluster_configs soak || return 1
  for node in "${NODE_IPS[@]}"; do printf 'modified\n' > "${fixture}/config-${node}.yaml"; done
  restore_cluster_configs soak || return 1
  for node in "${NODE_IPS[@]}"; do
    [[ "$(<"${fixture}/config-${node}.yaml")" == "${node}" ]] || return 1
  done
  [[ -z "$(find "${fixture}" -name '*.soak*' -print)" ]]
)

failed_backup_rolls_back_with_errexit() (
  local fixture node body
  config_backup_fixture
  rm "${fixture}/config-second.yaml"
  export fixture
  body="$(declare -f backup_cluster_configs instance_for_ip node_sh shell_quote fail)"
  body+=$'\nset -e\nNODE_IPS=(first second third)\nNODE_INSTANCES=(first second third)\nbackup_cluster_configs soak'
  ! bash -c "${body}" >/dev/null 2>&1 || return 1
  [[ -z "$(find "${fixture}" -name '*.soak*' -print)" ]]
)

backup_rollback_reports_errors_and_continues() (
  local fixture node
  config_backup_fixture
  rm "${fixture}/config-third.yaml"
  node_sh() {
    local command="$2"
    if [[ "$1" == first && "${command}" == 'rm -f -- '* ]]; then return 255; fi
    command="${command//\/etc\/keepafloatd/${fixture}}"
    bash -c "${command}"
  }
  ! backup_cluster_configs soak >"${fixture}/out" 2>"${fixture}/err" || return 1
  [[ -f "${fixture}/config-first.yaml.soak-bak" && ! -e "${fixture}/config-second.yaml.soak-bak" ]] || return 1
  grep -q 'could not remove captured backup on first' "${fixture}/err"
)

campaign_audit_rejects_soak_backups() (
  local fixture node
  config_backup_fixture
  printf 'operator\n' > "${fixture}/config-first.yaml.operator-bak"
  campaign_config_backups_absent || return 1
  printf 'soak\n' > "${fixture}/config-second.yaml.soak-bak"
  ! campaign_config_backups_absent
)

failed_soak_capture_does_not_mutate_the_cluster() (
  local calls
  calls="$(mktemp)"
  trap 'rm -f "${calls}"' EXIT
  eval "$(sed -n '/^SOAK_TAG=/,/^on_exit()/p' "${HERE}/soak.sh" | sed '$d')"
  say() { :; }
  heal_all() { printf 'heal\n' >> "${calls}"; }
  clear_health_sentinels() { printf 'health\n' >> "${calls}"; }
  restore_cluster_configs() { printf 'restore\n' >> "${calls}"; }
  clean_reform() { printf 'reform\n' >> "${calls}"; }
  wait_for_steady_state() { printf 'wait\n' >> "${calls}"; }
  cleanup_soak || return 1
  [[ ! -s "${calls}" ]] || return 1
  SOAK_CONFIG_BACKED_UP=1
  cleanup_soak || return 1
  [[ "$(<"${calls}")" == $'heal\nhealth\nrestore\nreform\nwait' ]]
)

standalone_scenario_capture_is_safe() (
  local scenario="${1:?}" backup_tag="${2:?}" fixture_mode="${3:?}"
  local fixture node stale_node="" status=0
  config_backup_fixture
  case "${fixture_mode}" in
    stale-first) stale_node=first ;;
    stale-middle) stale_node=second ;;
  esac
  if [[ -n "${stale_node}" ]]; then
    printf 'stale\n' > "${fixture}/config-${stale_node}.yaml.${backup_tag}-bak"
  fi
  node_sh() {
    local command="$2"
    case "${command}" in
      *'temporary=$(mktemp '*)
        [[ "${fixture_mode}:$1" != transport-middle:second ]] || return 255 ;;
      'rm -f -- '*) ;;
      'test -f /etc/keepafloatd/'*)
        printf 'restore\n' >> "${fixture}/effects" ;;
      'rm -f /run/kafd-notify.log '*)
        printf 'notify cleanup\n' >> "${fixture}/effects"; return 0 ;;
      *)
        printf 'mutation\n' >> "${fixture}/effects"
        printf 'modified\n' > "${fixture}/config-$1.yaml"
        return 42 ;;
    esac
    command="${command//\/etc\/keepafloatd/${fixture}}"
    bash -c "${command}"
  }
  scenario_start() { :; }
  scenario_end() { exit "$((1 - _PASS))"; }
  evid() { printf '%s\n' "$*" | tee -a "${fixture}/evidence"; }
  kafd_stop() { printf 'stop\n' >> "${fixture}/effects"; }
  set_healthy() { printf 'health\n' >> "${fixture}/effects"; }
  clean_reform() { printf 'reform\n' >> "${fixture}/effects"; }
  cleanup_ownership_test_artifacts() { printf 'IPv6 cleanup\n' >> "${fixture}/effects"; }
  mkdir "${fixture}/scenarios"
  cp "${HERE}/scenarios/${scenario}.sh" "${fixture}/scenarios/"
  # Run the actual scenario in a fresh shell so conditional assertions cannot suppress ERR traps.
  printf '%s\n' 'set -Euo pipefail' \
    'NODE_IPS=(first second third)' 'NODE_INSTANCES=(first second third)' \
    'IPV6_TEST_VIP=2001:db8::1' 'IFACE=fixture-interface' '_PASS=1' \
    "$(declare -f backup_cluster_configs restore_cluster_configs instance_for_ip shell_quote \
      node_sh scenario_start scenario_end evid kafd_stop set_healthy clean_reform \
      cleanup_ownership_test_artifacts scenario_unexpected_error install_scenario_error_guard)" \
    'install_scenario_error_guard' > "${fixture}/scenario.sh"
  export fixture fixture_mode
  bash "${fixture}/scenarios/${scenario}.sh" > "${fixture}/output" 2>&1 || status=$?
  [[ "${status}" -eq 1 ]] || { cat "${fixture}/output"; return 1; }
  for node in "${NODE_IPS[@]}"; do
    if [[ "$(<"${fixture}/config-${node}.yaml")" != "${node}" ]]; then
      printf '%s: %s config was overwritten with %s\n' \
        "${scenario}" "${node}" "$(<"${fixture}/config-${node}.yaml")" >&2
      return 1
    fi
    if [[ "${node}" == "${stale_node}" ]]; then
      [[ "$(<"${fixture}/config-${node}.yaml.${backup_tag}-bak")" == stale ]] || return 1
    else
      [[ ! -e "${fixture}/config-${node}.yaml.${backup_tag}-bak" ]] || return 1
    fi
  done
  if [[ "${fixture_mode}" == post-capture ]]; then
    grep -q 'unchecked command failed (42)' "${fixture}/output" &&
      [[ "$(grep -c '^mutation$' "${fixture}/effects")" -eq 1 &&
        "$(grep -c '^restore$' "${fixture}/effects")" -eq 3 ]] &&
      grep -q '^reform$' "${fixture}/effects"
  else
    local expected_node=second
    [[ "${fixture_mode}" != stale-first ]] || expected_node=first
    grep -q "config capture failed on ${expected_node};" "${fixture}/output" &&
      grep -q 'config capture failed; recovery was not started' "${fixture}/evidence" &&
      [[ ! -s "${fixture}/effects" ]]
  fi
)

guard_restore_fixture() {
  source "${HERE}/run-all-guard.sh"
  fixture="$(mktemp -d)" || exit 1
  trap 'rm -rf -- "${fixture}"' EXIT
  NODE_IPS=(first second third)
  RUNALL_GUARD_ACTIVE=1
  RUNALL_GUARD_COMPLETE=1
  guard_fault=none
  for node in "${NODE_IPS[@]}"; do
    mkdir -p "${fixture}/${node}/etc" "${fixture}/${node}/bin" \
      "${fixture}/${node}/run" "${fixture}/${node}/local" || exit 1
    printf 'original-config-%s\n' "${node}" > "${fixture}/${node}/etc/config-${node}.yaml.runall-guard"
    printf 'original-binary-%s\n' "${node}" > "${fixture}/${node}/bin/keepafloatd.runall-guard"
    printf 'changed\n' > "${fixture}/${node}/etc/config-${node}.yaml"
    printf 'changed\n' > "${fixture}/${node}/bin/keepafloatd"
    RUNALL_CONFIG_HASH[${node}]="$(sha256sum "${fixture}/${node}/etc/config-${node}.yaml.runall-guard" | awk '{print $1}')"
    RUNALL_BINARY_HASH[${node}]="$(sha256sum "${fixture}/${node}/bin/keepafloatd.runall-guard" | awk '{print $1}')"
    RUNALL_GUARD_CAPTURED[${node}]=1
  done
  instance_for_ip() {
    [[ "${guard_fault}:$1" != mapping:second ]] || return 1
    printf '%s\n' "$1"
  }
  kafd_stop() {
    printf '%s\n' "$1" >> "${fixture}/stops"
    [[ "${guard_fault}:$1" != stop:second ]]
  }
  node_sh() (
    local command="$2" base
    printf '%s\n' "$1" >> "${fixture}/calls"
    [[ "${guard_fault}:$1" != transport:second ]] || return 255
    base="$(shell_quote "${fixture}/$1")"
    command="${command//\/etc\/keepafloatd/${base}/etc}"
    command="${command//\/usr\/bin\/keepafloatd/${base}/bin/keepafloatd}"
    command="${command//\/usr\/local\/bin\//${base}/local/}"
    command="${command//\/run\//${base}/run/}"
    command="${command//\/tmp\/kafd-notify.log/${base}/notify.log}"
    export guard_fault fixture_node="$1"
    mv() {
      local source="${@: -2:1}"
      if [[ "${guard_fault}:${fixture_node}" == move:second && "${source}" == *'/bin/'* ]]; then
        return 1
      fi
      command mv "$@"
    }
    systemctl() { [[ "${guard_fault}:${fixture_node}" != reload:second ]]; }
    export -f mv systemctl
    bash -c "${command}" || return $?
    [[ "${guard_fault}:$1" != lost-response:second ]]
  )
  cleanup_ownership_test_artifacts() { [[ "${guard_fault}" != cleanup ]]; }
  ownership_markers_absent() { [[ "${guard_fault}" != markers ]]; }
  restore_baseline() {
    printf 'baseline\n' >> "${fixture}/baseline"
    [[ "${guard_fault}" != baseline ]]
  }
}

guard_restore_files_match() {
  local selected="${1:-all}" node
  for node in "${NODE_IPS[@]}"; do
    [[ "${selected}" == all || "${selected}" == "${node}" ]] || continue
    [[ "$(<"${fixture}/${node}/etc/config-${node}.yaml")" == "original-config-${node}" &&
      "$(<"${fixture}/${node}/bin/keepafloatd")" == "original-binary-${node}" ]] || {
      printf 'guard did not restore both files on %s\n' "${node}" >&2
      return 1
    }
  done
}

guard_restore_succeeds() (
  local fixture node guard_fault
  guard_restore_fixture
  restore_scenario_guard || return 1
  guard_restore_files_match && [[ "${RUNALL_GUARD_ACTIVE}" -eq 0 &&
    "$(<"${fixture}/stops")" == $'first\nsecond\nthird' ]]
)

guard_restore_continues_and_retries() (
  local fixture node guard_fault mode="${1:?}" diagnostic
  guard_restore_fixture
  guard_fault="${mode}"
  ! restore_scenario_guard >"${fixture}/out" 2>"${fixture}/err" || return 1
  [[ "${RUNALL_GUARD_ACTIVE}" -eq 1 && ! -e "${fixture}/baseline" ]] || return 1
  guard_restore_files_match first && guard_restore_files_match third || return 1
  case "${mode}" in
    stop) diagnostic='campaign guard stop failed on second' ;;
    mapping) diagnostic='campaign guard instance lookup failed on second' ;;
    *) diagnostic='campaign guard restore failed on second' ;;
  esac
  grep -Fxq "${diagnostic}" "${fixture}/err" || return 1
  if [[ "${mode}" == stop || "${mode}" == mapping ]]; then
    [[ "$(<"${fixture}/second/etc/config-second.yaml")" == changed &&
      "$(<"${fixture}/second/bin/keepafloatd")" == changed ]] || return 1
  fi
  guard_fault=none
  restore_scenario_guard || return 1
  guard_restore_files_match && [[ "${RUNALL_GUARD_ACTIVE}" -eq 0 ]]
)

guard_restore_retries_global_failure() (
  local fixture node guard_fault mode="${1:?}"
  guard_restore_fixture
  guard_fault="${mode}"
  ! restore_scenario_guard || return 1
  guard_restore_files_match && [[ "${RUNALL_GUARD_ACTIVE}" -eq 1 ]] || return 1
  guard_fault=none
  restore_scenario_guard && [[ "${RUNALL_GUARD_ACTIVE}" -eq 0 ]]
)

guard_restore_continues_under_errexit() (
  local fixture node guard_fault body status=0
  guard_restore_fixture
  guard_fault=transport
  body="$(declare -f restore_scenario_guard verify_restored_guard_hashes \
    node_sh shell_quote kafd_stop instance_for_ip cleanup_ownership_test_artifacts \
    ownership_markers_absent restore_baseline)"
  body+=$'\n'
  body+="$(declare -p NODE_IPS RUNALL_GUARD_COMPLETE RUNALL_GUARD_ACTIVE \
    RUNALL_GUARD_CAPTURED RUNALL_CONFIG_HASH RUNALL_BINARY_HASH REALCLUSTER_CAMPAIGN_BACKUP_TAGS)"
  export fixture guard_fault
  body+=$'\nset -Eeuo pipefail\nrestore_scenario_guard\nprintf "unexpected success\\n"'
  bash -c "${body}" >"${fixture}/out" 2>"${fixture}/err" || status=$?
  [[ "${status}" -eq 1 && ! -e "${fixture}/baseline" ]] &&
    guard_restore_files_match third &&
    grep -Fxq 'campaign guard restore failed on second' "${fixture}/err"
)

guard_restore_rejects_unverifiable_files() (
  local fixture node guard_fault kind="${1:?}" path="${2:?}" target
  guard_restore_fixture
  target="${fixture}/second/${path}"
  case "${kind}" in
    missing) rm "${target}.runall-guard" ;;
    corrupt) printf 'corrupt\n' > "${target}.runall-guard" ;;
  esac
  ! restore_scenario_guard >"${fixture}/out" 2>"${fixture}/err" || return 1
  [[ "${RUNALL_GUARD_ACTIVE}" -eq 1 && ! -e "${fixture}/baseline" ]] || return 1
  [[ "$(<"${target}")" == changed ]] || return 1
  if [[ "${kind}" == corrupt ]]; then
    [[ "$(<"${target}.runall-guard")" == corrupt ]] || return 1
  fi
  guard_restore_files_match third &&
    grep -Fxq 'campaign guard restore failed on second' "${fixture}/err"
)

guard_restore_rejects_drift_after_consumed_backup() (
  local fixture node guard_fault
  guard_restore_fixture
  guard_fault=baseline
  ! restore_scenario_guard || return 1
  printf 'drift\n' > "${fixture}/first/etc/config-first.yaml"
  guard_fault=none
  ! restore_scenario_guard >"${fixture}/out" 2>"${fixture}/err" &&
    [[ "${RUNALL_GUARD_ACTIVE}" -eq 1 && "$(wc -l < "${fixture}/baseline")" -eq 1 ]] &&
    grep -Fxq 'campaign guard restore failed on first' "${fixture}/err"
)

guard_restore_rejects_incomplete_capture() (
  local fixture node guard_fault mode="${1:?}"
  guard_restore_fixture
  case "${mode}" in
    incomplete) RUNALL_GUARD_COMPLETE=0 ;;
    missing-node) RUNALL_GUARD_CAPTURED[second]=pending ;;
    hash) RUNALL_BINARY_HASH[second]=invalid ;;
  esac
  ! restore_scenario_guard && [[ ! -e "${fixture}/calls" && ! -e "${fixture}/stops" ]]
)

assert "campaign guard restores exact config and binary on every node" guard_restore_succeeds
for guard_mode in transport stop mapping move reload lost-response; do
  assert "campaign guard continues and retries after ${guard_mode} failure" \
    guard_restore_continues_and_retries "${guard_mode}"
done
assert "campaign guard continues after transport failure under errexit" \
  guard_restore_continues_under_errexit
for guard_mode in cleanup markers baseline; do
  assert "campaign guard retries after ${guard_mode} failure with consumed backups" \
    guard_restore_retries_global_failure "${guard_mode}"
done
for guard_mode in missing corrupt; do
  for guard_path in etc/config-second.yaml bin/keepafloatd; do
    assert "campaign guard rejects ${guard_mode} ${guard_path} but restores other nodes" \
      guard_restore_rejects_unverifiable_files "${guard_mode}" "${guard_path}"
  done
done
assert "campaign guard rejects drift after a consumed backup" guard_restore_rejects_drift_after_consumed_backup
for guard_mode in incomplete missing-node hash; do
  assert "campaign guard rejects ${guard_mode} capture before effects" \
    guard_restore_rejects_incomplete_capture "${guard_mode}"
done

assert "API credentials survive shell and form encoding" api_credentials_survive_shell_and_form_encoding
assert "API auth failures block the API request" api_auth_failures_block_the_api_request
assert "API failed response is not cached" api_failed_response_is_not_cached
assert "API auth explicitly uses Bash on the management node" api_auth_uses_bash_on_the_management_node
assert "API request arguments remain separate" api_request_arguments_remain_separate
assert "example preserves environment credentials" example_preserves_environment_credentials
assert "hard kill targets one unit and propagates failures" hard_kill_is_scoped_and_propagates_failure
assert "D16 activation evidence fails closed" d16_activation_evidence_fails_closed
assert "final audit requires a campaign start" final_audit_requires_a_campaign_start
assert "failed backup removes only its own captures" failed_backup_removes_only_its_own_captures
assert "successful backup restores every original" successful_backup_restores_every_original
assert "failed backup rolls back under errexit" failed_backup_rolls_back_with_errexit
assert "backup rollback reports errors and continues" backup_rollback_reports_errors_and_continues
assert "campaign audit rejects soak backups" campaign_audit_rejects_soak_backups
assert "failed soak capture does not mutate the cluster" failed_soak_capture_does_not_mutate_the_cluster
for capture_mode in stale-first stale-middle transport-middle post-capture; do
  assert "B3 standalone capture: ${capture_mode}" standalone_scenario_capture_is_safe \
    B3_notify_hook b3-notify "${capture_mode}"
  assert "D24 standalone capture: ${capture_mode}" standalone_scenario_capture_is_safe \
    D24_ipv6_crash_removed_vip_cleanup d24 "${capture_mode}"
done

removed_vip_evidence_is_fail_closed() (
  local mode="${1:?}"
  NODE_IPS=(first second third)
  IFACE=fixture0
  eval "$(sed -n '/^vip_absent_everywhere()/,/^}/p' "${HERE}/scenarios/C5_config_mutation.sh")"
  node_sh() {
    if [[ "$2" == *'grep -F -q'* ]]; then
      case "${mode}:$1" in
        transport:*|partial:third|malformed:second) return 255 ;;
        present:second) return 0 ;;
        *) return 1 ;;
      esac
    fi
    case "${mode}:$1" in
      transport:*) return 255 ;;
      partial:third) printf '0\n'; return 255 ;;
      malformed:second) printf 'unknown\n' ;;
      present:second) printf '1\n' ;;
      *) printf '0\n' ;;
    esac
  }
  if [[ "${mode}" == absent ]]; then
    vip_absent_everywhere 192.0.2.200
  else
    ! vip_absent_everywhere 192.0.2.200
  fi
)

release_timing_evidence_fixture() {
  NODE_IPS=(fixture-node)
  NODE_INSTANCES=(fixture-instance)
  IFACE=fixture0
  timing_context='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 100000000'
  node_sh() {
    [[ "$1" == fixture-node && "$2" == *'_BOOT_ID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'* &&
      "$2" == *'_SYSTEMD_INVOCATION_ID=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'* &&
      "$2" == *'short-monotonic'* ]] || return 64
    printf '%s\n' "${timing_log}"
    [[ "${timing_mode}" != transport ]]
  }
}

release_timing_uses_first_matching_event() (
  local timing_context timing_mode=ok timing_log result
  release_timing_evidence_fixture
  timing_log='[   99.900000] node daemon: unbound 192.0.2.200/32 on fixture0
[  100.500000] node daemon: unbound 192.0.2.201/32 on fixture0
[  101.000000] node daemon: unbound 192.0.2.200/32 on fixture1
[  102.000000] node daemon: unbound 192.0.2.200/32 on fixture0
[  104.000000] node daemon: unbound 192.0.2.200/32 on fixture0'
  result="$(vip_release_elapsed_ms fixture-node 192.0.2.200 "${timing_context}")" || return 1
  [[ "${result}" == 2000 && "${result}" -lt 3000 ]]
)

release_timing_accepts_exact_lower_bound() (
  local timing_context timing_mode=ok timing_log result
  release_timing_evidence_fixture
  timing_log='[  103.000000] node daemon: unbound 192.0.2.200/32 on fixture0'
  result="$(vip_release_elapsed_ms fixture-node 192.0.2.200 "${timing_context}")" || return 1
  [[ "${result}" == 3000 ]]
)

release_timing_rejects_unreadable_evidence() (
  local timing_context timing_mode="${1:?}" timing_log
  release_timing_evidence_fixture
  case "${timing_mode}" in
    transport) timing_log='[  103.000000] node daemon: unbound 192.0.2.200/32 on fixture0' ;;
    empty) timing_log='' ;;
    malformed) timing_log='[ invalid] node daemon: unbound 192.0.2.200/32 on fixture0' ;;
    old) timing_log='[   99.000000] node daemon: unbound 192.0.2.200/32 on fixture0' ;;
    other-interface) timing_log='[  103.000000] node daemon: unbound 192.0.2.200/32 on fixture01' ;;
  esac
  ! vip_release_elapsed_ms fixture-node 192.0.2.200 "${timing_context}" >/dev/null
)

release_timing_rejects_invalid_context_before_ssh() (
  local context marker
  marker="$(mktemp)" || exit 1
  trap 'rm -f -- "${marker}"' EXIT
  NODE_IPS=(fixture-node)
  NODE_INSTANCES=(fixture-instance)
  node_sh() { printf 'called\n' >> "${marker}"; return 255; }
  for context in '' 'not identifiers' \
    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb -1' \
    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 1 extra' \
    $'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 1\nextra'; do
    local result=0
    vip_release_elapsed_ms fixture-node 192.0.2.200 "${context}" >/dev/null || result=$?
    [[ "${result}" -eq 1 ]] || return 1
  done
  [[ ! -s "${marker}" ]]
)

timed_failure_capture_validates_remote_result() (
  local mode="${1:?}" expected result=0
  NODE_IPS=(fixture-node)
  NODE_INSTANCES=(fixture-instance)
  expected='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 100000000'
  node_sh() {
    [[ "$1" == fixture-node && "$2" == *'keepafloatd@fixture-instance'* &&
      "$2" == *'time.monotonic_ns()'* && "$2" == *'keepafloatd-unhealthy'* ]] || return 64
    case "${mode}" in
      transport) printf '%s\n' "${expected}"; return 255 ;;
      malformed) printf 'invalid context\n' ;;
      extra) printf '%s extra\n' "${expected}" ;;
      multiline) printf '%s\nextra\n' "${expected}" ;;
      *) printf '%s\n' "${expected}" ;;
    esac
  }
  if [[ "${mode}" == success ]]; then
    [[ "$(timed_sentinel_fail fixture-node)" == "${expected}" ]]
  else
    timed_sentinel_fail fixture-node >/dev/null || result=$?
    [[ "${result}" -eq 1 ]]
  fi
)

d13_scenario_rejects_early_release_despite_late_polling() (
  local timing_context timing_mode=ok timing_log failures=0 unexpected=0 body
  release_timing_evidence_fixture
  timing_log='[  102.000000] node daemon: unbound 192.0.2.200/32 on fixture0'
  victim=fixture-node
  victim_vip=192.0.2.200
  timed_sentinel_fail() { printf '%s\n' "${timing_context}"; }
  sleep() { :; }
  wait_until() { shift; "$@"; }
  node_lacks_all_vips() { return 0; }
  victim_still_has_vip() { return 0; }
  evid() { :; }
  check() {
    local title="$1"; shift
    if ! "$@"; then
      [[ "${title}" == 'reset streak observes the full three-second lower bound' ]] || unexpected=1
      failures=$((failures + 1))
    fi
  }
  body="$(sed -n '/^second_context=/,/^check "all VIPs move/{ /^check "all VIPs move/!p; }' \
    "${HERE}/scenarios/D13_failure_delay_reset.sh")"
  [[ -n "${body}" ]] || return 1
  eval "${body}" || return 1
  [[ "${failures}" -eq 1 && "${unexpected}" -eq 0 ]]
)

d13_scenario_waits_for_journal_delivery() (
  local marker body failures=0 second_elapsed_ms
  marker="$(mktemp)" || exit 1
  trap 'rm -f -- "${marker}"' EXIT
  victim=fixture-node
  victim_vip=192.0.2.200
  timed_sentinel_fail() { printf 'fixture-context\n'; }
  vip_release_elapsed_ms() {
    printf 'read\n' >> "${marker}"
    [[ "$(wc -l < "${marker}")" -gt 1 ]] || return 1
    printf '3000\n'
  }
  sleep() { :; }
  wait_until() {
    local attempt; shift
    for attempt in 1 2 3; do "$@" && return 0; done
    return 1
  }
  victim_still_has_vip() { return 0; }
  node_lacks_all_vips() { return 0; }
  evid() { :; }
  check() { shift; "$@" || failures=$((failures + 1)); }
  body="$(sed -n '/^second_context=/,/^check "all VIPs move/{ /^check "all VIPs move/!p; }' \
    "${HERE}/scenarios/D13_failure_delay_reset.sh")"
  eval "${body}" || return 1
  [[ "${failures}" -eq 0 && "${second_elapsed_ms}" == 3000 &&
    "$(wc -l < "${marker}")" -eq 2 ]]
)

for evidence_mode in absent present transport partial malformed; do
  assert "C5 removed VIP evidence: ${evidence_mode}" removed_vip_evidence_is_fail_closed "${evidence_mode}"
done
assert "D13 measures the first matching unbind, not observation time" release_timing_uses_first_matching_event
assert "D13 accepts the exact configured lower bound" release_timing_accepts_exact_lower_bound
for evidence_mode in transport empty malformed old other-interface; do
  assert "D13 rejects ${evidence_mode} journal evidence" \
    release_timing_rejects_unreadable_evidence "${evidence_mode}"
done
assert "D13 rejects invalid timing context before SSH" release_timing_rejects_invalid_context_before_ssh
for capture_mode in success transport malformed extra multiline; do
  assert "D13 timed failure capture: ${capture_mode}" \
    timed_failure_capture_validates_remote_result "${capture_mode}"
done
assert "D13 scenario fails an early release even when observed later" \
  d13_scenario_rejects_early_release_despite_late_polling
assert "D13 waits for journal delivery without changing the event duration" \
  d13_scenario_waits_for_journal_delivery

scenario_timing_uses_event_not_poll_time() (
  local scenario="${1:?}" mode="${2:?}" body expected_title
  local timing_context timing_mode=ok timing_log simulated_ms=0 actual_ms=5000
  local failures=0 unexpected=0 delay_secs=6 ip=fixture-node victim=fixture-node
  local victim_vip=192.0.2.200 reads
  reads="$(mktemp)" || return 1
  trap 'rm -f -- "${reads}"' EXIT
  release_timing_evidence_fixture
  VIPS=(192.0.2.200 192.0.2.201)
  [[ "${mode}" == early ]] || actual_ms=6000
  case "${scenario}" in
    D5)
      expected_title='first failback is not earlier than 6s'
      timing_log="[  10$((actual_ms / 1000)).000000] node daemon: bound 192.0.2.201/32 on fixture0"
      body="$(sed -n '/^evid "restoring health/,/^check "node re-enters/{ /^check "node re-enters/!p; }' \
        "${HERE}/scenarios/D5_failback_timing.sh")"
      ;;
    D11)
      expected_title='release is not earlier than configured six-second delay'
      timing_log="[  10$((actual_ms / 1000)).000000] node daemon: unbound 192.0.2.200/32 on fixture0"
      body="$(sed -n '/^evid "failing .*six-second delay/,/^replacement=/{ /^replacement=/!p; }' \
        "${HERE}/scenarios/D11_failover_delay_nopreempt.sh")"
      ;;
  esac
  if [[ "${mode}" == other-early ]]; then
    if [[ "${scenario}" == D11 ]]; then
      timing_log+=$'\n[  105.000000] node daemon: unbound 192.0.2.201/32 on fixture0'
    else
      timing_log+=$'\n[  105.000000] node daemon: bound 192.0.2.200/32 on fixture0'
    fi
  fi
  node_sh() {
    printf 'read\n' >> "${reads}"
    [[ "${mode}" != missing ]] || return 255
    if [[ "${mode}" == delayed && "$(wc -l < "${reads}")" -eq 1 ]]; then return 1; fi
    printf '%s\n' "${timing_log}"
  }
  date() { printf '%s\n' "$((simulated_ms / 1000))"; }
  now_ms() { printf '%s\n' "${simulated_ms}"; }
  sleep() { simulated_ms=$((simulated_ms + $1 * 1000)); }
  sentinel_recover() { :; }
  timed_sentinel_recover() { printf '%s\n' "${timing_context}"; }
  set_unhealthy_async() { HEALTH_FAILURE_CONTEXT="${timing_context}"; RGW_MUTATED=1; }
  node_lacks_all_vips() {
    if [[ "${scenario}" == D5 ]]; then (( simulated_ms < actual_ms ));
    else (( simulated_ms >= actual_ms )); fi
  }
  node_has_any_vip() { (( simulated_ms >= actual_ms )); }
  victim_still_has_vip() { (( simulated_ms < actual_ms )); }
  evid() { :; }
  check() {
    local title="$1"; shift
    if ! "$@"; then
      if [[ ( "${mode}" == early || "${mode}" == other-early ) &&
        "${title}" != "${expected_title}" ]]; then unexpected=1; fi
      failures=$((failures + 1))
    fi
  }
  [[ -n "${body}" ]] || return 1
  eval "${body}" || return 1
  case "${mode}" in
    early|other-early) [[ "${failures}" -eq 1 && "${unexpected}" -eq 0 ]] ;;
    missing) [[ "${failures}" -gt 0 ]] ;;
    delayed) [[ "${failures}" -eq 0 && "$(wc -l < "${reads}")" -eq 2 ]] ;;
    exact) [[ "${failures}" -eq 0 && -s "${reads}" ]] ;;
  esac
)

bind_timing_uses_first_configured_vip() (
  local timing_context timing_mode=ok timing_log
  release_timing_evidence_fixture
  VIPS=(192.0.2.200 192.0.2.201)
  timing_log='[   99.000000] node daemon: bound 192.0.2.200/32 on fixture0
[  101.000000] node daemon: unbound 192.0.2.200/32 on fixture0
[  102.000000] node daemon: bound 192.0.2.202/32 on fixture0
[  103.000000] node daemon: bound 192.0.2.200/32 on fixture1
[  106.000000] node daemon: bound 192.0.2.200/32 on fixture0
[  105.000000] node daemon: bound 192.0.2.201/32 on fixture0'
  [[ "$(vip_bind_elapsed_ms fixture-node "${timing_context}")" == 5000 ]]
)

bind_timing_rejects_invalid_evidence() (
  local timing_context timing_mode="${1:?}" timing_log
  release_timing_evidence_fixture
  VIPS=(192.0.2.200 192.0.2.201)
  case "${timing_mode}" in
    empty) timing_log='' ;;
    malformed) timing_log='[ 106.000] node daemon: bound 192.0.2.200/32 on fixture0' ;;
    old) timing_log='[  99.000000] node daemon: bound 192.0.2.200/32 on fixture0' ;;
    transport) timing_log='[ 106.000000] node daemon: bound 192.0.2.200/32 on fixture0' ;;
    unbind) timing_log='[ 106.000000] node daemon: unbound 192.0.2.200/32 on fixture0' ;;
    interface) timing_log='[ 106.000000] node daemon: bound 192.0.2.200/32 on fixture1' ;;
    vip) timing_log='[ 106.000000] node daemon: bound 192.0.2.202/32 on fixture0' ;;
  esac
  ! vip_bind_elapsed_ms fixture-node "${timing_context}" >/dev/null
)

timed_node_command_requires_successful_mutation() (
  local mode="${1:?}" calls result=0 context
  calls="$(mktemp)" || return 1
  trap 'rm -f -- "${calls}"' EXIT
  NODE_IPS=(fixture-node)
  NODE_INSTANCES=(fixture-instance)
  export timing_calls="${calls}" timing_case="${mode}"
  systemctl() {
    [[ "$*" == *'InvocationID'* && "$*" == *'keepafloatd@fixture-instance'* ]] || return 64
    [[ "${timing_case}" != identity ]] || { printf 'invalid\n'; return 0; }
    printf 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n'
  }
  python3() {
    [[ "$*" == *'time.monotonic_ns()'* ]] || return 64
    printf 'timestamp\n' >> "${timing_calls}"
    printf 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 100000000\n'
  }
  fixture_change() {
    [[ "$(<"${timing_calls}")" == timestamp ]] || return 64
    printf 'mutation\n' >> "${timing_calls}"
    printf 'fixture effect output\n'
    [[ "${timing_case}" != mutation ]] || return 42
  }
  export -f systemctl python3 fixture_change
  node_sh() { bash -c "$2"; }
  context="$(timed_node_command fixture-node fixture_change)" || result=$?
  case "${mode}" in
    success) [[ "${result}" -eq 0 && "${context}" == 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 100000000' && "$(<"${calls}")" == $'timestamp\nmutation' ]] ;;
    mutation) [[ "${result}" -ne 0 && -z "${context}" && "$(<"${calls}")" == $'timestamp\nmutation' ]] ;;
    identity) [[ "${result}" -eq 1 && -z "${context}" && ! -s "${calls}" ]] ;;
  esac
)

timed_health_actions_preserve_the_real_mutation() (
  local mode="${1:?}" calls result=0
  calls="$(mktemp)" || return 1
  trap 'rm -f -- "${calls}"' EXIT
  RGW_MUTATED=0
  HEALTH_FAILURE_CONTEXT=stale
  timed_node_command() {
    [[ "$1" == fixture-node ]] || return 64
    printf '%s\n' "$2" > "${calls}"
    [[ "${mode}" != failure ]] || return 255
    printf 'captured-context\n'
  }
  node_sh() { printf 'unexpected-untimed-SSH\n' > "${calls}"; return 64; }
  if [[ "${mode}" == recovery ]]; then
    [[ "$(timed_sentinel_recover fixture-node)" == captured-context ]] || return 1
    grep -F -q 'rm -f /run/keepafloatd-unhealthy' "${calls}"
  else
    set_unhealthy_async fixture-node timed || result=$?
    [[ "${RGW_MUTATED}" -eq 1 ]] || return 1
    grep -F -q 'systemctl stop --no-block haproxy' "${calls}" || return 1
    grep -F -q 'ceph-radosgw@' "${calls}" || return 1
    if [[ "${mode}" == failure ]]; then
      [[ "${result}" -ne 0 && -z "${HEALTH_FAILURE_CONTEXT}" ]]
    else
      [[ "${result}" -eq 0 && "${HEALTH_FAILURE_CONTEXT}" == captured-context ]]
    fi
  fi
)

service_failure_propagates_errors() (
  local mode="${1:?}" timing="${2-}" calls result=0
  calls="$(mktemp)" || return 1
  trap 'rm -f -- "${calls}"' EXIT
  RGW_MUTATED=0
  NODE_IPS=(fixture-node)
  NODE_INSTANCES=(fixture-instance)
  export service_calls="${calls}" service_case="${mode}"
  find() {
    [[ "${service_case}" != lookup ]] || return 1
    [[ "${service_case}" != missing ]] || return 0
    printf '/fixture/ceph-rgw.fixture\n'
  }
  systemctl() {
    if [[ "$*" == 'show --property=InvocationID --value keepafloatd@fixture-instance' ]]; then
      printf 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n'
      return 0
    fi
    printf '%s\n' "$*" >> "${service_calls}"
    [[ "$*" == 'stop --no-block haproxy ceph-radosgw@rgw.fixture' ]] || return 64
    [[ "${service_case}" != stop ]]
  }
  python3() {
    [[ "$*" == *'time.monotonic_ns()'* ]] || return 64
    printf 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 100000000\n'
  }
  export -f find systemctl python3
  node_sh() { bash -c "$2"; }
  set_unhealthy_async fixture-node "${timing}" || result=$?
  [[ "${RGW_MUTATED}" -eq 1 ]] || return 1
  case "${mode}" in
    success) [[ "${result}" -eq 0 && -s "${calls}" ]] ;;
    stop) [[ "${result}" -ne 0 && -s "${calls}" ]] ;;
    lookup|missing) [[ "${result}" -ne 0 && ! -s "${calls}" ]] ;;
  esac
)

timing_argument_guards_run_before_ssh() (
  local calls result
  calls="$(mktemp)" || return 1
  trap 'rm -f -- "${calls}"' EXIT
  node_sh() { printf 'called\n' >> "${calls}"; return 255; }
  result=0
  set_unhealthy_async fixture-node unsupported >/dev/null || result=$?
  [[ "${result}" -eq 1 ]] || return 1
  result=0
  vip_event_elapsed_ms fixture-node '' invalid 192.0.2.200 >/dev/null || result=$?
  [[ "${result}" -eq 1 ]] || return 1
  result=0
  vip_event_elapsed_ms fixture-node '' bound >/dev/null || result=$?
  [[ "${result}" -eq 1 && ! -s "${calls}" ]]
)

for scenario in D5 D11; do
  for timing_case in early exact missing delayed other-early; do
    assert "${scenario} timing oracle: ${timing_case}" \
      scenario_timing_uses_event_not_poll_time "${scenario}" "${timing_case}"
  done
done
assert "D5 measures the first returning configured VIP, not an unbind" bind_timing_uses_first_configured_vip
for timing_case in empty malformed old transport unbind interface vip; do
  assert "D5 rejects ${timing_case} bind evidence" bind_timing_rejects_invalid_evidence "${timing_case}"
done
for timing_case in success mutation identity; do
  assert "timed remote command: ${timing_case}" timed_node_command_requires_successful_mutation "${timing_case}"
done
for timing_case in recovery service failure; do
  assert "timed health action: ${timing_case}" timed_health_actions_preserve_the_real_mutation "${timing_case}"
done
for service_case in success stop lookup missing; do
  assert "untimed service failure: ${service_case}" service_failure_propagates_errors "${service_case}"
  assert "timed service failure: ${service_case}" service_failure_propagates_errors "${service_case}" timed
done
assert "invalid timing arguments never contact a node" timing_argument_guards_run_before_ssh
