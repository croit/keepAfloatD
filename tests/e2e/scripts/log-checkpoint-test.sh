#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_NODES='alpha beta'
source "${script_dir}/lib.sh"
scratch="$(mktemp -d)"
trap 'rmdir "${scratch}"' EXIT
export TMPDIR="${scratch}"

reset_fixture() {
  alpha_id=a1
  beta_id=b1
  alpha_log=$'2000-01-01T00:00:00.000000001Z ready'
  beta_log=$'2000-01-01T00:00:00.000000002Z old one\n2000-01-01T00:00:00.000000003Z old two\n2000-01-01T00:00:00.000000004Z old three'
  merged_order=(beta alpha)
  broken_service=''
  broken_ps=''
  replaced_during_read=''
}

compose() {
  if [[ "$1 $2 $3" == 'ps -q --all' && "$#" == 4 ]]; then
    [[ "$4" != "${broken_ps}" ]] || return 1
    local id_var="${4}_id"
    printf '%s\n' "${!id_var}"
  elif [[ "$1 $2" == 'logs --no-color' ]]; then
    shift 2
    local service log_var line
    for service in "${merged_order[@]}"; do
      [[ " $* " == *" ${service} "* ]] || continue
      log_var="${service}_log"
      while IFS= read -r line; do
        printf '%s | %s\n' "${service}" "${line}"
      done <<<"${!log_var}"
      [[ "${service}" != "${broken_service}" ]] || return 1
    done
  else
    printf 'unexpected compose command: %s\n' "$*" >&2
    return 97
  fi
}

docker() {
  [[ "$#" == 3 && "$1 $2" == 'logs --timestamps' ]] || {
    printf 'unexpected docker command: %s\n' "$*" >&2
    return 97
  }
  local service id_var log_var
  for service in alpha beta; do
    id_var="${service}_id"
    if [[ "$3" == "${!id_var}" ]]; then
      log_var="${service}_log"
      [[ -z "${!log_var}" ]] || printf '%s\n' "${!log_var}"
      [[ "${service}" != "${broken_service}" ]] || return 1
      if [[ "${service}" == "${replaced_during_read}" ]]; then
        printf -v "${id_var}" '%s' c9
      fi
      return 0
    fi
  done
  return 1
}

expect() {
  local expected="$1" label="$2" status=0
  shift 2
  "$@" || status=$?
  if [[ "${status}" != "${expected}" ]]; then
    printf 'FAIL %s: expected status %s, got %s\n' "${label}" "${expected}" "${status}" >&2
    return 1
  fi
  printf 'PASS %s\n' "${label}"
}

capture_checkpoint() {
  checkpoint="$(log_checkpoint)"
}

reset_fixture
checkpoint="$(log_checkpoint)"
alpha_log+=$'\n2000-01-01T00:00:01.000000001Z expected event'
merged_order=(alpha beta)
expect 0 'new event survives aggregate reordering' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
alpha_log+=$'\n2000-01-01T00:00:00.000000005Z expected event'
checkpoint="$(log_checkpoint)"
beta_log+=$'\n2000-01-01T00:00:01.000000001Z unrelated event'
expect 1 'old event cannot move past the checkpoint' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
checkpoint="$(log_checkpoint)"
alpha_log+=$'\n2000-01-01T00:00:01.000000001Z expected event'
expect 0 'same container restart accepts a new event' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
checkpoint="$(log_checkpoint)"
beta_log+=$'\n2000-01-01T00:00:01.000000001Z expected event 42'
expect 0 'later service events retain extended regex matching' \
  cluster_logs_contain_after "${checkpoint}" 'expected event (41|42)'

reset_fixture
checkpoint="$(log_checkpoint)"
expect 1 'unchanged logs do not satisfy the condition' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
checkpoint="$(log_checkpoint)"
alpha_id=a2
alpha_log+=$'\n2000-01-01T00:00:01.000000001Z expected event'
expect 1 'container replacement invalidates the checkpoint' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
checkpoint="$(log_checkpoint)"
alpha_log=''
beta_log+=$'\n2000-01-01T00:00:01.000000001Z expected event'
expect 1 'lost logs cannot be hidden by a match elsewhere' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
checkpoint="$(log_checkpoint)"
alpha_log=$'2000-01-01T00:00:00.000000001Z changed\n2000-01-01T00:00:01.000000001Z expected event'
expect 1 'changed prefix invalidates the checkpoint' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
checkpoint="$(log_checkpoint)"
alpha_log+=$'\n2000-01-01T00:00:01.000000001Z expected event'
broken_service=alpha
expect 1 'partial Docker output cannot satisfy the condition' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
checkpoint="$(log_checkpoint)"
alpha_log+=$'\n2000-01-01T00:00:01.000000001Z expected event'
broken_service=beta
expect 1 'all containers must be readable even after a match' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
checkpoint="$(log_checkpoint)"
alpha_id=''
expect 1 'missing container is not successful observation' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
checkpoint="$(log_checkpoint)"
broken_ps=alpha
expect 1 'container discovery errors fail closed' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
alpha_log=''
checkpoint="$(log_checkpoint)"
alpha_log='2000-01-01T00:00:01.000000001Z expected event'
expect 0 'first event after an empty log is visible' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
checkpoint="$(log_checkpoint)"
alpha_log+=$'\n2000-01-01T00:00:01.000000001Z expected event'
replaced_during_read=alpha
expect 1 'replacement during observation fails closed' \
  cluster_logs_contain_after "${checkpoint}" 'expected event'

reset_fixture
broken_service=beta
expect 1 'checkpoint capture rejects partial Docker output' capture_checkpoint

reset_fixture
broken_ps=alpha
expect 1 'checkpoint capture rejects discovery errors' capture_checkpoint

reset_fixture
alpha_id=''
expect 1 'checkpoint capture requires every container' capture_checkpoint

reset_fixture
alpha_id=$'a1\na2'
expect 1 'ambiguous service identity is rejected' capture_checkpoint

reset_fixture
replaced_during_read=alpha
expect 1 'replacement during checkpoint capture fails closed' capture_checkpoint

reset_fixture
checkpoint="$(log_checkpoint)"
alpha_log+=$'\n2000-01-01T00:00:01.000000001Z expected event'
expect 1 'legacy global offsets cannot be used as container checkpoints' \
  cluster_logs_contain_after 4 'expected event'
expect 1 'incomplete checkpoint cannot hide an unread service' \
  cluster_logs_contain_after "${checkpoint%$'\n'*}" 'expected event'
expect 1 'unexpected checkpoint records are rejected' \
  cluster_logs_contain_after "${checkpoint}"$'\nextra' 'expected event'
expect 0 'existing wait API accepts an opaque checkpoint' \
  wait_for_log_any_after "${checkpoint}" 0 'expected event'

reset_fixture
checkpoint="$(log_checkpoint)"
status=0
message="$(wait_for_log_any_after "${checkpoint}" 0 'expected event' 2>&1)" || status=$?
[[ "${status}" == 1 && "${message}" == '[e2e] ERROR: timed out waiting for post-checkpoint log pattern: expected event' ]] || {
  printf 'FAIL exhausted wait must retain its failure and diagnostic\n' >&2
  exit 1
}
printf 'PASS exhausted wait retains its failure and diagnostic\n'

if [[ -n "$(ls -A "${scratch}")" ]]; then
  printf 'FAIL checkpoint helpers leaked temporary files\n' >&2
  exit 1
fi
printf 'PASS successful and failed observations clean temporary files\n'

printf 'All log checkpoint tests passed\n'
