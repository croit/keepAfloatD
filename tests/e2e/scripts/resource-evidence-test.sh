#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT
export ARTIFACT_DIR="${scratch}"
. "${script_dir}/lib.sh"

# No Docker access: exercise the ordinary artifact entry point with local stubs.
compose() { return 0; }
service_is_running() { return 1; }
capture_resource_evidence() { printf 'numeric-resource-sentinel\n'; }
capture_cluster_artifacts sample
if [[ ! -f "${scratch}/sample/resources.txt" ]] ||
   ! grep -q '^numeric-resource-sentinel$' "${scratch}/sample/resources.txt"; then
  echo 'FAIL ordinary artifact collector omitted resource evidence' >&2
  exit 1
fi
echo 'PASS ordinary artifact collector includes bounded resource evidence'

docker() {
  case "$1" in
    compose) printf '0123456789ab\n' ;;
    inspect)
      [[ "$2" == --format && "$3" == 'pid='* && "$3" != *'.Config'* ]] || return 91
      printf 'pid=123 oom_killed=false cpu_quota=200000\n' ;;
    exec)
      [[ "$2" == 0123456789ab && "$3" == sh && "$4" == -c ]] || return 92
      [[ "$5" == *'/sys/fs/cgroup/cpu.stat'* && "$5" != *cmdline* && "$5" != *environ* ]] || return 93
      printf 'nr_throttled 7\n' ;;
    stats)
      [[ "$2" == --no-stream && "$3" == --format && "$5" == 0123456789ab ]] || return 94
      printf '0123456789ab cpu=1.0%% pids=3\n' ;;
    *) return 95 ;;
  esac
}
sh() { printf 'some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n'; }
timeout() {
  [[ "$1" == -k && "$2" == 1s && "$3" == 2s ]] || return 96
  shift 3
  "$@"
}
export -f docker sh timeout
output=$(bash "${script_dir}/resource-evidence.sh" unused.yml fixture node-a)
for expected in section=job-visible-resources nr_throttled section=docker-stats cpu_quota; do
  grep -q "${expected}" <<<"${output}" || { echo "FAIL missing ${expected}" >&2; exit 1; }
done
if grep -Eq 'probe_status=[1-9]|secret-sentinel' <<<"${output}"; then
  echo 'FAIL unsafe or unsuccessful resource probe' >&2
  exit 1
fi
echo 'PASS bounded, scoped numeric Docker and pressure evidence'

docker() { printf 'secret-sentinel\n' >&2; return 42; }
export -f docker
output=$(bash "${script_dir}/resource-evidence.sh" unused.yml fixture node-a)
grep -q 'container_unavailable service=node-a status=42' <<<"${output}"
if grep -q secret-sentinel <<<"${output}"; then
  echo 'FAIL Docker error leaked into artifacts' >&2
  exit 1
fi
echo 'PASS unavailable resource probes preserve evidence without leaking errors'

output=$(bash -c '
  . "$1/lib.sh"
  timeout() {
    [[ "$1" == -k && "$2" == 1s && "$3" == 12s ]] || return 97
    return 124
  }
  capture_resource_evidence
' fixture "${script_dir}")
grep -qx 'resource_collection_unavailable status=124' <<<"${output}"
echo 'PASS whole resource collector budget is finite and failure is nonfatal'

sh() { bash "$@"; }
test() {
  [[ "$1" == -r ]] && return 0
  builtin test "$@"
}
head() {
  if [[ "$1" == -c && "$2" == 1024 ]]; then
    printf 'partial-metric\n'
    printf 'secret-read-error-sentinel\n' >&2
    return 42
  fi
  command head "$@"
}
export -f sh test head
output=$(bash "${script_dir}/resource-evidence.sh" unused.yml fixture)
if ! grep -q 'unavailable status=42' <<<"${output}"; then
  echo 'FAIL partial metric read was reported as successful' >&2
  printf '%s\n' "${output}" >&2
  exit 1
fi
grep -q partial-metric <<<"${output}"
if grep -q secret-read-error-sentinel <<<"${output}"; then
  echo 'FAIL metric read error leaked into artifacts' >&2
  exit 1
fi
echo 'PASS partial metric reads are explicitly unavailable without leaking errors'
