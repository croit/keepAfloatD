#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT

process() {
  local root=$1 pid=$2 name=$3 state=$4
  mkdir -p "${root}/${pid}"
  printf '%s\n' "${name}" >"${root}/${pid}/comm"
  printf 'Name:\t%s\nState:\t%s\n' "${name}" "${state}" \
    >"${root}/${pid}/status"
}

check() {
  local expected=$1 label=$2 root=$3 actual=0
  sh "${script_dir}/address-commands-finished.sh" "${root}" || actual=$?
  if [[ "${actual}" != "${expected}" ]]; then
    printf 'FAIL %s: expected %s, got %s\n' "${label}" "${expected}" "${actual}" >&2
    exit 1
  fi
  printf 'PASS %s\n' "${label}"
}

mkdir "${scratch}/empty"
check 0 'no address commands' "${scratch}/empty"
process "${scratch}/zombie" 10 ip 'Z (zombie)'
check 0 'exited child need not be reaped' "${scratch}/zombie"
process "${scratch}/dead" 10 ip 'X (dead)'
check 0 'dead child cannot change addresses' "${scratch}/dead"
for state in R S D T; do
  process "${scratch}/${state}" 10 ip "${state}"
  check 1 "live ip state ${state} blocks mutation" "${scratch}/${state}"
done
process "${scratch}/mixed" 10 ip Z
process "${scratch}/mixed" 11 ip S
check 1 'zombie cannot hide live command' "${scratch}/mixed"
process "${scratch}/other" 10 'other process' S
check 0 'unrelated process does not block mutation' "${scratch}/other"
process "${scratch}/unknown" 10 ip ''
check 1 'missing state blocks mutation' "${scratch}/unknown"
mkdir -p "${scratch}/missing/10"
check 1 'unreadable live process blocks mutation' "${scratch}/missing"
check 1 'missing proc root blocks mutation' "${scratch}/absent"
