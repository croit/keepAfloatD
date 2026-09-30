#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
HARNESS="${ROOT}/tests/realcluster"

command -v python3 >/dev/null || {
  echo "Python 3 is required for real-cluster harness self-tests" >&2
  exit 1
}

for required in README.md env.example.sh lib.sh scenario.sh run-all.sh \
  run-all-guard.sh self-test.sh safety-self-test.sh final-audit.sh soak.sh raft-deadline-probe.py \
  evidence.sh evidence-self-test.sh journal-evidence.py journal-evidence-test.py; do
  if [[ ! -f "${HARNESS}/${required}" ]]; then
    echo "Missing real-cluster harness dependency: ${required}" >&2
    exit 1
  fi
done

scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT

awk -F '|' '$2 ~ /^ *[A-D]?[0-9]+_/ { gsub(/^ +| +$/, "", $2); print $2 }' \
  "${HARNESS}/README.md" | LC_ALL=C sort > "${scratch}/documented"
find "${HARNESS}/scenarios" -type f -name '*.sh' -exec basename {} .sh \; \
  | LC_ALL=C sort > "${scratch}/actual"
if [[ ! -s "${scratch}/actual" ]] || ! diff -u "${scratch}/documented" "${scratch}/actual"; then
  echo "Real-cluster scenarios do not match the README coverage table" >&2
  exit 1
fi

for helper in isolated-health-proof.sh isolated-health-proof.py; do
  test -f "${ROOT}/tests/e2e/scripts/${helper}"
done

mapfile -t shell_files < <(find "${HARNESS}" -type f -name '*.sh' \
  ! -name 'env.sh' ! -path '*/results/*' | sort)
for script in "${shell_files[@]}"; do
  bash -n "${script}"
done

# Reusable sources must not bake in the private topology. RFC 5737 example ranges remain allowed
# in env.example.sh and self-test fixtures.
if grep -RInE \
  --exclude='env.sh' --exclude='REPORT.md' --exclude='FINDINGS.md' \
  --exclude-dir='results' \
  '(10\.[0-9]+\.[0-9]+\.[0-9]+|172\.(1[6-9]|2[0-9]|3[01])\.[0-9]+\.[0-9]+|192\.168\.[0-9]+\.[0-9]+)' \
  "${HARNESS}"; then
  echo "real-cluster reusable sources contain a private topology address" >&2
  exit 1
fi

mkdir "${scratch}/harness"
cp -a "${HARNESS}/." "${scratch}/harness/"
cp "${scratch}/harness/env.example.sh" "${scratch}/harness/env.sh"
export SSH_CTL_DIR="${scratch}/ssh"
bash "${scratch}/harness/self-test.sh"
