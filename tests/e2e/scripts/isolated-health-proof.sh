#!/usr/bin/env bash
# Run as root on Linux; every VIP effect is dry-run inside a disposable network namespace.
# KEEP_AFLOATD_BIN must name the candidate executable.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${KEEP_AFLOATD_BIN:?set KEEP_AFLOATD_BIN to the candidate executable}"
export KEEP_AFLOATD_BIN
test -x "$KEEP_AFLOATD_BIN"
python3 "$HERE/isolated-health-proof.py" --proof-control preflight
own_control=0
if [[ -z "${KAFD_PROOF_CONTROL_DIR:-}" ]]; then
  KAFD_PROOF_CONTROL_DIR=$(mktemp -d /tmp/kafd-proof-local.XXXXXX)
  own_control=1
fi
export KAFD_PROOF_CONTROL_DIR
namespace=$(python3 "$HERE/isolated-health-proof.py" --proof-control create-namespace "$KAFD_PROOF_CONTROL_DIR")
cleanup() {
  local result=$?
  trap - EXIT
  if ((own_control)); then
    python3 "$HERE/isolated-health-proof.py" --proof-control cleanup "$KAFD_PROOF_CONTROL_DIR" || result=1
  else
    python3 "$HERE/isolated-health-proof.py" --proof-control cleanup-namespace "$KAFD_PROOF_CONTROL_DIR" || result=1
  fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
ip -n "$namespace" link set lo up
# Python arms a hard watchdog, adding only the candidate's logged startup fences.
ip netns exec "$namespace" \
  python3 "$HERE/isolated-health-proof.py"
