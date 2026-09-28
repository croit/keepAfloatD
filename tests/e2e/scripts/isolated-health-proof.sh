#!/usr/bin/env bash
# Run as root on Linux; every VIP effect is dry-run inside a disposable network namespace.
# KEEP_AFLOATD_BIN must name the candidate executable.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${KEEP_AFLOATD_BIN:?set KEEP_AFLOATD_BIN to the candidate executable}"
export KEEP_AFLOATD_BIN
test -x "$KEEP_AFLOATD_BIN"
namespace="kafd-proof-$$"
test ! -e "/run/netns/$namespace"
ip netns add "$namespace"
cleanup() {
  local pids
  pids="$(ip netns pids "$namespace")"
  if [[ -n "$pids" ]]; then
    kill -KILL $pids 2>/dev/null || true
  fi
  ip netns del "$namespace"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
ip -n "$namespace" link set lo up
timeout --signal=TERM --kill-after=5 45 ip netns exec "$namespace" \
  python3 "$HERE/isolated-health-proof.py"
