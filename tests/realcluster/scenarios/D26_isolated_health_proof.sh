#!/usr/bin/env bash
# Three real candidate daemons on the lab kernel; VIP effects are dry-run in a private namespace.
SCENARIO_NAME=D26_isolated_health_proof
source "$(dirname "$0")/../scenario.sh"
scenario_start "blocked health probe cannot retain an isolated leader's VIP; original processes recover"

payload="$(tar -C "${HERE}/../e2e/scripts" -czf - \
  isolated-health-proof.sh isolated-health-proof.py | base64 -w0)"

run_native_proof_regression() {
  node_sh "${NODE_IPS[0]}" "
    set -euo pipefail
    work=\$(mktemp -d /run/kafd-proof-campaign.XXXXXX)
    cleanup() {
      rm -f \"\$work/isolated-health-proof.sh\" \"\$work/isolated-health-proof.py\"
      rmdir \"\$work\"
    }
    trap cleanup EXIT
    printf '%s' '${payload}' | base64 -d | tar -xzf - -C \"\$work\"
    KEEP_AFLOATD_BIN=/usr/bin/keepafloatd bash \"\$work/isolated-health-proof.sh\"
  "
}

check "actual candidate fences a blocked-probe leader and recovers without restart" \
  run_native_proof_regression
check "the real service cluster remains uniquely available" wait_for_available_cluster
scenario_end
