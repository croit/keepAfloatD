#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly VIP="${VIPS[0]}"
holder="$(wait_for_stable_vip_holder "${VIP}" 15 3)" || {
  dump_cluster_diagnostics
  fail "${VIP} did not settle on one holder before the timing check"
  exit 1
}

vip_moved_from() {
  local current
  current="$(holder_for_vip "$1")"
  [[ "${current}" != "$2" && "${current}" != "none" && "${current}" != duplicate:* ]]
}

# A direct bind check two seconds into the six-second delay proves the setting is not merely
# accepted by YAML. One container query avoids consuming the delay through repeated remote probes.
set_node_unhealthy "${holder}"
sleep 2
node_has_vip_bound "${holder}" "${VIP}" || {
  dump_cluster_diagnostics
  fail "${VIP} left ${holder} before failover_delay_secs elapsed"
  exit 1
}

wait_until 15 vip_moved_from "${VIP}" "${holder}" || {
  dump_cluster_diagnostics
  fail "${VIP} did not fail over after failover_delay_secs elapsed"
  exit 1
}
assert_node_lacks_all_vips "${holder}"
assert_unique_holders
log "${VIP} stayed on ${holder} during the delay and failed over afterward"
