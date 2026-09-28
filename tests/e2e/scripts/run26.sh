#!/usr/bin/env bash
set -euo pipefail

# Dedicated config-identity suite. The first scenario rejects real drift; the second proves fields
# ignored by the effective policy do not create a false mismatch.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
export COMPOSE_PROJECT_NAME="keepafloatd-e2e-issue26"
export ARTIFACT_DIR="${ROOT_DIR}/e2e-artifacts/compose26"
export KEEPAFLOATD_E2E_CONFIG_DIR="configs26"

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

readonly DRIFT_SCENARIO="${ROOT_DIR}/tests/e2e/scenarios26/01_config_drift_fenced.sh"
readonly EQUIVALENCE_SCENARIO="${ROOT_DIR}/tests/e2e/scenarios26/02_semantic_equivalence_joins.sh"

down_cluster() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}

trap 'down_cluster' EXIT
reset_cluster
bash "${DRIFT_SCENARIO}"
capture_cluster_artifacts "01_config_drift_fenced" || true

export KEEPAFLOATD_E2E_CONFIG_DIR="configs26-equivalent"
reset_cluster
bash "${EQUIVALENCE_SCENARIO}"
capture_cluster_artifacts "02_semantic_equivalence_joins" || true
