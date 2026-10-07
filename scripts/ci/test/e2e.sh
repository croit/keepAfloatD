#!/usr/bin/env sh
set -eu

export KEEPAFLOATD_IMAGE="${KEEPAFLOATD_IMAGE:-${IMAGE_TAG:?IMAGE_TAG is required}}"

bash ./tests/e2e/scripts/address-commands-test.sh
bash ./tests/e2e/scripts/resource-evidence-test.sh
bash ./tests/e2e/scripts/log-checkpoint-test.sh
bash ./tests/e2e/scripts/scenario-runner-test.sh

# Separate jobs keep cold-start safety pauses within the runner's job budget.
case "${E2E_SUITE:-all}" in
  all)
    bash ./tests/e2e/scripts/run.sh
    bash ./tests/e2e/scripts/run22.sh
    bash ./tests/e2e/scripts/run5.sh
    bash ./tests/e2e/scripts/run26.sh
    ;;
  core) bash ./tests/e2e/scripts/run.sh ;;
  timing) bash ./tests/e2e/scripts/run22.sh ;;
  membership)
    bash ./tests/e2e/scripts/run5.sh
    bash ./tests/e2e/scripts/run26.sh
    ;;
  *) printf 'Unknown E2E_SUITE: %s\n' "$E2E_SUITE" >&2; exit 2 ;;
esac
