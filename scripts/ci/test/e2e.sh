#!/usr/bin/env sh
set -eu

export KEEPAFLOATD_IMAGE="${KEEPAFLOATD_IMAGE:-${IMAGE_TAG:?IMAGE_TAG is required}}"

# 3-node suite (scenarios/), issue-22 timing/nopreempt regressions (scenarios22/), then the 5-node
# minimal-movement suite (scenarios5/, separate compose). set -eu propagates any failure.
bash ./tests/e2e/scripts/run.sh
bash ./tests/e2e/scripts/run22.sh
bash ./tests/e2e/scripts/run5.sh
bash ./tests/e2e/scripts/run26.sh
