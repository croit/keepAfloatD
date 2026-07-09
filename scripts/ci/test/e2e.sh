#!/usr/bin/env sh
set -eu

export KEEPAFLOATD_IMAGE="${KEEPAFLOATD_IMAGE:-${IMAGE_TAG:?IMAGE_TAG is required}}"

# 3-node suite (scenarios/), then the 5-node minimal-movement suite (scenarios5/, separate compose).
# set -eu propagates a failure in either suite to the CI job.
bash ./tests/e2e/scripts/run.sh
bash ./tests/e2e/scripts/run5.sh
