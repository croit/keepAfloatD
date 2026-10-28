#!/usr/bin/env sh
set -eu

. ./scripts/shared/rust-env.sh

WORKDIR="${CI_PROJECT_DIR:-/app}"
cd "${WORKDIR}"

./scripts/ci/test/package-security.sh
# Run CPU-bound restart children separately, without shortening the rest of the suite.
cargo test --all-targets --locked -- --skip cluster_test::restart::
cargo test --all-targets --locked cluster_test::restart:: -- --test-threads=1
