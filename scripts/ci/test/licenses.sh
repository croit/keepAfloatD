#!/usr/bin/env sh
set -eu

. ./scripts/shared/rust-env.sh

WORKDIR="${CI_PROJECT_DIR:-/app}"
cd "${WORKDIR}"

cargo deny check licenses
cargo deny check advisories
./scripts/ci/test/third-party-licenses-test.sh
./scripts/ci/test/third-party-licenses.sh
