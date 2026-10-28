#!/usr/bin/env sh
set -eu

. ./scripts/shared/rust-env.sh

WORKDIR="${CI_PROJECT_DIR:-/app}"
cd "${WORKDIR}"

# Merge both mandatory test groups before enforcing the unchanged coverage floors.
cargo tarpaulin --config scripts/ci/test/coverage.toml
./scripts/ci/test/coverage-per-file-test.sh
./scripts/ci/test/coverage-per-file.sh coverage/cobertura.xml
cp coverage/cobertura.xml "${WORKDIR}/cobertura.xml"
