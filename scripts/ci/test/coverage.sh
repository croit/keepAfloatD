#!/usr/bin/env sh
set -eu

. ./scripts/shared/rust-env.sh

WORKDIR="${CI_PROJECT_DIR:-/app}"
cd "${WORKDIR}"

cargo tarpaulin --all-targets --out Xml --output-dir coverage
./scripts/ci/test/coverage-per-file-test.sh
./scripts/ci/test/coverage-per-file.sh coverage/cobertura.xml
cp coverage/cobertura.xml "${WORKDIR}/cobertura.xml"
