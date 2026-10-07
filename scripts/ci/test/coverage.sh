#!/usr/bin/env sh
set -eu

. ./scripts/shared/rust-env.sh

WORKDIR="${CI_PROJECT_DIR:-/app}"
cd "${WORKDIR}"

# Real signal tests need about 64 seconds for admission startup; allow
# twice that runtime without changing their readiness or shutdown bounds.
cargo tarpaulin --engine llvm --locked --all-targets --timeout 130 \
  --out Xml --output-dir coverage
./scripts/ci/test/coverage-per-file-test.sh
./scripts/ci/test/coverage-per-file.sh coverage/cobertura.xml
cp coverage/cobertura.xml "${WORKDIR}/cobertura.xml"
