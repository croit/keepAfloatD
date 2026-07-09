#!/usr/bin/env sh
set -eu

. ./scripts/shared/rust-env.sh

WORKDIR="${CI_PROJECT_DIR:-/app}"
cd "${WORKDIR}"

cargo tarpaulin --all-targets --out Xml --output-dir coverage
cp coverage/cobertura.xml "${WORKDIR}/cobertura.xml"
