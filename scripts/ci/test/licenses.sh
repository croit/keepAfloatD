#!/usr/bin/env sh
set -eu

. ./scripts/shared/rust-env.sh

WORKDIR="${CI_PROJECT_DIR:-/app}"
cd "${WORKDIR}"

cargo deny check licenses
