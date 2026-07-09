#!/usr/bin/env sh
set -eu

. ./scripts/shared/rust-env.sh

if ! command -v cargo-deny >/dev/null 2>&1; then
  cargo install --locked cargo-deny
fi
