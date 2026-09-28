#!/usr/bin/env sh
set -eu

. ./scripts/shared/rust-env.sh

# cargo-deny fetches the RustSec advisory database with the git CLI, which the slim Rust image
# does not ship. GitHub runners already have git; the guard keeps this a no-op there.
if ! command -v git >/dev/null 2>&1 && command -v apt-get >/dev/null 2>&1; then
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends git ca-certificates
  rm -rf /var/lib/apt/lists/*
fi

if ! command -v cargo-deny >/dev/null 2>&1; then
  cargo install --locked cargo-deny
fi
