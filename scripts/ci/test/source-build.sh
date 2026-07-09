#!/usr/bin/env sh
set -eu

# Prove the tarball from build/source-tarball.sh is self-contained: unpack it and
# build entirely offline. Catches a missing vendor dir or a stale Cargo.lock
# before a release ships, since the rpm packagebuilder builds the same way.

. ./scripts/shared/rust-env.sh

cd "${CI_PROJECT_DIR:-$(pwd)}"

TARBALL="$(find dist -maxdepth 1 -name 'keepafloatd-*.tar.gz' | sort | tail -1)"
[ -n "${TARBALL}" ] || { echo "No source tarball in dist/" >&2; exit 1; }

STAGING="$(mktemp -d)"
trap 'rm -rf "${STAGING}"' EXIT
tar -xzf "${TARBALL}" -C "${STAGING}"
SRC="$(find "${STAGING}" -maxdepth 1 -type d -name 'keepafloatd-*')"

( cd "${SRC}" && cargo build --release --locked --offline )
echo "Offline build from ${TARBALL} succeeded"
