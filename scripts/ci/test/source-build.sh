#!/usr/bin/env sh
set -eu

# Prove the tarball from build/source-tarball.sh is self-contained: unpack it and
# build entirely offline. Catches a missing vendor dir or a stale Cargo.lock
# before a release ships, since the rpm packagebuilder builds the same way.

. ./scripts/shared/rust-env.sh

cd "${CI_PROJECT_DIR:-$(pwd)}"

if [ -z "${TARBALL:-}" ]; then
  set -- dist/keepafloatd-*.tar.gz
  [ -e "$1" ] || { echo "No source tarball in dist/" >&2; exit 1; }
  [ "$#" -eq 1 ] || {
    echo "Multiple source tarballs in dist/; set TARBALL to the exact archive" >&2
    exit 1
  }
  TARBALL="$1"
fi
[ -f "${TARBALL}" ] || { echo "Source tarball not found: ${TARBALL}" >&2; exit 1; }

STAGING="$(mktemp -d)"
trap 'rm -rf "${STAGING}"' EXIT
tar -xzf "${TARBALL}" -C "${STAGING}"
SRC="$(find "${STAGING}" -maxdepth 1 -type d -name 'keepafloatd-*')"

( cd "${SRC}" && ./scripts/ci/test/realcluster-harness.sh )
( cd "${SRC}" && cargo build --release --locked --offline )
echo "Offline build from ${TARBALL} succeeded"
