#!/usr/bin/env sh
set -eu

# Build a self-contained, offline-buildable source tarball:
#   dist/keepafloatd-<version>.tar.gz  ->  unpacks to keepafloatd-<version>/
# carrying the tracked sources, Cargo.lock, a vendored copy of every crates.io
# dependency, and a .cargo/config.toml that points cargo at vendor/. Downstream
# package builders then build with `cargo build --release --locked --offline`,
# needing no network access to crates.io.

. ./scripts/shared/rust-env.sh

VERSION="${VERSION:-0.0.0}"
VERSION="${VERSION#v}"                 # match the rpm spec's bare %{version}
NAME="keepafloatd-${VERSION}"
WORKDIR="${CI_PROJECT_DIR:-$(pwd)}"

cd "${WORKDIR}"
mkdir -p dist

STAGING="$(mktemp -d)"
trap 'rm -rf "${STAGING}"' EXIT

# Snapshot exactly the tracked files at HEAD (the release commit in CI). This
# respects .gitignore, so target/, dist/, config.yaml, ... never leak in.
git archive --format=tar --prefix="${NAME}/" HEAD | tar -x -C "${STAGING}"

# git archive ships the placeholder version, so stamp the release version into
# the staged tree (working-tree stamps never reach the archive).
( cd "${STAGING}/${NAME}" && VERSION="${VERSION}" "${WORKDIR}/scripts/ci/build/set-version.sh" )

# Vendor every dependency pinned by the committed Cargo.lock and wire up source
# replacement so the unpacked tree builds with no network access.
( cd "${STAGING}/${NAME}" \
  && mkdir -p .cargo \
  && cargo vendor --locked --versioned-dirs vendor >> .cargo/config.toml )

tar -czf "dist/${NAME}.tar.gz" -C "${STAGING}" "${NAME}"
echo "Source tarball ready: dist/${NAME}.tar.gz"
