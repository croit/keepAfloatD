#!/usr/bin/env sh
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "Usage: $0 REVISION [MANIFEST]" >&2
  exit 2
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
REVISION=$1
MANIFEST=${2:-"${ROOT}/scripts/public/files.txt"}
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT

TRACKED="${TEMP_DIR}/tracked.txt"
REFERENCED="${TEMP_DIR}/referenced.txt"
MANIFEST_SOURCES="${TEMP_DIR}/manifest-sources.txt"
MANIFEST_RUST="${TEMP_DIR}/manifest-rust.txt"
MISSING="${TEMP_DIR}/missing.txt"
STATUS=0

# List tracked regular files under the given paths at REVISION (or in the worktree).
tracked_under() {
  if [ "${REVISION}" = "WORKTREE" ]; then
    git -C "${ROOT}" ls-files -- "$@"
  else
    git -C "${ROOT}" ls-tree -r --name-only "${REVISION}" -- "$@"
  fi
}

report_missing() {
  label=$1
  if [ -s "${MISSING}" ]; then
    while IFS= read -r path; do
      echo "Public manifest omits ${label}: ${path}" >&2
    done < "${MISSING}"
    STATUS=1
  fi
}

awk -F '|' '{ print $1 }' "${MANIFEST}" | LC_ALL=C sort -u > "${MANIFEST_SOURCES}"
awk '/^src\/.*[.]rs$/' "${MANIFEST_SOURCES}" > "${MANIFEST_RUST}"

# Production Rust sources: every tracked src/**/*.rs must be exported.
tracked_under src | awk '/^src\/.*[.]rs$/' | LC_ALL=C sort -u > "${TRACKED}"
comm -23 "${TRACKED}" "${MANIFEST_RUST}" > "${MISSING}"
report_missing "tracked production Rust source"

# Packaging inputs: every tracked file under deploy/ is consumed by cargo-deb, cargo-generate-rpm
# or systemd and must be exported.
tracked_under deploy | LC_ALL=C sort -u > "${TRACKED}"
comm -23 "${TRACKED}" "${MANIFEST_SOURCES}" > "${MISSING}"
report_missing "tracked deploy file"

# Cargo.toml references: every quoted repository-relative path that is tracked at REVISION
# (assets, maintainer scripts, package scripts) must be exported so the public tree builds
# the same packages. Absolute and untracked paths (install destinations, build outputs) are
# ignored because git reports nothing for them.
if [ "${REVISION}" = "WORKTREE" ]; then
  cat "${ROOT}/Cargo.toml"
else
  git -C "${ROOT}" show "${REVISION}:Cargo.toml"
fi | grep -oE '"[A-Za-z0-9_.@/-]+"' | tr -d '"' | grep -v '^/' | LC_ALL=C sort -u \
  | while IFS= read -r candidate; do
      tracked_under "${candidate}"
    done | LC_ALL=C sort -u > "${REFERENCED}"
comm -23 "${REFERENCED}" "${MANIFEST_SOURCES}" > "${MISSING}"
report_missing "Cargo.toml-referenced file"

exit "${STATUS}"
