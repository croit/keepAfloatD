#!/usr/bin/env sh
set -eu

# Stamp the release version into Cargo.toml and Cargo.lock so the binary
# (--version) and the .deb/.rpm packages carry the release version instead of
# the repo placeholder. Cargo requires full semver, so vYYMM.N becomes YYMM.N.0.
VERSION="${VERSION:?set VERSION to the release tag, e.g. v2606.2}"
SEMVER="${VERSION#v}"
case "${SEMVER}" in
  *.*.*) ;;
  *.*) SEMVER="${SEMVER}.0" ;;
  *) SEMVER="${SEMVER}.0.0" ;;
esac

# First standalone version line is the [package] version at the top of the file.
sed -i "0,/^version = \"[^\"]*\"/s//version = \"${SEMVER}\"/" Cargo.toml

awk -v ver="${SEMVER}" '
  /^name = "keepafloatd"$/ { in_pkg = 1 }
  in_pkg && /^version = / { sub(/"[^"]*"/, "\"" ver "\""); in_pkg = 0 }
  { print }
' Cargo.lock > Cargo.lock.tmp && mv Cargo.lock.tmp Cargo.lock

grep -q "^version = \"${SEMVER}\"" Cargo.toml
grep -q "^version = \"${SEMVER}\"" Cargo.lock
echo "stamped version ${SEMVER}"
