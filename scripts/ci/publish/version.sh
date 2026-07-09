#!/usr/bin/env sh
set -eu

if [ -z "${PUBLISH_REF:-}" ]; then
  echo "PUBLISH_REF is required" >&2
  exit 1
fi

echo "Checking out ${PUBLISH_REF}..."
git fetch origin --quiet "+refs/heads/*:refs/remotes/origin/*" --tags
git checkout --quiet "${PUBLISH_REF}"

COMMIT=$(git rev-parse HEAD)
echo "Commit: ${COMMIT}"

YYMM=$(date +%y%m)
existing=$(git tag -l "v${YYMM}.*" | grep -E "^v${YYMM}\.[0-9]+$" | sort -t. -k2 -n | tail -1 || true)

if [ -n "${existing}" ]; then
  MINOR=$(( ${existing##*.} + 1 ))
else
  MINOR=0
fi

VERSION="v${YYMM}.${MINOR}"
PREV_TAG=$(git tag --merged "${COMMIT}" -l "v[0-9]*" | grep -E "^v[0-9]{4}\.[0-9]+$" | sort -t. -k1,1 -k2,2n | tail -1 || true)

echo "Version: ${VERSION}"
echo "Previous tag: ${PREV_TAG:-<none>}"

printf 'VERSION=%s\nCOMMIT=%s\nPREV_TAG=%s\n' "${VERSION}" "${COMMIT}" "${PREV_TAG:-}" > version.env
