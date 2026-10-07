#!/usr/bin/env sh
set -eu

if [ -z "${PUBLISH_REF:-}" ]; then
  echo "PUBLISH_REF is required" >&2
  exit 1
fi

if [ -z "${CI_COMMIT_SHA:-}" ]; then
  echo "CI_COMMIT_SHA is required to identify the tested commit" >&2
  exit 1
fi

git fetch origin --quiet "+refs/heads/*:refs/remotes/origin/*" --tags
COMMIT=$(git rev-parse --verify "refs/remotes/origin/${PUBLISH_REF}^{commit}" 2>/dev/null ||
  git rev-parse --verify --end-of-options "${PUBLISH_REF}^{commit}")
if [ "${COMMIT}" != "${CI_COMMIT_SHA}" ]; then
  echo "PUBLISH_REF must resolve to the tested pipeline commit ${CI_COMMIT_SHA}" >&2
  exit 1
fi

git checkout --quiet --detach "${COMMIT}"
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
