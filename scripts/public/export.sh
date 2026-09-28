#!/usr/bin/env sh
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "Usage: $0 DESTINATION [REVISION]" >&2
  exit 2
fi

DESTINATION=$1
REVISION=${2:-WORKTREE}
ROOT=$(git rev-parse --show-toplevel)
MANIFEST="${ROOT}/scripts/public/files.txt"

if [ -e "${DESTINATION}" ]; then
  echo "Destination already exists: ${DESTINATION}" >&2
  exit 1
fi

STAGING=$(mktemp -d)
trap 'rm -rf "${STAGING}"' EXIT

set --
while IFS='|' read -r source destination; do
  if [ -z "${source}" ] || [ -z "${destination}" ]; then
    echo "Invalid public manifest entry: ${source}|${destination}" >&2
    exit 1
  fi
  if [ "${REVISION}" = "WORKTREE" ]; then
    if [ ! -e "${ROOT}/${source}" ]; then
      echo "Public manifest source is missing: ${source}" >&2
      exit 1
    fi
  elif ! git -C "${ROOT}" cat-file -e "${REVISION}:${source}" 2>/dev/null; then
    echo "Public manifest source is not tracked at ${REVISION}: ${source}" >&2
    exit 1
  fi
  set -- "$@" "${source}"
done < "${MANIFEST}"

mkdir -p "${DESTINATION}"
if [ "${REVISION}" = "WORKTREE" ]; then
  while IFS='|' read -r source destination; do
    mkdir -p "${STAGING}/$(dirname "${source}")"
    cp -a "${ROOT}/${source}" "${STAGING}/${source}"
  done < "${MANIFEST}"
else
  git -C "${ROOT}" archive "${REVISION}" -- "$@" | tar -x -C "${STAGING}"
fi

while IFS='|' read -r source destination; do
  target="${DESTINATION}/${destination}"
  mkdir -p "$(dirname "${target}")"
  cp -a "${STAGING}/${source}" "${target}"
done < "${MANIFEST}"
