#!/usr/bin/env sh
set -eu

ROOT=$(git rev-parse --show-toplevel)
REVISION=${1:-WORKTREE}
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT

TREE="${TEMP_DIR}/tree"
EXPECTED="${TEMP_DIR}/expected.txt"
ACTUAL="${TEMP_DIR}/actual.txt"

"${ROOT}/scripts/public/check-source-closure.sh" \
  "${REVISION}" "${ROOT}/scripts/public/files.txt"
"${ROOT}/scripts/public/export.sh" "${TREE}" "${REVISION}"

cut -d '|' -f 2 "${ROOT}/scripts/public/files.txt" | sort > "${EXPECTED}"
(
  cd "${TREE}"
  find . \( -type f -o -type l \) -print | sed 's#^./##' | sort > "${ACTUAL}"
)
diff -u "${EXPECTED}" "${ACTUAL}"

if find "${TREE}" -type l -print | grep -q .; then
  echo "Public tree contains a symbolic link" >&2
  exit 1
fi

FORBIDDEN_DATA='gitlab[.]|docker[.][^ /]*croit[.]io|[.]int[.]|172[.](1[6-9]|2[0-9]|3[01])[.]|/home/[[:alnum:]_.-]+|root'"@"'|[[:alnum:]_.+-]+'"@"'gmail[.]com'
if grep -RInE --exclude-dir=.git "${FORBIDDEN_DATA}" "${TREE}"; then
  echo "Public tree contains internal or personal data" >&2
  exit 1
fi

if grep -RInP --exclude=verify.sh --exclude-dir=.git '\p{Cyrillic}' "${TREE}"; then
  echo "Public tree contains Cyrillic text" >&2
  exit 1
fi

NON_ASCII_DASHES=$(printf '\342\200\224|\342\200\223')
if grep -RInE --exclude-dir=.git "${NON_ASCII_DASHES}" "${TREE}"; then
  echo "Public tree contains non-ASCII dash characters" >&2
  exit 1
fi

if grep -RInE --exclude-dir=.git \
  '(Fix|fix|Work item|work item|Issue|issue|pre)-? ?#[0-9]+|MR ![0-9]+|GL-[0-9]+' \
  "${TREE}/src" "${TREE}/tests" "${TREE}/Dockerfile" "${TREE}/scripts"; then
  echo "Public code contains an internal work item reference" >&2
  exit 1
fi

if grep -RIl --exclude=verify.sh --exclude-dir=.git \
  -E 'BEGIN (RSA |OPENSSH |EC |DSA )?PRIVATE KEY' "${TREE}" | grep -q .; then
  echo "Public tree contains private-key material" >&2
  exit 1
fi

if grep -RInE 'uses:[[:space:]]+[^[:space:]]+@[^[:space:]]+' "${TREE}/.github/workflows" \
  | grep -vE '@[0-9a-f]{40}([[:space:]]|$)'; then
  echo "Public workflow contains an unpinned action" >&2
  exit 1
fi

for forbidden in .gitlab-ci.yml .mailmap AGENTS.md CLAUDE.md; do
  if find "${TREE}" -name "${forbidden}" -print | grep -q .; then
    echo "Public tree contains forbidden file: ${forbidden}" >&2
    exit 1
  fi
done

echo "Public tree verification passed"
