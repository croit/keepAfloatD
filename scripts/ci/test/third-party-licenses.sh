#!/usr/bin/env sh
# Keep THIRD_PARTY_LICENSES.md in step with Cargo.lock.
#
# Check mode (CI):   third-party-licenses.sh [CARGO_LOCK] [INVENTORY]
# Write mode (dev):  third-party-licenses.sh --write
#   Regenerates the expression and package sections from
#   `cargo metadata --format-version 1 --locked` (needs jq) and refreshes the counts.
set -eu

HERE="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
ROOT="$(CDPATH= cd -- "${HERE}/../../.." && pwd)"
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT

lock_pairs() {
  awk '
    /^\[\[package\]\]/ { name = ""; version = "" }
    /^name = "/ { gsub(/^name = "|"$/, ""); name = $0 }
    /^version = "/ {
      gsub(/^version = "|"$/, "")
      version = $0
      if (name != "" && name != "keepafloatd") print name " " version
      name = ""
    }
  ' "$1" | LC_ALL=C sort -u
}

inventory_pairs() {
  awk '/^- `[^`]+` `[^`]+`$/ { gsub(/`/, ""); print $2 " " $3 }' "$1" | LC_ALL=C sort -u
}

if [ "${1:-}" = "--write" ]; then
  INVENTORY="${ROOT}/THIRD_PARTY_LICENSES.md"
  META="${TEMP_DIR}/meta.txt"
  (cd "${ROOT}" && cargo metadata --format-version 1 --locked) \
    | jq -r '.packages[] | select(.name != "keepafloatd")
             | "\(.license // "NONE")\t\(.name)\t\(.version)"' \
    | LC_ALL=C sort > "${META}"
  CRATES=$(wc -l < "${META}" | tr -d ' ')
  EXPRESSIONS=$(cut -f 1 "${META}" | LC_ALL=C sort -u | wc -l | tr -d ' ')
  HEAD_END=$(grep -n '^## Observed License Expressions$' "${INVENTORY}" | cut -d : -f 1)
  {
    sed -n "1,$((HEAD_END - 1))p" "${INVENTORY}" \
      | sed -e "s/^- Non-root crates observed: \`[0-9]*\`$/- Non-root crates observed: \`${CRATES}\`/" \
            -e "s/^- Distinct third-party license expressions observed: \`[0-9]*\`$/- Distinct third-party license expressions observed: \`${EXPRESSIONS}\`/"
    echo '## Observed License Expressions'
    echo
    cut -f 1 "${META}" | LC_ALL=C sort -u | sed 's/^/- `/; s/$/`/'
    echo
    echo '## Packages by Expression'
    awk -F '\t' '
      $1 != current { print ""; print "### `" $1 "`"; print ""; current = $1 }
      { print "- `" $2 "` `" $3 "`" }
    ' "${META}"
  } > "${TEMP_DIR}/inventory.md"
  mv "${TEMP_DIR}/inventory.md" "${INVENTORY}"
  echo "Regenerated ${INVENTORY} (${CRATES} crates, ${EXPRESSIONS} expressions)"
  exit 0
fi

LOCK=${1:-"${ROOT}/Cargo.lock"}
INVENTORY=${2:-"${ROOT}/THIRD_PARTY_LICENSES.md"}
STATUS=0

lock_pairs "${LOCK}" > "${TEMP_DIR}/lock.txt"
inventory_pairs "${INVENTORY}" > "${TEMP_DIR}/inventory.txt"

comm -23 "${TEMP_DIR}/lock.txt" "${TEMP_DIR}/inventory.txt" > "${TEMP_DIR}/missing.txt"
comm -13 "${TEMP_DIR}/lock.txt" "${TEMP_DIR}/inventory.txt" > "${TEMP_DIR}/stale.txt"
if [ -s "${TEMP_DIR}/missing.txt" ] || [ -s "${TEMP_DIR}/stale.txt" ]; then
  echo "THIRD_PARTY_LICENSES.md does not match Cargo.lock" >&2
  while IFS= read -r pair; do echo "  inventory lists ${pair} but the lockfile does not" >&2; done < "${TEMP_DIR}/stale.txt"
  while IFS= read -r pair; do echo "  lockfile resolves ${pair} but the inventory does not list it" >&2; done < "${TEMP_DIR}/missing.txt"
  STATUS=1
fi

LOCK_COUNT=$(wc -l < "${TEMP_DIR}/lock.txt" | tr -d ' ')
DECLARED_CRATES=$(sed -n 's/^- Non-root crates observed: `\([0-9]*\)`$/\1/p' "${INVENTORY}")
if [ "${DECLARED_CRATES}" != "${LOCK_COUNT}" ]; then
  echo "THIRD_PARTY_LICENSES.md declares crate count ${DECLARED_CRATES:-missing} but the lockfile has ${LOCK_COUNT}" >&2
  STATUS=1
fi

SECTION_COUNT=$(grep -c '^### ' "${INVENTORY}" || true)
DECLARED_EXPRESSIONS=$(sed -n 's/^- Distinct third-party license expressions observed: `\([0-9]*\)`$/\1/p' "${INVENTORY}")
if [ "${DECLARED_EXPRESSIONS}" != "${SECTION_COUNT}" ]; then
  echo "THIRD_PARTY_LICENSES.md declares expression count ${DECLARED_EXPRESSIONS:-missing} but the inventory lists ${SECTION_COUNT}" >&2
  STATUS=1
fi

if [ "${STATUS}" -eq 0 ]; then
  echo "THIRD_PARTY_LICENSES.md matches Cargo.lock (${LOCK_COUNT} crates, ${SECTION_COUNT} expressions)"
fi
exit "${STATUS}"
