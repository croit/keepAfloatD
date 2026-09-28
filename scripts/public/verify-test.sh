#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT

FIXTURE_REPO="${TEMP_DIR}/repo"
git clone --quiet --no-hardlinks "${ROOT}" "${FIXTURE_REPO}"

rm -rf "${FIXTURE_REPO}/scripts/public"
mkdir -p "${FIXTURE_REPO}/scripts/public"
cp -a "${ROOT}/scripts/public/." "${FIXTURE_REPO}/scripts/public/"
git -C "${FIXTURE_REPO}" add scripts/public
git -C "${FIXTURE_REPO}" \
  -c user.name='Public export test' \
  -c user.email='public-export-test@example.invalid' \
  commit --quiet --allow-empty -m 'Update verifier under test'

printf '%s\n' 'pub fn omitted_fixture() {}' > "${FIXTURE_REPO}/src/omitted_fixture.rs"
git -C "${FIXTURE_REPO}" add src/omitted_fixture.rs
git -C "${FIXTURE_REPO}" \
  -c user.name='Public export test' \
  -c user.email='public-export-test@example.invalid' \
  commit --quiet -m 'Add omitted production source fixture'

mkdir -p "${FIXTURE_REPO}/deploy/fixture" "${FIXTURE_REPO}/docs"
printf '%s\n' '# omitted deploy fixture' > "${FIXTURE_REPO}/deploy/fixture/omitted"
printf '%s\n' '# omitted referenced fixture' > "${FIXTURE_REPO}/docs/fixture-omitted.md"
printf '\n%s\n%s\n' '[package.metadata.public-export-fixture]' \
  'reference = "docs/fixture-omitted.md"' >> "${FIXTURE_REPO}/Cargo.toml"
git -C "${FIXTURE_REPO}" add deploy/fixture/omitted docs/fixture-omitted.md Cargo.toml
git -C "${FIXTURE_REPO}" \
  -c user.name='Public export test' \
  -c user.email='public-export-test@example.invalid' \
  commit --quiet -m 'Add omitted deploy and referenced fixtures'

EXPECTED_SOURCE='Public manifest omits tracked production Rust source: src/omitted_fixture.rs'
EXPECTED_DEPLOY='Public manifest omits tracked deploy file: deploy/fixture/omitted'
EXPECTED_REFERENCED='Public manifest omits Cargo.toml-referenced file: docs/fixture-omitted.md'
for revision in HEAD WORKTREE; do
  if OUTPUT=$(cd "${FIXTURE_REPO}" && scripts/public/verify.sh "${revision}" 2>&1); then
    echo "Expected ${revision} verification to reject omitted tracked files" >&2
    exit 1
  fi

  for expected in "${EXPECTED_SOURCE}" "${EXPECTED_DEPLOY}" "${EXPECTED_REFERENCED}"; do
    if ! printf '%s\n' "${OUTPUT}" | grep -F "${expected}" >/dev/null; then
      printf '%s\n' "${OUTPUT}" >&2
      echo "${revision} verification failed without the diagnostic: ${expected}" >&2
      exit 1
    fi
  done
done

echo "Public source closure self-test passed"
