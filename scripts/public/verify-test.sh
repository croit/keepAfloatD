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

# Exercise the topology gate without running or sourcing the fixture's harness.
cp "${ROOT}/scripts/ci/test/realcluster-harness.sh" \
  "${FIXTURE_REPO}/scripts/ci/test/realcluster-harness.sh"
cp "${ROOT}/tests/realcluster/safety-self-test.sh" \
  "${FIXTURE_REPO}/tests/realcluster/safety-self-test.sh"
cp "${FIXTURE_REPO}/tests/realcluster/self-test.sh" "${TEMP_DIR}/self-test.sh"
printf '%s\n' '#!/usr/bin/env bash' 'exit 0' \
  > "${FIXTURE_REPO}/tests/realcluster/self-test.sh"
bash "${FIXTURE_REPO}/scripts/ci/test/realcluster-harness.sh"
for parts in '10|20.30.40' '172|20.30.40' '192.168|30.40'; do
  address="${parts%|*}.${parts#*|}"
  printf '\nMGMT="%s"\n' "${address}" \
    >> "${FIXTURE_REPO}/tests/realcluster/env.example.sh"
  if OUTPUT=$(bash "${FIXTURE_REPO}/scripts/ci/test/realcluster-harness.sh" 2>&1); then
    echo "Expected the topology gate to reject a private address in env.example.sh" >&2
    exit 1
  fi
  printf '%s\n' "${OUTPUT}" | grep -F 'contain a private topology address' >/dev/null
  git -C "${FIXTURE_REPO}" show HEAD:tests/realcluster/env.example.sh \
    > "${FIXTURE_REPO}/tests/realcluster/env.example.sh"
done
cp "${TEMP_DIR}/self-test.sh" "${FIXTURE_REPO}/tests/realcluster/self-test.sh"

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

mkdir -p "${FIXTURE_REPO}/tests/realcluster/scenarios"
printf '%s\n' '#!/usr/bin/env bash' 'source "../fixture-helper.sh"' \
  > "${FIXTURE_REPO}/tests/realcluster/scenarios/fixture-scenario.sh"
printf '%s\n' '#!/usr/bin/env bash' 'true' \
  > "${FIXTURE_REPO}/tests/realcluster/fixture-helper.sh"
git -C "${FIXTURE_REPO}" add tests/realcluster
git -C "${FIXTURE_REPO}" \
  -c user.name='Public export test' \
  -c user.email='public-export-test@example.invalid' \
  commit --quiet -m 'Add omitted test scenario and helper fixtures'

EXPECTED_SOURCE='Public manifest omits tracked production Rust source: src/omitted_fixture.rs'
EXPECTED_DEPLOY='Public manifest omits tracked deploy file: deploy/fixture/omitted'
EXPECTED_REFERENCED='Public manifest omits Cargo.toml-referenced file: docs/fixture-omitted.md'
EXPECTED_SCENARIO='Public manifest omits tracked test file: tests/realcluster/scenarios/fixture-scenario.sh'
EXPECTED_HELPER='Public manifest omits tracked test file: tests/realcluster/fixture-helper.sh'
for revision in HEAD WORKTREE; do
  if OUTPUT=$(cd "${FIXTURE_REPO}" && scripts/public/verify.sh "${revision}" 2>&1); then
    echo "Expected ${revision} verification to reject omitted tracked files" >&2
    exit 1
  fi

  for expected in "${EXPECTED_SOURCE}" "${EXPECTED_DEPLOY}" "${EXPECTED_REFERENCED}" \
    "${EXPECTED_SCENARIO}" "${EXPECTED_HELPER}"; do
    if ! printf '%s\n' "${OUTPUT}" | grep -F "${expected}" >/dev/null; then
      printf '%s\n' "${OUTPUT}" >&2
      echo "${revision} verification failed without the diagnostic: ${expected}" >&2
      exit 1
    fi
  done
done

git -C "${FIXTURE_REPO}" ls-files src deploy tests docs/fixture-omitted.md \
  | while IFS= read -r path; do
      printf '%s|%s\n' "${path}" "${path}"
    done >> "${FIXTURE_REPO}/scripts/public/files.txt"
for revision in HEAD WORKTREE; do
  (cd "${FIXTURE_REPO}" && scripts/public/check-source-closure.sh "${revision}")
done

echo "Public source closure self-test passed"
