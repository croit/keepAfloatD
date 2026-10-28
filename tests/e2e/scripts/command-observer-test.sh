#!/usr/bin/env bash
# Stop-path checks for command-observer.sh against a fake docker CLI.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT

mkdir "${scratch}/bin"
cat >"${scratch}/bin/docker" <<'SHIM'
#!/usr/bin/env bash
# One observer container whose state lives in $FAKE_DOCKER: it exits $FAKE_STOP_DELAY seconds
# after SIGTERM (never, when empty), and at once on SIGKILL.
state="${FAKE_DOCKER}"
now=$(date +%s)
alive=true exit_code=0
if [[ -f "${state}/killed" ]]; then
  alive=false exit_code=137
elif [[ -f "${state}/term" && -n "${FAKE_STOP_DELAY}" ]] &&
  ((now >= $(<"${state}/term") + FAKE_STOP_DELAY)); then
  alive=false
fi
case "$1" in
  ps) [[ -f "${state}/removed" ]] || echo "${FAKE_ID}" ;;
  inspect)
    [[ ! -f "${state}/removed" ]] || exit 1
    echo "${FAKE_ID}|${FAKE_OWNER}|${FAKE_TARGET}|${FAKE_STARTED}|${FAKE_IMAGE}|container:${FAKE_TARGET}|${alive}|${exit_code}|$([[ ${alive} == true ]] && echo running || echo exited)"
    ;;
  kill)
    case "$3" in
      TERM) [[ -f "${state}/term" ]] || echo "${now}" >"${state}/term" ;;
      KILL) touch "${state}/killed" ;;
    esac
    ;;
  rm) touch "${state}/removed" ;;
  *) exit 2 ;;
esac
SHIM
chmod +x "${scratch}/bin/docker"
export PATH="${scratch}/bin:${PATH}"

# shellcheck source=tests/e2e/scripts/command-observer.sh
. "${HERE}/command-observer.sh"

export FAKE_ID="$(printf 'a%.0s' {1..64})" FAKE_TARGET="$(printf 'b%.0s' {1..64})"
export FAKE_OWNER=owner FAKE_STARTED=2000-01-01T00:00:00Z
export FAKE_IMAGE="sha256:$(printf 'c%.0s' {1..64})"
COMMAND_OBSERVER_OWNER=${FAKE_OWNER} COMMAND_OBSERVER_IMAGE=${FAKE_IMAGE}

# Stops one observer that exits $1 seconds after SIGTERM (never, when empty) and prints the
# exit status the stop path recorded.
stop_observer() {
  export FAKE_STOP_DELAY=$1 FAKE_DOCKER="${scratch}/state.$RANDOM"
  mkdir "${FAKE_DOCKER}"
  local observer_events="${FAKE_DOCKER}/events.jsonl" observer_name=observer
  local observer_target=${FAKE_TARGET} observer_boot_started=${FAKE_STARTED}
  local observer_id= observer_alive=true observer_exit= observer_state=
  _command_observer_owned
  _command_observer_remove || true
  grep -o '"event":"observer_exit","status":[0-9]*' "${observer_events}" | grep -o '[0-9]*$'
}

[[ "$(stop_observer 4)" == 0 ]] || {
  printf 'FAIL an observer that exits 4 s after SIGTERM was killed\n' >&2
  exit 1
}
printf 'PASS a slow observer stop is not escalated to SIGKILL\n'

[[ "$(stop_observer '')" == 137 ]] || {
  printf 'FAIL an observer that ignores SIGTERM was not killed\n' >&2
  exit 1
}
printf 'PASS a hung observer is still killed\n'
