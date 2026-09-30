#!/usr/bin/env bash
# Multi-hour disruption soak. C2 separately proves repeated snapshot/purge cycles; this test
# repeatedly exercises four recovery paths under accelerated health churn: a single-node restart,
# a clean full reform, SIGKILL recovery, and a brief partition. Every cycle must return to a fully
# active cluster with one agreed leader, unique reachable VIPs, and ZERO Defensive errors. A clean
# full reform must also return to an even spread; nopreempt recovery may safely remain uneven.
# Any failure aborts with evidence and the EXIT trap restores the exact pre-soak configuration.
#
# Usage: ./soak.sh [hours]   (default 3)
# Test harnesses may set SOAK_SECONDS and SOAK_CYCLE_SECONDS to exercise every disruption path in a
# bounded smoke run without pretending that the result is a multi-hour endurance run.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${HERE}/lib.sh"
mkdir -p "${SSH_CTL_DIR}" 2>/dev/null || true

HOURS="${1:-3}"
[[ "${HOURS}" =~ ^[0-9]+$ ]] || { printf 'hours must be a non-negative integer\n' >&2; exit 64; }
DURATION_SECONDS="${SOAK_SECONDS:-$(( HOURS * 3600 ))}"
CYCLE_SECONDS="${SOAK_CYCLE_SECONDS:-90}"
[[ "${DURATION_SECONDS}" =~ ^[1-9][0-9]*$ ]] \
  || { printf 'SOAK_SECONDS/duration must be a positive integer\n' >&2; exit 64; }
[[ "${CYCLE_SECONDS}" =~ ^[1-9][0-9]*$ ]] \
  || { printf 'SOAK_CYCLE_SECONDS must be a positive integer\n' >&2; exit 64; }
END=$(( $(date +%s) + DURATION_SECONDS ))
LOG="${HERE}/results/soak.log"; : > "${LOG}"
say() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "${LOG}"; }

defensive_total_since() {  # sum Defensive errors across all nodes since <journal timestamp>
  local since="${1:?}" ip t=0 d
  for ip in "${NODE_IPS[@]}"; do
    d="$(journal_event_count "${ip}" "${since}" \
      'Defensive|LogIndexNotFound|quit RaftCore')" || return 1
    t=$(( t + d ))
  done
  echo "${t}"
}
ceph_healthy() { [[ "$(ceph_health_status)" == "HEALTH_OK" ]]; }
soak_recovered() {
  case "${1:?}" in
    full-reform) wait_for_steady_state ;;
    *)           wait_for_available_cluster ;;
  esac
}

run_soak_disruption() {
  local kind="${1:?}" victim="${2:?}"
  case "${kind}" in
    single-restart)
      say "cycle ${cycle}: single-restart ${victim}"
      kafd_restart "${victim}"
      ;;
    full-reform)
      say "cycle ${cycle}: CLEAN FULL REFORM"
      clean_reform
      ;;
    sigkill)
      say "cycle ${cycle}: SIGKILL ${victim}"
      kafd_kill "${victim}" && sleep 3 && kafd_start "${victim}"
      ;;
    partition)
      say "cycle ${cycle}: partition ${victim} 20s"
      partition_node "${victim}" && sleep 20 && heal_node "${victim}"
      ;;
  esac
}

SOAK_TAG=soak
SOAK_CONFIG_BACKED_UP=0
cleanup_soak() {
  local cleanup_ok=0
  [[ "${SOAK_CONFIG_BACKED_UP}" -eq 1 ]] || return 0
  say "restoring exact pre-soak config and clean cluster baseline"
  heal_all || cleanup_ok=1
  clear_health_sentinels || cleanup_ok=1
  if [[ "${SOAK_CONFIG_BACKED_UP}" -eq 1 ]]; then
    restore_cluster_configs "${SOAK_TAG}" || cleanup_ok=1
  fi
  clean_reform || cleanup_ok=1
  wait_for_steady_state || cleanup_ok=1
  if [[ "${cleanup_ok}" -eq 0 ]]; then
    say "cluster restored to exact pre-soak config and steady state"
  else
    say "FATAL: soak cleanup did not restore a steady baseline"
  fi
  return "${cleanup_ok}"
}
on_exit() {
  local rc=$?
  trap - EXIT INT TERM
  cleanup_soak || { [[ "${rc}" -ne 0 ]] || rc=4; }
  if [[ "${rc}" -eq 0 ]]; then
    say "RESULT soak: PASS"
  else
    say "RESULT soak: FAIL (exit ${rc})"
  fi
  exit "${rc}"
}
trap on_exit EXIT
trap 'exit 130' INT TERM

say "SOAK START - ${DURATION_SECONDS}s, ${CYCLE_SECONDS}s between disruptions, accelerated deterministic probe"
SOAK_SINCE="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
backup_cluster_configs "${SOAK_TAG}" || { say "FATAL: could not back up cluster configs"; exit 1; }
SOAK_CONFIG_BACKED_UP=1
prepare_cluster_secret || { say "FATAL: could not establish a common cluster secret"; exit 1; }
clear_health_sentinels || { say "FATAL: could not clear health sentinels"; exit 1; }
configure_sentinel_health 100 100 5 || { say "FATAL: could not configure deterministic probe"; exit 1; }
clean_reform || { say "FATAL: cluster did not reform with soak config"; exit 1; }
wait_for_steady_state || { say "FATAL: cluster not steady at soak start"; exit 1; }

cycle=0; disruptions=0
declare -a KINDS=(single-restart full-reform sigkill partition)
while (( $(date +%s) < END )); do
  cycle=$(( cycle + 1 ))
  sleep "${CYCLE_SECONDS}"
  kind="${KINDS[$(( (cycle - 1) % ${#KINDS[@]} ))]}"
  victim="${NODE_IPS[$(( (cycle - 1) % 3 ))]}"
  run_soak_disruption "${kind}" "${victim}" \
    || { say "ABORT cycle ${cycle} (${kind}): disruption command failed"; dump_diag | tee -a "${LOG}"; exit 2; }
  disruptions=$(( disruptions + 1 ))

  # Reconverge + assertions.
  if ! soak_recovered "${kind}"; then
    say "ABORT cycle ${cycle} (${kind}): cluster did not return to its required safe state"
    dump_diag | tee -a "${LOG}"; exit 2
  fi
  if ! d="$(defensive_total_since "${SOAK_SINCE}")"; then
    say "ABORT cycle ${cycle} (${kind}): could not read Defensive evidence"
    dump_diag | tee -a "${LOG}"; exit 3
  fi
  if [[ "${d}" -ne 0 ]]; then
    say "ABORT cycle ${cycle} (${kind}): ${d} Defensive errors after disruption"
    dump_diag | tee -a "${LOG}"; exit 3
  fi
  # Ceph stays healthy (partitions/restarts must not harm the data plane).
  if ! wait_until 60 ceph_healthy; then
    ch="$(ceph_health_status)"
    say "ABORT cycle ${cycle} (${kind}): Ceph did not recover to HEALTH_OK (got ${ch})"
    dump_diag | tee -a "${LOG}"; exit 3
  fi
  say "cycle ${cycle} (${kind}) OK - 0 Defensive, unique holders; remaining $(( (END - $(date +%s)) / 60 ))m"
done

if (( disruptions < ${#KINDS[@]} )); then
  say "ABORT: only ${disruptions} disruptions ran; at least ${#KINDS[@]} are required to cover every kind"
  exit 5
fi
say "disruption phase PASS - ${cycle} cycles, ${disruptions} disruptions, ZERO Defensive throughout"
