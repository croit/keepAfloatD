#!/usr/bin/env bash
# Run every real-cluster scenario in order, tally results, write results/report.md.
# Usage: ./run-all.sh [scenario-glob]   (default: all scenarios/*.sh in sorted order)
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${HERE}/lib.sh"
source "${HERE}/run-all-guard.sh"
install_campaign_guard_traps

glob="${1:-*}"
mapfile -t scripts < <(ls "${HERE}"/scenarios/${glob}.sh 2>/dev/null | sort -V)
[[ ${#scripts[@]} -gt 0 ]] || { echo "no scenarios match '${glob}'"; exit 2; }

REPORT="${HERE}/results/report.md"
mkdir -p "${HERE}/results"
{
  echo "# keepafloatd real-cluster campaign - results"
  echo
  echo "Cluster: ${REALCLUSTER_NAME} (mgmt ${MGMT}; nodes ${NODE_IPS[*]})"
  echo "VIPs: ${VIPS[*]}   |   started $(date -u +%FT%TZ)"
  echo
  echo "| scenario | result |"
  echo "|---|---|"
} > "${REPORT}"

log "warming SSH + asserting steady state before campaign"
mgmt_sh hostname >/dev/null
prepare_cluster_secret || { log "could not establish required cluster_secret"; exit 3; }
if [[ "${REALCLUSTER_SECRET_CHANGED}" -eq 1 ]]; then
  log "repaired missing/mismatched required cluster_secret before campaign"
  clean_reform
fi
wait_for_steady_state || { log "cluster not in steady state; aborting"; exit 3; }

# Freeze the exact normalized per-node configs before any scenario mutates them. The final audit
# compares these hashes without repairing drift, so a steady-looking cluster cannot mask a leaked
# timing, notify, secret, or VIP change.
BASELINE_HASHES="${REALCLUSTER_BASELINE_HASHES:-${HERE}/results/baseline-config.sha256}"
: > "${BASELINE_HASHES}"
for ip in "${NODE_IPS[@]}"; do
  inst="$(instance_for_ip "${ip}")"
  hash="$(node_sh "${ip}" "sha256sum /etc/keepafloatd/config-${inst}.yaml | awk '{print \$1}'")" \
    || { log "could not hash baseline config on ${ip}"; exit 3; }
  [[ "${hash}" =~ ^[a-f0-9]{64}$ ]] || { log "invalid baseline config hash on ${ip}"; exit 3; }
  printf '%s %s\n' "${ip}" "${hash}" >> "${BASELINE_HASHES}"
done
log "recorded exact baseline config hashes in ${BASELINE_HASHES}"

pass=0; fail=0
scenario_timeout="${REALCLUSTER_SCENARIO_TIMEOUT:-1800}"

for s in "${scripts[@]}"; do
  name="$(basename "${s%.sh}")"
  log "──────── running ${name} ────────"
  capture_scenario_guard || { log "could not capture exact pre-scenario guard for ${name}"; exit 3; }
  scenario_ok=0
  scenario_rc=0
  timeout --signal=TERM --kill-after=30 "${scenario_timeout}" bash "${s}" &
  RUNALL_SCENARIO_PGID=$!
  wait "${RUNALL_SCENARIO_PGID}" || scenario_rc=$?
  RUNALL_SCENARIO_PGID=""
  if [[ "${scenario_rc}" -eq 0 ]]; then
    scenario_ok=1
  fi
  restore_ok=1
  if ! restore_scenario_guard; then
    log "exact post-scenario restore failed for ${name}"
    scenario_ok=0
    restore_ok=0
  fi
  # A killed child cannot report its process-local RGW_MUTATED flag. For scenarios that can stop
  # RGW, own the delayed controller tail in the parent before allowing another scenario to start.
  if [[ "${scenario_rc}" -ne 0 && "${restore_ok}" -eq 1 ]]; then
    case "${name}" in
      B1_rgw_failover_s3 | B3_notify_hook | D11_failover_delay_nopreempt)
        if ! settle_after_rgw_recovery || ! verify_restored_guard_hashes; then
          log "post-failure RGW/controller state did not return to the exact guard for ${name}"
          scenario_ok=0
          restore_ok=0
        fi
        ;;
    esac
  fi
  if [[ "${scenario_ok}" -eq 1 ]]; then
    echo "| ${name} | ✅ PASS |" >> "${REPORT}"; pass=$((pass+1))
  else
    echo "| ${name} | ❌ FAIL |" >> "${REPORT}"; fail=$((fail+1))
  fi
  if [[ "${restore_ok}" -eq 0 ]]; then
    log "aborting campaign after failed exact restore; later scenarios would lack a verified baseline"
    exit 3
  fi
done

{
  echo
  echo "**Totals: ${pass} passed, ${fail} failed** - finished $(date -u +%FT%TZ)"
  echo
  echo "Per-scenario evidence: see results/<scenario>.log"
} >> "${REPORT}"

log "campaign done: ${pass} passed, ${fail} failed → ${REPORT}"
[[ ${fail} -eq 0 ]]
