#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Minimal-movement VIP distribution on a 5-node / 4-VIP cluster (driven by run5.sh).
#
# We assert the user's failover narrative *relative to the observed steady-state placement*, not a
# hardcoded one: the assignment is a stable, minimal-movement, load-balancing rebalance, so the
# absolute cold-start holder of each VIP depends on join/election timing and is not deterministic
# across runs. Reading the real placement and asserting the relative behavior is strictly stronger
# than hardcoding holders — it also proves that uninvolved VIPs do NOT move (the property that
# distinguishes minimal-movement from a global round-robin reshuffle).
#
# Narrative (A..E are physical nodes; victim1/victim2 are the two we kill; E0 is the idle node):
#   steady : four VIPs on four distinct nodes, one node (E0) idle.
#   kill victim1 : its single VIP fails over to the idle node E0; every other VIP stays put.
#   kill victim2 : its VIP fails over onto one of the surviving holders, which now holds two.
#   restart victim1 : the overloaded node sheds its HIGHER-IP VIP to the now-empty victim1; the
#                     cluster returns to one VIP per node.
# Quorum stays >= 3 of 5 throughout (we only ever have two nodes down at once).

IPA="${VIPS[0]}"
IPB="${VIPS[1]}"
IPC="${VIPS[2]}"
IPD="${VIPS[3]}"

# expect_holders vip1 holder1 vip2 holder2 ... — true iff every VIP currently maps to its holder.
expect_holders() {
  while (($#)); do
    local vip="$1" want="$2"
    shift 2
    [[ "$(holder_for_vip "${vip}")" == "${want}" ]] || return 1
  done
}

higher_ip() {
  printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n 1
}

snapshot_holders() {
  local out="" vip
  for vip in "${VIPS[@]}"; do out+="${vip}=$(holder_for_vip "${vip}") "; done
  printf '%s' "${out}"
}

# Wait until the VIP->holder map is even AND unchanged across a sustained interval, so the cluster
# has fully quiesced (committed == bound, no in-flight handoffs) before we perturb it. Capturing a
# transiently-even bound state mid-handoff would let the strict "only the victim's VIP moves"
# assertions race the cluster's own formation/rebalance.
wait_for_settled() {
  local timeout_secs="${1:?timeout required}"
  local deadline=$((SECONDS + timeout_secs))
  local prev="" cur
  while ((SECONDS < deadline)); do
    if even_over_nodes "${NODES[@]}"; then
      cur="$(snapshot_holders)"
      [[ -n "${prev}" && "${cur}" == "${prev}" ]] && return 0
      prev="${cur}"
    else
      prev=""
    fi
    sleep 3
  done
  dump_cluster_diagnostics
  fail "cluster did not settle into a stable even spread"
  return 1
}

# --- 0) Steady state: settle, then capture the actual placement and pick our victims. --------
wait_for_settled 40 || exit 1
declare -A H0
for vip in "${VIPS[@]}"; do
  H0["${vip}"]="$(holder_for_vip "${vip}")"
done

E0=""
for node in "${NODES[@]}"; do
  holds=false
  for vip in "${VIPS[@]}"; do
    [[ "${H0[${vip}]}" == "${node}" ]] && holds=true
  done
  ${holds} || E0="${node}"
done
[[ -n "${E0}" ]] || {
  dump_cluster_diagnostics
  fail "could not identify the idle node at steady state"
  exit 1
}

# Kill the holders of IPA and IPB. They are distinct nodes (even spread) and never the idle node.
victim1="${H0[${IPA}]}"
victim2="${H0[${IPB}]}"
log "steady: IPA=${H0[${IPA}]} IPB=${H0[${IPB}]} IPC=${H0[${IPC}]} IPD=${H0[${IPD}]} idle=${E0}"
log "victims: victim1=${victim1} (holds IPA) victim2=${victim2} (holds IPB)"

# --- 1) Kill victim1: only IPA relocates, and it goes to the idle node E0. --------------------
kill_service "${victim1}" KILL
wait_for_service_exit "${victim1}" 10
wait_until 45 expect_holders \
  "${IPA}" "${E0}" \
  "${IPB}" "${H0[${IPB}]}" \
  "${IPC}" "${H0[${IPC}]}" \
  "${IPD}" "${H0[${IPD}]}" || {
  dump_cluster_diagnostics
  fail "after killing ${victim1}: expected only IPA to move to ${E0}, others unchanged"
  exit 1
}
assert_unique_holders
log "victim1 down: IPA failed over to ${E0}; IPB/IPC/IPD unchanged (minimal movement)"

# --- 2) Kill victim2: IPB relocates onto a surviving holder, which now holds two. --------------
kill_service "${victim2}" KILL
wait_for_service_exit "${victim2}" 10
post_c_down() {
  expect_holders \
    "${IPA}" "${E0}" \
    "${IPC}" "${H0[${IPC}]}" \
    "${IPD}" "${H0[${IPD}]}" || return 1
  local hb
  hb="$(holder_for_vip "${IPB}")"
  case "${hb}" in
    none | duplicate:*) return 1 ;;
  esac
  # IPB must land on a still-running holder (making it hold two), never on a downed victim.
  [[ "${hb}" != "${victim1}" && "${hb}" != "${victim2}" ]]
}
wait_until 45 post_c_down || {
  dump_cluster_diagnostics
  fail "after killing ${victim2}: expected IPB to fail over onto a surviving holder; IPA/IPC/IPD unchanged"
  exit 1
}
assert_unique_holders

# Snapshot the post-failover map and identify the overloaded node and the two VIPs it holds.
declare -A H2
for vip in "${VIPS[@]}"; do
  H2["${vip}"]="$(holder_for_vip "${vip}")"
done
overloaded="$(holder_for_vip "${IPB}")"
over_vips=()
for vip in "${VIPS[@]}"; do
  [[ "${H2[${vip}]}" == "${overloaded}" ]] && over_vips+=("${vip}")
done
[[ "${#over_vips[@]}" -eq 2 ]] || {
  dump_cluster_diagnostics
  fail "expected ${overloaded} to hold exactly two VIPs, holds: ${over_vips[*]:-none}"
  exit 1
}
hi="$(higher_ip "${over_vips[0]}" "${over_vips[1]}")"
lo="${over_vips[0]}"
[[ "${lo}" == "${hi}" ]] && lo="${over_vips[1]}"
log "victim2 down: IPB landed on ${overloaded} (now holds ${over_vips[*]}); higher-IP VIP is ${hi}"

# --- 3) Restart victim1: it rejoins empty; the overloaded node sheds its HIGHER-IP VIP to it. --
# Use --no-deps: victim1's depends_on chain would otherwise transitively restart victim2, which we
# need to stay down.
restart_checkpoint="$(log_checkpoint)"
start_services_no_deps "${victim1}"
wait_for_service_running "${victim1}" 10
wait_for_log_any_after "${restart_checkpoint}" 40 'joining via replication instead of forming a new one'

# Expected: the higher-IP VIP moves from ${overloaded} to the rejoined ${victim1}; the lower-IP VIP
# stays on ${overloaded}; the other two VIPs keep their holders from H2.
expected=()
for vip in "${VIPS[@]}"; do
  if [[ "${vip}" == "${hi}" ]]; then
    expected+=("${vip}" "${victim1}")
  else
    expected+=("${vip}" "${H2[${vip}]}")
  fi
done
wait_until 50 expect_holders "${expected[@]}" || {
  dump_cluster_diagnostics
  fail "after restarting ${victim1}: expected ${hi} to rebalance onto ${victim1}, ${lo} to stay on ${overloaded}, others unchanged"
  exit 1
}
wait_until 5 all_vips_arpable || {
  fail "rebalanced VIPs are not ARP-reachable"
  exit 1
}
assert_unique_holders
wait_for_single_agreed_leader 20
assert_node_lacks_all_vips "${victim2}"
log "victim1 back: ${hi} rebalanced onto ${victim1}; ${lo} stayed on ${overloaded}; spread even again"
