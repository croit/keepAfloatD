#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT_DIR}/tests/e2e/docker-compose.yml}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-keepafloatd-e2e}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${ROOT_DIR}/e2e-artifacts/compose}"
export COMPOSE_PROJECT_NAME

# Cluster topology. Overridable via space-separated env vars so a larger suite (e.g. the
# 5-node/4-VIP minimal-movement scenario driven by run5.sh) can reuse this library unchanged.
# Defaults are the 3-node/3-VIP layout used by scenarios 01–11; leaving the env vars unset keeps
# that behavior identical.
#
# Note: there are deliberately no fixed expected-assignment arrays. The minimal-movement assignment
# guarantees an even spread but not a fixed placement (holder identity is leader- and
# history-dependent), so scenarios assert the even-spread shape via even_over_nodes(), not specific
# holders.
if [[ -n "${E2E_VIPS:-}" ]]; then read -r -a VIPS <<<"${E2E_VIPS}"; else VIPS=("10.50.0.100" "10.50.0.101" "10.50.0.102"); fi
if [[ -n "${E2E_NODES:-}" ]]; then read -r -a NODES <<<"${E2E_NODES}"; else NODES=("node-a" "node-b" "node-c"); fi
readonly -a VIPS NODES

log() {
  printf '[e2e] %s\n' "$*"
}

fail() {
  printf '[e2e] ERROR: %s\n' "$*" >&2
  return 1
}

compose() {
  docker compose -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT_NAME}" "$@"
}

service_container_id() {
  compose ps -q --all "${1:?service required}"
}

service_is_running() {
  local service="${1:?service required}"
  local cid
  cid="$(service_container_id "${service}")"
  [[ -n "${cid}" ]] || return 1
  [[ "$(docker inspect -f '{{.State.Running}}' "${cid}")" == "true" ]]
}

service_exit_code() {
  local service="${1:?service required}"
  local cid
  cid="$(service_container_id "${service}")"
  [[ -n "${cid}" ]] || return 1
  docker inspect -f '{{.State.ExitCode}}' "${cid}"
}

service_is_not_running() {
  ! service_is_running "${1:?service required}"
}

service_status_summary() {
  local parts=()
  local service

  for service in e2e-fixtures "${NODES[@]}" e2e-runner; do
    local cid
    cid="$(service_container_id "${service}")"
    if [[ -z "${cid}" ]]; then
      parts+=("${service}=missing")
      continue
    fi
    parts+=("${service}=$(docker inspect -f '{{.State.Status}}/{{.State.ExitCode}}' "${cid}")")
  done

  printf '%s\n' "${parts[*]}"
}

node_sh() {
  local service="${1:?service required}"
  shift
  compose exec -T "${service}" sh -lc "$*"
}

runner_sh() {
  compose exec -T e2e-runner bash -lc "$*"
}

wait_until() {
  local timeout_secs="${1:?timeout required}"
  shift
  local deadline=$((SECONDS + timeout_secs))
  until "$@"; do
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 0.2
  done
}

capture_cluster_artifacts() {
  local scenario="${1:?scenario required}"
  local dir="${ARTIFACT_DIR}/${scenario}"
  mkdir -p "${dir}"

  compose ps --all >"${dir}/compose-ps.txt" 2>&1 || true
  compose logs --no-color >"${dir}/compose.log" 2>&1 || true

  local service
  for service in e2e-fixtures "${NODES[@]}" e2e-runner; do
    compose logs --no-color "${service}" >"${dir}/${service}.log" 2>&1 || true
    if service_is_running "${service}"; then
      if [[ "${service}" == "e2e-runner" ]]; then
        runner_sh "ip -o addr show" >"${dir}/${service}.ip-addr.txt" 2>&1 || true
      elif [[ "${service}" != "e2e-fixtures" ]]; then
        node_sh "${service}" "ip -o addr show" >"${dir}/${service}.ip-addr.txt" 2>&1 || true
        node_sh "${service}" "ip route show table main" >"${dir}/${service}.routes.txt" 2>&1 || true
      fi
    fi
  done
}

dump_cluster_diagnostics() {
  printf '[e2e] --- compose ps --all ---\n' >&2
  compose ps --all >&2 || true
  printf '[e2e] --- assignment summary ---\n' >&2
  current_assignments_summary >&2 || true
  printf '[e2e] --- service summary ---\n' >&2
  service_status_summary >&2 || true

  local service
  for service in e2e-fixtures "${NODES[@]}" e2e-runner; do
    printf '[e2e] --- logs: %s ---\n' "${service}" >&2
    compose logs --no-color --tail 200 "${service}" >&2 || true
  done

  for service in "${NODES[@]}"; do
    if service_is_running "${service}"; then
      printf '[e2e] --- inspect: %s ---\n' "${service}" >&2
      node_sh "${service}" "ls -l /opt/keepafloatd-tests /opt/keepafloatd-tests/configs /shared && echo '--- config ---' && cat /opt/keepafloatd-tests/configs/${service#node-}.yaml && echo '--- ip addr ---' && ip -o addr show && echo '--- routes ---' && ip route show table main && echo '--- health check ---' && /bin/sh /opt/keepafloatd-tests/health.sh /shared/${service}.unhealthy; echo status:$?" >&2 || true
    fi
  done
}

clear_health_toggles() {
  runner_sh 'rm -f /shared/*.unhealthy'
}

node_has_vip_bound() {
  local service="${1:?service required}"
  local vip="${2:?vip required}"

  service_is_running "${service}" || return 1
  node_sh "${service}" "ip -o -4 addr show dev eth0 | grep -F -q ' ${vip}/32 '"
}

holder_for_vip() {
  local vip="${1:?vip required}"
  local -a holders=()
  local service

  for service in "${NODES[@]}"; do
    if node_has_vip_bound "${service}" "${vip}"; then
      holders+=("${service}")
    fi
  done

  case "${#holders[@]}" in
    0) printf 'none\n' ;;
    1) printf '%s\n' "${holders[0]}" ;;
    *) printf 'duplicate:%s\n' "$(IFS=,; echo "${holders[*]}")" ;;
  esac
}

current_assignments_summary() {
  local parts=()
  local vip

  for vip in "${VIPS[@]}"; do
    parts+=("${vip}=$(holder_for_vip "${vip}")")
  done

  printf '%s\n' "${parts[*]}"
}

# Quiet predicate (for wait_until): ${1} holds none of the VIPs.
node_lacks_all_vips() {
  local service="${1:?service required}"
  local vip
  for vip in "${VIPS[@]}"; do
    node_has_vip_bound "${service}" "${vip}" && return 1
  done
  return 0
}

# Quiet predicate (for wait_until): every VIP has exactly one holder among the running nodes.
all_vips_uniquely_held() {
  local vip holder
  for vip in "${VIPS[@]}"; do
    holder="$(holder_for_vip "${vip}")"
    case "${holder}" in
      none | duplicate:*) return 1 ;;
    esac
  done
}

assert_unique_holders() {
  local vip
  for vip in "${VIPS[@]}"; do
    local holder
    holder="$(holder_for_vip "${vip}")"
    case "${holder}" in
      none)
        fail "no holder has VIP ${vip}"
        return 1
        ;;
      duplicate:*)
        fail "multiple holders detected for VIP ${vip}: ${holder#duplicate:}"
        return 1
        ;;
    esac
  done
}

assignments_match() {
  local -a expected=("$@")
  local idx=0
  local vip

  for vip in "${VIPS[@]}"; do
    local holder
    holder="$(holder_for_vip "${vip}")"
    [[ "${holder}" == "${expected[idx]}" ]] || return 1
    ((idx += 1))
  done
}

all_vips_arpable() {
  local vip
  for vip in "${VIPS[@]}"; do
    runner_sh "arping -q -c 1 -w 1 -I eth0 ${vip}"
  done
}

wait_for_assignments() {
  local timeout_secs="${1:?timeout required}"
  shift
  local -a expected=("$@")

  wait_until "${timeout_secs}" assignments_match "${expected[@]}" || {
    dump_cluster_diagnostics
    fail "timed out waiting for assignments ${expected[*]} (current: $(current_assignments_summary); services: $(service_status_summary))"
    return 1
  }
  wait_until 5 all_vips_arpable || {
    fail "VIPs became assigned but are not ARP-reachable"
    return 1
  }
  assert_unique_holders
}

# Quiet predicate (for wait_until): every VIP is uniquely held, each holder is one of the given
# nodes, and the VIPs are spread as evenly as possible across exactly those nodes (each holds
# floor..ceil of |VIPS|/N). This is the shape the minimal-movement assignment guarantees; it does
# NOT pin which node holds which VIP (placement is leader- and history-dependent), so scenarios
# assert this rather than fixed holders.
even_over_nodes() {
  local -a nodes=("$@")
  local -A count=()
  local n vip holder
  for n in "${nodes[@]}"; do count["${n}"]=0; done
  for vip in "${VIPS[@]}"; do
    holder="$(holder_for_vip "${vip}")"
    case "${holder}" in
      none | duplicate:*) return 1 ;;
    esac
    [[ -n "${count[${holder}]+x}" ]] || return 1
    count["${holder}"]=$((count["${holder}"] + 1))
  done
  local total="${#VIPS[@]}" k="${#nodes[@]}"
  local floor=$((total / k)) ceil=$(((total + k - 1) / k))
  for n in "${nodes[@]}"; do
    ((count["${n}"] >= floor && count["${n}"] <= ceil)) || return 1
  done
  return 0
}

wait_for_even_over_nodes() {
  local timeout_secs="${1:?timeout required}"
  shift

  wait_until "${timeout_secs}" even_over_nodes "$@" || {
    dump_cluster_diagnostics
    fail "VIPs not evenly distributed over [$*] (current: $(current_assignments_summary); services: $(service_status_summary))"
    return 1
  }
  wait_until 5 all_vips_arpable || {
    fail "VIPs became assigned but are not ARP-reachable"
    return 1
  }
  assert_unique_holders
}

wait_for_steady_state() {
  # VIPs spread as evenly as possible across every node, plus one agreed leader. Both placement and
  # leadership are non-deterministic, so we assert the even-spread shape (not specific holders) and
  # a single agreed leader (not a specific id).
  wait_for_even_over_nodes 30 "${NODES[@]}"
  wait_for_single_agreed_leader 20
}

wait_for_service_exit() {
  local service="${1:?service required}"
  local timeout_secs="${2:?timeout required}"

  wait_until "${timeout_secs}" service_is_not_running "${service}" || {
    fail "timed out waiting for ${service} to exit"
    return 1
  }
}

assert_service_exit_code() {
  local service="${1:?service required}"
  local expected="${2:?exit code required}"
  local actual

  actual="$(service_exit_code "${service}")"
  [[ "${actual}" == "${expected}" ]] || {
    fail "unexpected exit code for ${service}: got ${actual}, want ${expected}"
    return 1
  }
}

set_node_unhealthy() {
  local service="${1:?service required}"
  runner_sh "touch /shared/${service}.unhealthy"
}

add_blackhole_route() {
  local service="${1:?service required}"
  local peer_ip="${2:?peer ip required}"
  node_sh "${service}" "ip route replace blackhole ${peer_ip}/32"
}

install_minority_partition() {
  add_blackhole_route node-a 10.50.0.11
  add_blackhole_route node-a 10.50.0.12
  add_blackhole_route node-b 10.50.0.10
  add_blackhole_route node-c 10.50.0.10
}

remove_blackhole_route() {
  local service="${1:?service required}"
  local peer_ip="${2:?peer ip required}"
  node_sh "${service}" "ip route del blackhole ${peer_ip}/32 2>/dev/null || true"
}

# Isolate node-a from {node-b, node-c} using blackholes placed ONLY on node-a. Because node-a is
# never restarted in the stale-survivor scenario, this isolation persists even while node-b and
# node-c are killed and restarted (their base routes are restored on restart, but node-a still
# drops their traffic). The reverse routes (on b/c) are deliberately omitted for that reason.
isolate_node_a() {
  add_blackhole_route node-a 10.50.0.11
  add_blackhole_route node-a 10.50.0.12
}

heal_node_a_partition() {
  remove_blackhole_route node-a 10.50.0.11
  remove_blackhole_route node-a 10.50.0.12
}

assert_node_lacks_all_vips() {
  local service="${1:?service required}"
  local vip
  for vip in "${VIPS[@]}"; do
    if node_has_vip_bound "${service}" "${vip}"; then
      fail "${service} still has VIP ${vip} bound"
      return 1
    fi
  done
}

wait_for_log_any() {
  local timeout_secs="${1:?timeout required}"
  local pattern="${2:?pattern required}"

  wait_until "${timeout_secs}" cluster_logs_contain "${pattern}" || {
    dump_cluster_diagnostics
    fail "timed out waiting for log pattern: ${pattern}"
    return 1
  }
}

log_checkpoint() {
  compose logs --no-color "${NODES[@]}" 2>&1 | wc -l | tr -d ' '
}

capture_compose_logs() {
  local output="${1:?output required}"
  shift
  compose logs --no-color "$@" >"${output}" 2>&1 || true
}

cluster_logs_contain() {
  local pattern="${1:?pattern required}"
  local log_file
  local status

  log_file="$(mktemp)"
  capture_compose_logs "${log_file}" "${NODES[@]}"
  if grep -E -q "${pattern}" "${log_file}"; then
    status=0
  else
    status=$?
  fi
  rm -f "${log_file}"
  return "${status}"
}

cluster_logs_contain_after() {
  local checkpoint="${1:?checkpoint required}"
  local pattern="${2:?pattern required}"
  local log_file
  local filtered_log_file
  local status

  log_file="$(mktemp)"
  filtered_log_file="$(mktemp)"
  capture_compose_logs "${log_file}" "${NODES[@]}"
  tail -n "+$((checkpoint + 1))" "${log_file}" >"${filtered_log_file}"
  if grep -E -q "${pattern}" "${filtered_log_file}"; then
    status=0
  else
    status=$?
  fi
  rm -f "${filtered_log_file}" "${log_file}"
  return "${status}"
}

# Number of node-log lines matching ${pattern}. Counting occurrences is stable under
# compose-log reordering, unlike a line-number checkpoint: callers snapshot the count before an
# event and assert no NEW match appeared after it (a before/after delta), instead of relying on
# 'tail -n +N', which can let early lines slip past when containers restart and the aggregated
# log is re-interleaved.
cluster_logs_count() {
  local pattern="${1:?pattern required}"
  local log_file
  local count

  log_file="$(mktemp)"
  capture_compose_logs "${log_file}" "${NODES[@]}"
  count="$(grep -E -c "${pattern}" "${log_file}" || true)"
  rm -f "${log_file}"
  printf '%s' "${count}"
}

service_logs_contain() {
  local service="${1:?service required}"
  local pattern="${2:?pattern required}"
  local log_file
  local status

  log_file="$(mktemp)"
  capture_compose_logs "${log_file}" "${service}"
  if grep -E -q "${pattern}" "${log_file}"; then
    status=0
  else
    status=$?
  fi
  rm -f "${log_file}"
  return "${status}"
}

wait_for_log_any_after() {
  local checkpoint="${1:?checkpoint required}"
  local timeout_secs="${2:?timeout required}"
  local pattern="${3:?pattern required}"

  wait_until "${timeout_secs}" cluster_logs_contain_after "${checkpoint}" "${pattern}" || {
    fail "timed out waiting for post-checkpoint log pattern: ${pattern}"
    return 1
  }
}

assert_log_contains() {
  local service="${1:?service required}"
  local pattern="${2:?pattern required}"

  service_logs_contain "${service}" "${pattern}" || {
    fail "log for ${service} does not contain pattern: ${pattern}"
    return 1
  }
}

kill_service() {
  local service="${1:?service required}"
  local signal="${2:?signal required}"
  compose kill -s "${signal}" "${service}" >/dev/null
}

signal_keepafloatd() {
  local service="${1:?service required}"
  local signal="${2:?signal required}"

  node_sh "${service}" '
    for proc_dir in /proc/[0-9]*; do
      [ -r "${proc_dir}/comm" ] || continue
      if [ "$(cat "${proc_dir}/comm")" = "keepafloatd" ]; then
        kill -s '"${signal}"' "${proc_dir##*/}"
        exit 0
      fi
    done
    exit 1
  '
}

start_service() {
  local service="${1:?service required}"
  compose start "${service}" >/dev/null
}

# Start specific services WITHOUT their compose dependencies, so a deliberately-down peer
# (referenced via depends_on) is not pulled back up.
start_services_no_deps() {
  compose up -d --no-deps "$@" >/dev/null
}

wait_for_service_running() {
  local service="${1:?service required}"
  local timeout_secs="${2:?timeout required}"

  wait_until "${timeout_secs}" service_is_running "${service}" || {
    fail "timed out waiting for ${service} to start"
    return 1
  }
}

# Most recently observed leader id for one node (from its 'raft current leader is now Some(N)'
# lines), ignoring transient 'None'. Empty if the node has never observed a leader.
node_last_leader() {
  local service="${1:?service required}"
  local log_file id
  log_file="$(mktemp)"
  capture_compose_logs "${log_file}" "${service}"
  id="$(grep -oE 'raft current leader is now Some\([0-9]+\)' "${log_file}" | tail -n 1 | grep -oE '[0-9]+' || true)"
  rm -f "${log_file}"
  printf '%s' "${id}"
}

# True when every running node reports the same, non-empty leader id (i.e. one cluster, one leader).
single_agreed_leader() {
  local first="" svc leader
  for svc in "${NODES[@]}"; do
    service_is_running "${svc}" || continue
    leader="$(node_last_leader "${svc}")"
    [[ -n "${leader}" ]] || return 1
    if [[ -z "${first}" ]]; then
      first="${leader}"
    elif [[ "${leader}" != "${first}" ]]; then
      return 1
    fi
  done
  [[ -n "${first}" ]]
}

# Leader id agreed by the running nodes (empty if none yet). Assumes the cluster has converged.
current_leader_id() {
  local svc
  for svc in "${NODES[@]}"; do
    if service_is_running "${svc}"; then
      node_last_leader "${svc}"
      return 0
    fi
  done
}

# Map a Raft node id to its compose service name (NODES is ordered by id: 1->node-a, 2->node-b, ...).
service_for_id() {
  local id="${1:?id required}"
  printf '%s' "${NODES[$((id - 1))]}"
}

wait_for_single_agreed_leader() {
  local timeout_secs="${1:?timeout required}"

  wait_until "${timeout_secs}" single_agreed_leader || {
    dump_cluster_diagnostics
    fail "nodes did not converge on a single agreed leader"
    return 1
  }
}

# True when the running nodes agree on a single leader whose id differs from ${1}. Used after
# killing the current leader: node_last_leader reports the stale leader until a NEW one is logged,
# so callers must wait for the id to actually change, not merely for agreement.
leader_agreed_other_than() {
  local excluded="${1:?excluded id required}"
  single_agreed_leader || return 1
  local id
  id="$(current_leader_id)"
  [[ -n "${id}" && "${id}" != "${excluded}" ]]
}

wait_for_leader_other_than() {
  local excluded="${1:?excluded id required}"
  local timeout_secs="${2:?timeout required}"

  wait_until "${timeout_secs}" leader_agreed_other_than "${excluded}" || {
    dump_cluster_diagnostics
    fail "no new agreed leader (other than ${excluded}) emerged"
    return 1
  }
}

reset_cluster() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  compose up -d --build
  clear_health_toggles
}
