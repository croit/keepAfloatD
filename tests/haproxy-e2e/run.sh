#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly ROOT_DIR
readonly COMPOSE_FILE="${ROOT_DIR}/tests/haproxy-e2e/docker-compose.yml"
readonly COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-keepafloatd-haproxy-e2e}"
readonly ARTIFACT_DIR="${ARTIFACT_DIR:-${ROOT_DIR}/e2e-artifacts/haproxy-e2e}"
readonly -a NODES=("node-a" "node-b" "node-c")
readonly -a VIPS=("10.50.0.100" "10.50.0.101" "10.50.0.102")
# No fixed expected-assignment arrays: the minimal-movement assignment guarantees an even spread
# but not a fixed placement (holder identity is leader- and history-dependent), so we assert the
# even-spread shape via even_over_nodes(), not specific holders.

log() {
    printf '[haproxy-e2e] %s\n' "$*"
}

fail() {
    printf '[haproxy-e2e] ERROR: %s\n' "$*" >&2
    return 1
}

compose() {
    docker compose -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT_NAME}" "$@"
}

node_sh() {
    local service="${1:?service required}"
    shift
    compose exec -T "${service}" sh -lc "$*"
}

runner_http_body() {
    local vip="${1:?vip required}"
    compose exec -T e2e-runner python -c \
        "import urllib.request; print(urllib.request.urlopen('http://${vip}/', timeout=3).read().decode(), end='')"
}

wait_until() {
    local timeout_secs="${1:?timeout required}"
    shift
    local deadline=$((SECONDS + timeout_secs))
    until "$@"; do
        if (( SECONDS >= deadline )); then
            return 1
        fi
        sleep 0.5
    done
}

node_has_vip_bound() {
    local service="${1:?service required}"
    local vip="${2:?vip required}"
    node_sh "${service}" "ip -o -4 addr show dev eth0 | grep -F -q ' ${vip}/32 '"
}

holder_for_vip() {
    local vip="${1:?vip required}"
    local holder="none"
    local service

    for service in "${NODES[@]}"; do
        if node_has_vip_bound "${service}" "${vip}"; then
            if [[ "${holder}" != "none" ]]; then
                printf 'duplicate:%s,%s\n' "${holder}" "${service}"
                return 0
            fi
            holder="${service}"
        fi
    done

    printf '%s\n' "${holder}"
}

current_assignments_summary() {
    local vip
    local parts=()
    for vip in "${VIPS[@]}"; do
        parts+=("${vip}=$(holder_for_vip "${vip}")")
    done
    printf '%s\n' "${parts[*]}"
}

assert_unique_holders() {
    local vip
    local holder
    for vip in "${VIPS[@]}"; do
        holder="$(holder_for_vip "${vip}")"
        case "${holder}" in
            none)
                fail "no holder for VIP ${vip}"
                return 1
                ;;
            duplicate:*)
                fail "multiple holders for VIP ${vip}: ${holder#duplicate:}"
                return 1
                ;;
        esac
    done
}

# Predicate: every VIP is uniquely held, each holder is one of the given nodes, and the VIPs are
# spread as evenly as possible across exactly those nodes (each holds floor..ceil of |VIPS|/N).
# Asserts the shape the minimal-movement assignment guarantees, not a fixed placement.
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
        fail "timed out waiting for an even VIP spread over [$*] (current: $(current_assignments_summary))"
        return 1
    }
    assert_unique_holders
}

assert_all_vips_http_ok() {
    local vip
    local body
    for vip in "${VIPS[@]}"; do
        body="$(runner_http_body "${vip}")" || return 1
        grep -F -q 'keepafloatd haproxy article backend' <<<"${body}" || {
            fail "unexpected HTTP body for VIP ${vip}"
            return 1
        }
    done
}

wait_for_log_any() {
    local timeout_secs="${1:?timeout required}"
    local pattern="${2:?pattern required}"
    wait_until "${timeout_secs}" bash -lc \
        "docker compose -f '${COMPOSE_FILE}' -p '${COMPOSE_PROJECT_NAME}' logs --no-color ${NODES[*]} 2>&1 | grep -E -q '${pattern}'"
}

log_checkpoint() {
    compose logs --no-color "${NODES[@]}" 2>&1 | wc -l | tr -d ' '
}

wait_for_log_any_after() {
    local checkpoint="${1:?checkpoint required}"
    local timeout_secs="${2:?timeout required}"
    local pattern="${3:?pattern required}"
    wait_until "${timeout_secs}" bash -lc \
        "docker compose -f '${COMPOSE_FILE}' -p '${COMPOSE_PROJECT_NAME}' logs --no-color ${NODES[*]} 2>&1 | tail -n +$((checkpoint + 1)) | grep -E -q '${pattern}'"
}

stop_haproxy() {
    local service="${1:?service required}"
    node_sh "${service}" "pkill -x haproxy"
}

wait_for_haproxy_stop() {
    local service="${1:?service required}"
    wait_until 10 bash -lc \
        "docker compose -f '${COMPOSE_FILE}' -p '${COMPOSE_PROJECT_NAME}' exec -T '${service}' sh -lc '! pgrep -x haproxy >/dev/null'"
}

capture_cluster_artifacts() {
    local scenario="${1:?scenario required}"
    local dir="${ARTIFACT_DIR}/${scenario}"
    mkdir -p "${dir}"

    compose ps --all >"${dir}/compose-ps.txt" 2>&1 || true
    compose logs --no-color >"${dir}/compose.log" 2>&1 || true

    local service
    for service in backend-http e2e-runner "${NODES[@]}"; do
        compose logs --no-color "${service}" >"${dir}/${service}.log" 2>&1 || true
        if compose ps --status running --services | grep -F -x -q "${service}"; then
            compose exec -T "${service}" sh -lc "ip -o addr show || true" >"${dir}/${service}.ip-addr.txt" 2>&1 || true
        fi
    done
}

dump_cluster_diagnostics() {
    printf '[haproxy-e2e] --- compose ps --all ---\n' >&2
    compose ps --all >&2 || true
    printf '[haproxy-e2e] --- assignments ---\n' >&2
    current_assignments_summary >&2 || true
    printf '[haproxy-e2e] --- logs ---\n' >&2
    compose logs --no-color >&2 || true
}

cleanup() {
    local status=$?
    if (( status != 0 )); then
        capture_cluster_artifacts failed
        dump_cluster_diagnostics
    fi
    compose down -v --remove-orphans >/dev/null 2>&1 || true
    trap - EXIT
    exit "${status}"
}

trap cleanup EXIT

log "building release binary for HAProxy article lab"
cargo build --release

log "building local haproxy stub"
mkdir -p target/haproxy-e2e
rustc tests/haproxy-e2e/haproxy.rs -O -o target/haproxy-e2e/haproxy

log "preparing compact node build context"
rm -rf target/haproxy-e2e/docker-context
mkdir -p target/haproxy-e2e/docker-context/configs
cp target/haproxy-e2e/haproxy target/haproxy-e2e/docker-context/haproxy
cp tests/haproxy-e2e/entrypoint.sh target/haproxy-e2e/docker-context/entrypoint.sh
cp tests/haproxy-e2e/node.Dockerfile target/haproxy-e2e/docker-context/Dockerfile
cp tests/haproxy-e2e/configs/*.yaml target/haproxy-e2e/docker-context/configs/

log "building node image"
DOCKER_BUILDKIT=0 docker build --pull=false -t keepafloatd-haproxy-e2e-node:local target/haproxy-e2e/docker-context

log "starting HAProxy article lab"
compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d

log "waiting for steady-state VIP distribution"
wait_for_even_over_nodes 30 "${NODES[@]}"
wait_until 20 assert_all_vips_http_ok || {
    fail "VIP HTTP checks did not become healthy in steady state"
    exit 1
}
log "steady-state assignments: $(current_assignments_summary)"

checkpoint="$(log_checkpoint)"
log "stopping haproxy on node-a to trigger health-driven failover"
stop_haproxy node-a
wait_for_haproxy_stop node-a || {
    fail "haproxy did not stop on node-a"
    exit 1
}

# node-a's haproxy is down, so its health check fails and it sheds its VIPs onto the healthy pair.
wait_for_even_over_nodes 20 node-b node-c
wait_until 20 assert_all_vips_http_ok || {
    fail "VIP HTTP checks did not recover after haproxy stop"
    exit 1
}

capture_cluster_artifacts success
log "haproxy article lab passed: steady state and failover both validated"
