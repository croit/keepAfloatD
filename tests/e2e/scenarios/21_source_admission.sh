#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

peer_address="$(awk '/^raft_listen:/ {gsub(/"/, "", $2); print $2}' "${ROOT_DIR}/tests/e2e/configs/a.yaml")"
target_address="$(awk '/^raft_listen:/ {gsub(/"/, "", $2); print $2}' "${ROOT_DIR}/tests/e2e/configs/b.yaml")"
secret="$(awk '/^cluster_secret:/ {gsub(/"/, "", $2); print $2}' "${ROOT_DIR}/tests/e2e/configs/a.yaml")"
peer_id="$(awk '/^node_id:/ {print $2}' "${ROOT_DIR}/tests/e2e/configs/a.yaml")"
peer_ip="${peer_address%:*}"
peer_container="$(service_container_id node-a)"
network="$(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' "${peer_container}")"
[[ -n "${network}" && "${network}" != *$'\n'* ]] || fail 'expected exactly one test peer network'

kill_service node-a KILL
wait_for_service_exit node-a 10
wait_for_even_over_nodes "$(cleanup_budget_seconds 30)" node-b node-c

# Only the stopped test peer's address is borrowed; no Raft state is submitted.
docker network disconnect "${network}" "${peer_container}"
runner_sh "ip address add ${peer_ip}/32 dev eth0"
compose cp "${ROOT_DIR}/tests/e2e/scripts/auth_wire.py" e2e-runner:/tmp/auth_wire.py
compose cp "${ROOT_DIR}/tests/e2e/scripts/source-admission.py" e2e-runner:/tmp/source-admission.py
compose exec -T e2e-runner python3 /tmp/source-admission.py \
  "${peer_ip}" "${target_address}" "${peer_id}" "$(awk '/^node_id:/ {print $2}' "${ROOT_DIR}/tests/e2e/configs/b.yaml")" "${secret}"
runner_sh "ip address del ${peer_ip}/32 dev eth0"
wait_for_even_over_nodes 10 node-b node-c
assert_unique_holders
log 'separate source admission phases remained bounded without disrupting VIP ownership'
