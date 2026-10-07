#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR
# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

peer_address="$(awk '/^raft_listen:/ {gsub(/"/, "", $2); print $2}' "${ROOT_DIR}/tests/e2e/configs/a.yaml")"
secret="$(awk '/^cluster_secret:/ {gsub(/"/, "", $2); print $2}' "${ROOT_DIR}/tests/e2e/configs/a.yaml")"
peer_ip="${peer_address%:*}"
peer_container="$(service_container_id node-a)"
network="$(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' "${peer_container}")"
[[ -n "${network}" && "${network}" != *$'\n'* ]] || fail 'expected the test peer to have exactly one network'

for node in "${NODES[@]}"; do
  kill_service "${node}" KILL
  wait_for_service_exit "${node}" 10
done

# Replace only this test container's endpoint with a discovery-only peer.
docker network disconnect "${network}" "${peer_container}"
runner_sh "ip address add ${peer_ip}/32 dev eth0"
compose cp "${ROOT_DIR}/tests/e2e/scripts/auth_wire.py" e2e-runner:/tmp/auth_wire.py
compose cp "${ROOT_DIR}/tests/e2e/scripts/formation-peer.py" e2e-runner:/tmp/formation-peer.py
compose exec -T -d e2e-runner python3 /tmp/formation-peer.py "${peer_address}" "${secret}"
wait_until 5 runner_sh 'test -f /shared/formation-ready'

# Fresh containers exclude startup logs from the preceding healthy cluster.
compose rm -f node-b node-c
start_services_no_deps node-b node-c
for node in node-b node-c; do
  node_id=2
  [[ "${node}" == node-b ]] || node_id=3
  startup_budget="$(startup_budget_seconds 15 "${node}")"
  wait_until "${startup_budget}" runner_sh "test -f /shared/formation-discovered-${node_id}"
  assert_node_lacks_all_vips "${node}"
done

# Neither blank daemon is restarted after seeing the existing-cluster reply.
runner_sh 'touch /shared/formation-stop'
wait_until 5 runner_sh 'test -f /shared/formation-stopped'
runner_sh "ip address del ${peer_ip}/32 dev eth0"
wait_for_startup_over_nodes 30 node-b node-c
assert_unique_holders
service_is_not_running node-a || fail 'the discovery peer must remain absent'
log 'blank majority recovered after Join without a daemon restart'
