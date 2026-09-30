#!/usr/bin/env bash
# D22: Validate both startup boundaries added after the final audit. An occupied submit socket must
# terminate the composition root, and an IPv4-mapped submit endpoint that aliases the IPv4 Raft
# endpoint must be rejected during config loading. Both failures must remain VIP-less and recover
# after exact repair.
SCENARIO_NAME=D22_startup_endpoint_failures
source "$(dirname "$0")/../scenario.sh"
scenario_start "submit bind failure and IPv4-mapped endpoint alias fail closed"

target="${NODE_IPS[0]}"
inst="$(instance_for_ip "${target}")"
cfg="/etc/keepafloatd/config-${inst}.yaml"
submit_port="$(node_sh "${target}" "sed -n 's/^client_submit_listen:.*:\([0-9][0-9]*\)\"/\1/p' ${cfg}")"
raft_port="$(node_sh "${target}" "sed -n 's/^raft_listen:.*:\([0-9][0-9]*\)\"/\1/p' ${cfg}")"
check "target config exposes one submit port" test -n "${submit_port}"
check "target config exposes one Raft port" test -n "${raft_port}"

evid "occupying ${target}:${submit_port} before starting keepafloatd"
kafd_stop "${target}"
check "survivors serve every VIP uniquely while target is stopped" \
  wait_for_live_service_without 45 "${target}"
node_sh "${target}" "
  nohup /usr/bin/nc --listen --keep-open ${target} ${submit_port} \
    >/run/kafd-submit-blocker.log 2>&1 </dev/null &
  echo \$! > /run/kafd-submit-blocker.pid
  sleep 1
  kill -0 \$(cat /run/kafd-submit-blocker.pid)
"
bind_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
kafd_start "${target}" || true

submit_bind_failed() {
  local logs
  logs="$(kafd_log_since "${target}" "${bind_since}")" || return 1
  grep -q 'Error: submit server' <<< "${logs}" &&
    grep -q 'Address in use' <<< "${logs}"
}
bind_failure_closed() {
  local blocker
  blocker="$(node_sh "${target}" "cat /run/kafd-submit-blocker.pid")" || return 1
  [[ "${blocker}" =~ ^[0-9]+$ ]] || return 1
  node_process_absent "${target}" keepafloatd &&
    node_tcp_listener_absent "${target}" "${raft_port}" &&
    node_tcp_listener_owned_only_by_pid "${target}" "${submit_port}" "${blocker}"
}
check "occupied submit listener terminates the composition root" wait_until 20 submit_bind_failed
check "failed submit startup leaves no daemon or Raft listener" \
  wait_until 20 holds_for 5 bind_failure_closed
check "impaired node stays VIP-less while systemd retries" holds_for 5 node_lacks_all_vips "${target}"

kafd_stop "${target}"
node_sh "${target}" "
  test ! -f /run/kafd-submit-blocker.pid || kill \$(cat /run/kafd-submit-blocker.pid) 2>/dev/null || true
  rm -f /run/kafd-submit-blocker.pid /run/kafd-submit-blocker.log
"
kafd_start "${target}"
check "node rejoins after the submit port is released" wait_for_available_cluster

tag=d22
backup_cluster_configs "${tag}"
kafd_stop "${target}"
mapped="[::ffff:${target}]:${raft_port}"
node_sh "${target}" "
  sed -i 's|^client_submit_listen:.*|client_submit_listen: \"${mapped}\"|' ${cfg}
  sed -i 's|client_submit_address: \"${target}:${submit_port}\"|client_submit_address: \"${mapped}\"|' ${cfg}
"
mapped_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
kafd_start "${target}" || true

mapped_alias_rejected() {
  kafd_log_since "${target}" "${mapped_since}" \
    | grep -q 'raft_address and client_submit_address must differ'
}
mapped_boundary_closed() {
  node_process_absent "${target}" keepafloatd &&
    node_tcp_listener_absent "${target}" "${raft_port}" &&
    node_tcp_listener_absent "${target}" "${submit_port}"
}
check "IPv4-mapped alias is rejected before either listener starts" \
  wait_until 20 mapped_alias_rejected
check "config rejection leaves no daemon, Raft listener, or submit listener" \
  wait_until 20 holds_for 5 mapped_boundary_closed
check "config-rejected node stays VIP-less" holds_for 5 node_lacks_all_vips "${target}"

kafd_stop "${target}"
restore_cluster_configs "${tag}"
clean_reform
check "exact config repair restores all three nodes" wait_for_steady_state
scenario_end
