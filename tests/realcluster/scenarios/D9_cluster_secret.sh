#!/usr/bin/env bash
# D9: Cluster-secret authentication. A node configured with a DIFFERENT cluster_secret than
# its peers must be refused at the Raft handshake (secrets_match) and cannot join - it holds
# no VIPs. Restoring the matching secret lets it rejoin. The current daemon requires a non-empty
# secret, so cleanup restores the original common value instead of deleting the field.
SCENARIO_NAME=D9_cluster_secret
source "$(dirname "$0")/../scenario.sh"
scenario_start "wrong cluster_secret is refused; node cannot join until it matches"

good="kafd-campaign-secret"
bad="WRONG-secret"
original_secret="$(node_sh "${NODE_IPS[0]}" "sed -n 's/^cluster_secret:[[:space:]]*\"\\(.*\\)\"/\\1/p' /etc/keepafloatd/config-$(instance_for_ip "${NODE_IPS[0]}").yaml")"
[[ -n "${original_secret}" ]] || { fail "baseline cluster_secret is missing"; exit 1; }
# Write the secret into a node's config WITHOUT restarting (so we can restart the whole cluster
# together - staggered single-node restarts create transient incarnation splits that take long to
# resolve, which is an operational artifact of secret rotation, not a keepafloatd fault).
write_secret() {  # write_secret <ip> <secret|"">
  local ip="${1:?}" sec="${2-}" inst; inst="$(instance_for_ip "${ip}")"
  if [[ -n "${sec}" ]]; then
    node_sh "${ip}" "cfg=/etc/keepafloatd/config-${inst}.yaml; sed -i '/^cluster_secret:/d' \$cfg; echo 'cluster_secret: \"${sec}\"' >> \$cfg"
  else
    node_sh "${ip}" "cfg=/etc/keepafloatd/config-${inst}.yaml; sed -i '/^cluster_secret:/d' \$cfg"
  fi
}
restart_all_together() {  # coordinated near-simultaneous restart of all nodes
  local ip pids=()
  for ip in "${NODE_IPS[@]}"; do node_sh "${ip}" "systemctl restart keepafloatd@$(instance_for_ip "${ip}")" & pids+=("$!"); done
  wait_for_all "${pids[@]}" 2>/dev/null
}

# Give the two peers the good secret, node 3 the wrong one - written first, then one coordinated
# restart so the matching majority forms cleanly and node 3 is fenced.
evid "peers ${NODE_IPS[0]},${NODE_IPS[1]} get good secret; ${NODE_IPS[2]} gets WRONG secret"
write_secret "${NODE_IPS[0]}" "${good}"
write_secret "${NODE_IPS[1]}" "${good}"
write_secret "${NODE_IPS[2]}" "${bad}"
mismatch_since="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
restart_all_together
sleep 8

# The two matching peers should form/keep a cluster; node 3 must be fenced out (no VIPs, and
# its log shows secret mismatch / it can't join the others).
check "mismatched node ${NODE_IPS[2]} holds no VIPs" wait_until 40 node_lacks_all_vips "${NODE_IPS[2]}"
mismatch_log="$(kafd_log_since "${NODE_IPS[1]}" "${mismatch_since}" | grep -iE 'secret.*mismatch|dropping' | tail -2)"
evid "secret-mismatch evidence (peer log):"; evid "${mismatch_log:-  (no explicit mismatch line captured)}"
check "matching peer logs the secret mismatch" test -n "${mismatch_log}"
# The two good peers should still hold the VIPs between them. Setting secrets restarts all three,
# so the two matching peers must reform a fresh cluster (the wrong-secret node was a member, now
# fenced) before redistributing - give that reform + settle a generous window.
check "matching peers keep VIPs uniquely held and reachable" \
  wait_for_live_service_without 90 "${NODE_IPS[2]}"

# Fix node 3's secret -> it rejoins. Restart it alone (the other two already agree on `good`, so
# node 3 joining their incarnation is a clean single-node rejoin, not a reform).
evid "correcting ${NODE_IPS[2]} secret -> should rejoin"
write_secret "${NODE_IPS[2]}" "${good}"
node_sh "${NODE_IPS[2]}" "systemctl restart keepafloatd@$(instance_for_ip "${NODE_IPS[2]}")"
check "corrected node rejoins with safe all-node service" wait_for_available_cluster

# Cleanup: restore the original required secret on every node via one coordinated restart.
for ip in "${NODE_IPS[@]}"; do write_secret "${ip}" "${original_secret}"; done
restart_all_together
check "original-secret baseline restored" wait_for_steady_state

scenario_end
