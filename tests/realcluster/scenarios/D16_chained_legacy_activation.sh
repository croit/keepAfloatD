#!/usr/bin/env bash
# Reject the retired plaintext authentication format on both listeners.
SCENARIO_NAME=D16_chained_legacy_activation
source "$(dirname "$0")/../scenario.sh"
scenario_start "legacy authentication fails closed and modern status remains available"

check "cluster is available before legacy rejection probes" wait_for_available_cluster
payload="$(base64 -w0 "${HERE}/raft-deadline-probe.py")"
auth_payload="$(base64 -w0 "${HERE}/../e2e/scripts/auth_wire.py")"
for index in "${!NODE_IPS[@]}"; do
  source_ip="${NODE_IPS[$index]}"
  target_index=$(((index + 1) % ${#NODE_IPS[@]}))
  target_id="${NODE_RAFT_IDS[$target_index]}"
  config="/etc/keepafloatd/config-$(instance_for_ip "${source_ip}").yaml"
  check "both listeners reject old framing and authenticated status recovers on ${target_id}" \
    node_sh "${source_ip}" \
      "timeout 15 python3 -c \"import base64,sys,types; module=types.ModuleType('auth_wire'); exec(base64.b64decode('${auth_payload}'),module.__dict__); sys.modules['auth_wire']=module; sys.argv=['probe','${config}','${target_id}','--legacy-rejection']; exec(compile(base64.b64decode('${payload}'),'probe','exec'))\""
done
check "legacy probes did not disrupt cluster service" wait_for_available_cluster
scenario_end
