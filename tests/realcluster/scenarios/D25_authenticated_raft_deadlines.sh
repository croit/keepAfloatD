#!/usr/bin/env bash
# D25: Authenticated idle/prefix/body pressure must expire and release listener capacity.
SCENARIO_NAME=D25_authenticated_raft_deadlines
source "$(dirname "$0")/../scenario.sh"
scenario_start "authenticated stalled Raft connections expire and normal cluster traffic recovers"

check "cluster is available before authenticated pressure" wait_for_available_cluster
payload="$(base64 -w0 "${HERE}/raft-deadline-probe.py")"
auth_payload="$(base64 -w0 "${HERE}/../e2e/scripts/auth_wire.py")"
for index in "${!NODE_IPS[@]}"; do
  source_ip="${NODE_IPS[$index]}"
  target_index=$(((index + 1) % ${#NODE_IPS[@]}))
  target_id="${NODE_RAFT_IDS[$target_index]}"
  config="/etc/keepafloatd/config-$(instance_for_ip "${source_ip}").yaml"
  check "authenticated deadlines and renewed status traffic from voter ${NODE_RAFT_IDS[$index]} to ${target_id}" \
    node_sh "${source_ip}" \
      "timeout 40 python3 -c \"import base64,sys,types; module=types.ModuleType('auth_wire'); exec(base64.b64decode('${auth_payload}'),module.__dict__); sys.modules['auth_wire']=module; sys.argv=['probe','${config}','${target_id}']; exec(compile(base64.b64decode('${payload}'),'probe','exec'))\""
  check "all voters agree and VIPs remain unique and reachable after pressure" wait_for_available_cluster
done
scenario_end
