#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
export ROOT_DIR

# shellcheck source=tests/e2e/scripts/lib.sh
. "${ROOT_DIR}/tests/e2e/scripts/lib.sh"

# Stale-survivor rejection (the hard split-brain case).
#
#   C = node-a : partitioned away, KEEPS its in-memory state, never restarted.
#   A,B = {node-b, node-c} : the majority that loses all state and reforms a brand-new cluster.
#
# node-a is isolated; the majority then wipes its state and cold-forms a fresh cluster (a new
# incarnation) while node-a is still away. When the partition heals, node-a returns holding the OLD
# incarnation and an inflated Raft term. Without the cluster-incarnation fence node-a could win an
# election on its longer, higher-term log and OVERWRITE the reformed majority. We assert the safe
# outcome: the reformed majority fences node-a's RPCs, node-a recognizes it is stale, resets (exits
# for a supervisor restart, modeled here by an explicit start), and rejoins the new cluster blank.
wait_for_single_agreed_leader 20

# 1. Isolate node-a using node-a-side blackholes ONLY, so the isolation survives the node-b/node-c
#    restarts below (their base routes are restored on restart; node-a's are not touched).
isolate_node_a
# The majority {node-b, node-c} takes over every VIP; node-a loses quorum and unbinds. node-a stays
# running here, holding stale Raft state and bumping its term via failed elections.
wait_for_even_over_nodes 25 node-b node-c
assert_node_lacks_all_vips node-a

# 2. Wipe the majority and let it reform a NEW cluster while node-a is still partitioned.
reform_checkpoint="$(log_checkpoint)"
kill_service node-b KILL
kill_service node-c KILL
wait_for_service_exit node-b 10
wait_for_service_exit node-c 10
start_service node-b
start_service node-c
wait_for_service_running node-b 10
wait_for_service_running node-c 10

# A fresh 'auto-formed' line after the checkpoint proves the pair cold-formed a new cluster (new
# incarnation) rather than recovering the old one; they return to the two-node VIP layout.
wait_for_log_any_after "${reform_checkpoint}" 40 'auto-formed Raft cluster'
wait_for_even_over_nodes 40 node-b node-c
assert_unique_holders

# 3. Heal the partition. node-a (old incarnation) now meets the reformed majority (new incarnation).
heal_checkpoint="$(log_checkpoint)"
autoform_at_heal="$(cluster_logs_count 'auto-formed Raft cluster')"
heal_node_a_partition

# 4a. The fence is active: the reformed majority drops node-a's foreign-incarnation Raft RPCs (and
#     node-a drops theirs), so node-a can never overwrite the new cluster.
wait_for_log_any_after "${heal_checkpoint}" 30 'cluster_epoch mismatch'

# 4b. node-a recognizes it is a stale survivor and resets by exiting; a supervisor would relaunch it
#     blank (compose uses restart:"no", so we start it explicitly to model systemd Restart=on-failure).
wait_for_service_exit node-a 30
start_service node-a
wait_for_service_running node-a 10

# 4c. Returning blank, node-a JOINS the new cluster via replication — it must NOT cold-form again.
wait_for_log_any_after "${heal_checkpoint}" 45 'joining via replication instead of forming a new one'

# 5. The cluster converges to the steady 3-node layout: unique holders, one agreed leader across all
#    three nodes. node-a holding stale state did not split the cluster at any point.
wait_for_even_over_nodes 45 "${NODES[@]}"
assert_unique_holders
wait_for_single_agreed_leader 20

# 6. node-a rejoined the EXISTING reformed cluster; its restart added no new cold-form. The
#    'auto-formed' count must be unchanged since the heal (only the step-2 reform formed a cluster).
autoform_final="$(cluster_logs_count 'auto-formed Raft cluster')"
if (( autoform_final != autoform_at_heal )); then
  dump_cluster_diagnostics
  fail "node-a re-formed a cluster instead of joining the reformed majority (auto-formed ${autoform_at_heal} -> ${autoform_final})"
  exit 1
fi
