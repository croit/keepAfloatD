#!/usr/bin/env bash
# Isolate a current VIP owner while the other two nodes reform. Its original boot
# must expire and release its VIPs; after healing, a fresh boot joins the majority.
SCENARIO_NAME=D7_stale_survivor
source "$(dirname "$0")/../scenario.sh"
scenario_start "isolated owner expires safely and rejoins the reformed majority with a fresh boot"

snapshot_vips
survivor="$(holder_for_vip "${VIPS[0]}")"
instance_for_ip "$survivor" >/dev/null
mapfile -t peers < <(nodes_except "$survivor")
test "${#peers[@]}" -eq 2
original_vips=()
for vip in "${VIPS[@]}"; do
  if node_has_vip_bound "$survivor" "$vip"; then original_vips+=("$vip"); fi
done
test "${#original_vips[@]}" -gt 0
evid "isolating stale survivor ${survivor}; peers ${peers[*]} will reform"
survivor_context="$(timed_node_command "${survivor}" :)"
original_replica="$(node_admitted_replica "$survivor" "$survivor_context")"

# Blackhole Raft and submit in both directions, leaving observation over SSH available.
partition_node "${survivor}"
sleep 5

# The two peers keep a quorum; the isolated owner cannot renew its permission.
check "isolated survivor sheds VIPs" wait_until 60 node_lacks_all_vips "${survivor}"

# Force the peers to reform under a new incarnation: stop both, then start both fresh. With the
# survivor partitioned, the two reforming peers can't see its old incarnation and mint a new one.
evid "reforming peers from scratch (new incarnation)"
for ip in "${peers[@]}"; do kafd_stop "${ip}"; done
sleep 3
for ip in "${peers[@]}"; do kafd_start "${ip}"; done
check "reformed peers complete activation without overlapping VIPs" \
  wait_for_startup_activation 30 "${peers[@]}"
check "reformed peers agree on an exact current leader" \
  wait_until 90 surviving_peers_agree_on_leader "${peers[@]}"
check "peers hold every VIP uniquely while the survivor is isolated" \
  wait_for_live_service_without 90 "${survivor}"

check "original boot expired terminally and released every original VIP before healing" \
  permission_expiry_observed "$survivor" "$survivor_context" "${original_vips[@]}"
rejoin_context="$(restarted_service_context "$survivor" "$survivor_context")"
promotion_contexts=()
for peer in "${peers[@]}"; do
  promotion_contexts+=("$peer" "$(timed_node_command "$peer" :)")
done

evid "healing partition; the replacement boot must complete an exact learner join"
heal_node "${survivor}"
check "replacement boot completes its policy-derived activation window without overlap" \
  wait_for_startup_activation 30 "$survivor"
check "reformed majority commits promotion of the fresh survivor boot" \
  wait_until 30 fresh_join_observed "$survivor" "$rejoin_context" "$original_replica" \
    "${promotion_contexts[@]}"

# Eventually the survivor rejoins cleanly via replication under the new incarnation. Nopreempt
# does not require it to take a VIP back from a healthy incumbent.
check "survivor rejoins cleanly under new incarnation" wait_until 120 all_vips_uniquely_held
check "all-node service is safely available after the fresh-boot join" \
  wait_for_available_cluster

scenario_end
