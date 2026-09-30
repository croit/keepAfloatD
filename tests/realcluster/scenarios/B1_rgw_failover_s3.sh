#!/usr/bin/env bash
# B1: Real RGW/front-end service failover with S3 continuity. The keepafloatd health probe curls
# the local HAProxy health endpoint; when the local front end and RGW backend die on a holder, its
# health flips false, keepafloatd moves the VIPs off it, and S3 through the VIP keeps working on a
# healthy node. Then both services are restored and the node re-enters the pool.
SCENARIO_NAME=B1_rgw_failover_s3
source "$(dirname "$0")/../scenario.sh"
scenario_start "local RGW/front-end death drives VIP failover; S3 stays available through the VIP"

vip="${VIPS[0]}"
if s3_ensure_bucket "${vip}"; then
  evid "  ✓ S3 test user and bucket are ready through VIP ${vip}"
else
  evid "  ✗ S3 test user or bucket setup failed through VIP ${vip}"
  _PASS=0
  scenario_end
fi
content="b1-$(date +%s)"
check "S3 put through VIP ${vip} succeeds" s3_put "${vip}" "${content}"
check "S3 get through VIP ${vip} matches" s3_get_ok "${vip}" "${content}"

snapshot_vips
holder="$(holder_for_vip "${vip}")"
evid "VIP ${vip} on ${holder}; stopping its local RGW/front end (real service failure)"
set_unhealthy "${holder}"

# keepafloatd must move all VIPs off the now-unhealthy node.
check "unhealthy holder ${holder} releases its VIPs" wait_until 60 node_lacks_all_vips "${holder}"
check "VIPs settle uniquely on reachable healthy survivors" \
  wait_for_live_service_without 60 "${holder}"

# Critical: S3 through the VIP still works (now served by a healthy node).
check "S3 still readable through VIP ${vip} after failover" wait_until 30 s3_get_ok "${vip}" "${content}"
new_content="b1b-$(date +%s)"
check "S3 put still works through VIP after failover" s3_put "${vip}" "${new_content}"
check "S3 get returns new content" s3_get_ok "${vip}" "${new_content}"

# Restore the services. The node becomes eligible again but the nopreempt baseline must not require
# it to take a VIP away from a healthy survivor.
set_healthy "${holder}"
check "recovered node rejoins without disrupting unique ownership" \
  wait_until 90 holds_for 5 post_rgw_stable
check "S3 remains readable after service recovery" s3_get_ok "${vip}" "${new_content}"

scenario_end
