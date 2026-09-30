#!/usr/bin/env bash
# Final read-only audit after the real-cluster campaign and bounded soak.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/realcluster/lib.sh
source "${HERE}/lib.sh"

expected_buildid="${FIXED_BUILDID:?FIXED_BUILDID is required}"
campaign_since="${REALCLUSTER_AUDIT_SINCE:?Set REALCLUSTER_AUDIT_SINCE to the campaign start time}"
baseline_hashes="${REALCLUSTER_BASELINE_HASHES:-${HERE}/results/baseline-config.sha256}"

assert_cluster_secret_consistent
[[ -f "${baseline_hashes}" ]]
wait_for_steady_state
assert_unique_holders
even_over_nodes "${NODE_IPS[@]}"
all_vips_pingable
all_nodes_agree_on_leader
campaign_config_backups_absent
campaign_runtime_artifacts_absent
partition_rules_absent
ownership_test_artifacts_absent

ceph_status="$(ceph_health_status)"
[[ "${ceph_status}" == "HEALTH_OK" ]]
printf 'ceph=%s leader=%s assignments=%s\n' \
  "${ceph_status}" "$(cluster_leader_id)" "$(current_assignments_summary)"

for ip in "${NODE_IPS[@]}"; do
  inst="$(instance_for_ip "${ip}")"
  cfg="/etc/keepafloatd/config-${inst}.yaml"
  expected_cfg_hash="$(awk -v ip="${ip}" '$1 == ip {print $2}' "${baseline_hashes}")"
  actual_cfg_hash="$(node_sh "${ip}" "sha256sum ${cfg} | awk '{print \$1}'")"
  buildid="$(running_keepafloatd_buildid "${ip}")"
  daemon="$(kafd_active "${ip}")"
  data_path=inactive
  service_path_healthy "${ip}" && data_path=active
  defensive="$(journal_event_count "${ip}" "${campaign_since}" \
    'Defensive|LogIndexNotFound|quit RaftCore|panicked at')"

  node_sh "${ip}" "
    set -euo pipefail
    vlan_address_count=\$(ip -N -j -4 addr show | jq -er \
      --arg address '${VLAN_TEST_VIP}' \
      '[.[].addr_info[]? | select(.local == \$address and .prefixlen == 32)] | length')
    test ! -e /run/keepafloatd-unhealthy &&
    test ! -e /run/kafd-notify.log &&
    test ! -e /usr/local/bin/kafd-notify.sh &&
    test ! -e /tmp/kafd-notify.log &&
    test ! -e /usr/bin/keepafloatd.d15-current &&
    test ! -e /usr/bin/keepafloatd.d16-current &&
    test ! -e /run/kafd-submit-blocker.pid &&
    test ! -e /run/kafd-ipfault &&
    test ! -e /run/systemd/system/keepafloatd@${inst}.service.d/zz-kafd-ipfault.conf &&
    test ! -e /sys/class/net/${IFACE}.${VLAN_TEST_ID} &&
    test \"\$vlan_address_count\" -eq 0 &&
    grep -Fq '127.0.0.1:9400/healthz' ${cfg}
  "

  [[ "${daemon}" == "active" ]]
  [[ "${data_path}" == "active" ]]
  [[ "${buildid}" == "${expected_buildid}" ]]
  [[ "${expected_cfg_hash}" =~ ^[a-f0-9]{64}$ ]]
  [[ "${actual_cfg_hash}" == "${expected_cfg_hash}" ]]
  [[ "${defensive}" == "0" ]]
  printf '%s daemon=%s data_path=%s buildid=%s config=%s artifacts=clean defensive=%s\n' \
    "${ip}" "${daemon}" "${data_path}" "${buildid}" "${actual_cfg_hash}" "${defensive}"
done

[[ "$(ipv4_bound_count "${MUTATION_TEST_VIP}" 32)" == "0" ]]
[[ "$(ipv4_bound_count "${OWNERSHIP_ADMIN_VIP}" 32)" == "0" ]]
[[ "$(ipv4_bound_count "${OWNERSHIP_FOREIGN_VIP}" 32)" == "0" ]]
[[ "$(ipv4_bound_count "${CIDR_TEST_VIP}" "${CIDR_TEST_PREFIX}")" == "0" ]]
[[ "$(ipv6_bound_count "${IPV6_TEST_VIP}" 128)" == "0" ]]
ownership_markers_absent

printf 'FINAL AUDIT PASS\n'
