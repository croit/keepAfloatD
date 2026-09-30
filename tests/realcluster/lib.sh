#!/usr/bin/env bash
# Shared helpers for the keepafloatd real-cluster campaign.
# Ported from tests/e2e/scripts/lib.sh, but driving the real 3-node cluster over the
# croit mgmt-node SSH jump host instead of docker-compose.
#
# Assertion *semantics* (unique holders, even spread, arpable) are kept identical to the
# docker e2e lib so results are comparable.

set -Euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${HERE}/env.sh"
# shellcheck source=evidence.sh
source "${HERE}/evidence.sh"
mkdir -p "${SSH_CTL_DIR}" 2>/dev/null || true

# Exact config-backup suffixes owned by scenarios and soak. Unknown backups are operator state: the
# campaign must preserve them, while these names must be absent before capture and after cleanup.
declare -ag REALCLUSTER_CAMPAIGN_BACKUP_TAGS=(
  c2 b3-notify d5 d11 d12 d13 d14 d16 d17 d18 d19 d20 d21 d22 d23 d24 soak
)

# ---------------------------------------------------------------------------
# Logging / failures
# ---------------------------------------------------------------------------
log()  { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fail() { printf '[%s] FAIL: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; return 1; }

scenario_unexpected_error() {
  local status="${1:?}" command="${2:-unknown command}"
  trap - ERR
  # Errtrace reaches background snapshots and command substitutions. Only the scenario's original
  # shell may restore the cluster; child shells propagate failure for their parent to handle.
  if [[ "${BASHPID}" != "${_SCENARIO_GUARD_PID:-}" ]]; then
    exit "${status}"
  fi
  evid "  ✗ unchecked command failed (${status}): ${command}"
  _PASS=0
  scenario_end
}

install_scenario_error_guard() {
  _SCENARIO_GUARD_PID="${BASHPID}"
  trap 'scenario_unexpected_error "$?" "$BASH_COMMAND"' ERR
}

# ---------------------------------------------------------------------------
# SSH plumbing: mgmt-node and node-via-jump-host command execution.
# ---------------------------------------------------------------------------
# Run a command ON the mgmt node.
mgmt_sh() {
  timeout --foreground "${SSH_COMMAND_TIMEOUT:?}" ssh ${SSH_OPTS} -l root "${MGMT}" "$@"
}

# Run a command ON a cluster node, proxied through the mgmt node.
# Usage: node_sh <node_ip> <command-string>
node_sh() {
  local ip="${1:?node ip required}"; shift
  local cmd="$*"
  # Single-quote-safe: base64 the inner command so quoting survives two SSH hops.
  # Inner hop uses NODE_SSH_OPTS (no ControlMaster - that dir only exists on the workstation).
  local b64; b64="$(printf '%s' "${cmd}" | base64 -w0)"
  mgmt_sh "ssh ${NODE_SSH_OPTS} -i ${NODE_KEY} -l root ${ip} \"echo ${b64} | base64 -d | bash\""
}

# Map a node IP to its keepafloatd@ instance name.
instance_for_ip() {
  local ip="${1:?ip required}" i
  for i in "${!NODE_IPS[@]}"; do
    [[ "${NODE_IPS[$i]}" == "${ip}" ]] && { printf '%s\n' "${NODE_INSTANCES[$i]}"; return 0; }
  done
  return 1
}
serverid_for_ip() {
  local ip="${1:?ip required}" i
  for i in "${!NODE_IPS[@]}"; do
    [[ "${NODE_IPS[$i]}" == "${ip}" ]] && { printf '%s\n' "${NODE_SERVERIDS[$i]}"; return 0; }
  done
  return 1
}

# ---------------------------------------------------------------------------
# croit API (token cached per run).
# ---------------------------------------------------------------------------
_TOKEN=""
croit_token() {
  local command
  [[ -n "${_TOKEN}" ]] && { printf '%s\n' "${_TOKEN}"; return 0; }
  command="set -o pipefail; \
    curl -fsSk -X POST $(shell_quote "${CROIT_URL}/api/auth/login-form") \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode grant_type=password \
    --data-urlencode $(shell_quote "username=${CROIT_USER}") \
    --data-urlencode password@- \
    | python3 -c 'import sys,json; token=json.load(sys.stdin).get(\"access_token\"); \
isinstance(token, str) and token.strip() or sys.exit(1); print(token)'"
  _TOKEN="$(printf '%s' "${CROIT_PASS}" | mgmt_sh "bash -c $(shell_quote "${command}")")" \
    || { _TOKEN=""; return 1; }
  [[ -n "${_TOKEN}" ]] || return 1
  printf '%s\n' "${_TOKEN}"
}
croit_api() {  # croit_api GET /servers ...
  local method="${1:?}" path="${2:?}"; shift 2
  local t args="" arg
  t="$(croit_token)" || return 1
  for arg in "$@"; do args+=" $(shell_quote "${arg}")"; done
  mgmt_sh "curl -fsSk -X $(shell_quote "${method}") \
    $(shell_quote "${CROIT_URL}/api${path}") \
    -H $(shell_quote "Authorization: Bearer ${t}")${args}"
}

# Read Ceph health without depending on the management UI credential. The generated monitoring
# identity is read-only and available on every cluster node; retain the API as a fallback for lab
# variants that do not provision that keyring.
ceph_health_status() {
  local status=""
  status="$(node_sh "${NODE_IPS[0]}" "timeout 15 ceph -n client.croit-monitoring -k /etc/ceph/ceph.client.croit-monitoring.keyring status -f json 2>/dev/null | jq -er .health.status" 2>/dev/null)" || true
  if [[ -n "${status}" ]]; then
    printf '%s\n' "${status}"
    return 0
  fi
  croit_api GET /cluster/status | jq -er .cephStatus.health.status
}

# Current keepafloatd requires one common non-empty secret. The croit HA generator used by this lab
# can still overwrite the manually repaired field, so preserve a campaign-local value across child
# scenarios and restore it before any daemon start. The value is never logged.
REALCLUSTER_SECRET="${REALCLUSTER_SECRET:-}"
REALCLUSTER_SECRET_CHANGED=0

ensure_cluster_secret_on_node() {
  local ip="${1:?}" inst cfg current
  [[ -n "${REALCLUSTER_SECRET}" ]] || return 1
  inst="$(instance_for_ip "${ip}")"; cfg="/etc/keepafloatd/config-${inst}.yaml"
  if ! current="$(node_sh "${ip}" "sed -n 's/^cluster_secret:[[:space:]]*\"\\(.*\\)\"/\\1/p' ${cfg}")"; then
    return 1
  fi
  [[ "${current}" == "${REALCLUSTER_SECRET}" ]] && return 0
  node_sh "${ip}" "sed -i '/^cluster_secret:/d' ${cfg}; printf '\\ncluster_secret: \"${REALCLUSTER_SECRET}\"\\n' >> ${cfg}"
  REALCLUSTER_SECRET_CHANGED=1
}

prepare_cluster_secret() {
  local ip current candidate=""
  REALCLUSTER_SECRET_CHANGED=0
  if [[ -z "${REALCLUSTER_SECRET}" ]]; then
    for ip in "${NODE_IPS[@]}"; do
      if ! current="$(node_sh "${ip}" "sed -n 's/^cluster_secret:[[:space:]]*\"\\(.*\\)\"/\\1/p' /etc/keepafloatd/config-$(instance_for_ip "${ip}").yaml")"; then
        return 1
      fi
      [[ -n "${candidate}" || -z "${current}" ]] || candidate="${current}"
    done
    if [[ "${candidate}" =~ ^[A-Za-z0-9._-]{16,256}$ ]]; then
      REALCLUSTER_SECRET="${candidate}"
    else
      REALCLUSTER_SECRET="$(openssl rand -hex 32)"
    fi
    export REALCLUSTER_SECRET
  fi
  [[ "${REALCLUSTER_SECRET}" =~ ^[A-Za-z0-9._-]{16,256}$ ]] || return 1
  for ip in "${NODE_IPS[@]}"; do ensure_cluster_secret_on_node "${ip}" || return 1; done
  export REALCLUSTER_SECRET
}

# Read-only final-audit check. Unlike prepare_cluster_secret, this must never repair drift before
# reporting the cluster clean.
assert_cluster_secret_consistent() {
  local ip current expected=""
  for ip in "${NODE_IPS[@]}"; do
    current="$(node_sh "${ip}" "sed -n 's/^cluster_secret:[[:space:]]*\"\\(.*\\)\"/\\1/p' /etc/keepafloatd/config-$(instance_for_ip "${ip}").yaml")" \
      || return 1
    [[ "${current}" =~ ^[A-Za-z0-9._-]{16,256}$ ]] || return 1
    if [[ -z "${expected}" ]]; then
      expected="${current}"
    elif [[ "${current}" != "${expected}" ]]; then
      return 1
    fi
  done
}

# ---------------------------------------------------------------------------
# keepafloatd service control (per node).
# ---------------------------------------------------------------------------
kafd_active()  {
  local ip="${1:?}"
  # Preserve SSH/query failures, but normalize systemd's ordinary non-zero "inactive" status into
  # readable evidence so leader checks can ignore a stopped voter without accepting stale logs.
  node_sh "${ip}" "systemctl is-active keepafloatd@$(instance_for_ip "${ip}") 2>/dev/null || true"
}
node_active()  { [[ "$(kafd_active "${1:?}")" == "active" ]]; }                # predicate
all_daemons_active() {
  local ip
  for ip in "${NODE_IPS[@]}"; do node_active "${ip}" || return 1; done
}
node_process_absent() {
  local ip="${1:?}" process="${2:?}"
  [[ "${process}" =~ ^[A-Za-z0-9._-]+$ ]] || return 1
  node_sh "${ip}" "if pgrep -x -- '${process}' >/dev/null; then exit 1; else status=\$?; test \"\${status}\" -eq 1; fi"
}
node_tcp_listener_absent() {
  local ip="${1:?}" port="${2:?}"
  [[ "${port}" =~ ^[0-9]+$ ]] && (( port >= 1 && port <= 65535 )) || return 1
  node_sh "${ip}" "listeners=\$(ss -H -lntp 'sport = :${port}') || exit 1; test -z \"\${listeners}\""
}
node_tcp_listener_owned_only_by_pid() {
  local ip="${1:?}" port="${2:?}" pid="${3:?}"
  [[ "${port}" =~ ^[0-9]+$ ]] && (( port >= 1 && port <= 65535 )) || return 1
  [[ "${pid}" =~ ^[0-9]+$ ]] && (( pid > 1 )) || return 1
  node_sh "${ip}" "
    set -euo pipefail
    listeners=\$(ss -H -lntp 'sport = :${port}') || exit 1
    test -n \"\${listeners}\"
    printf '%s\\n' \"\${listeners}\" | grep -q 'pid=${pid},'
    unexpected=\$(printf '%s\\n' \"\${listeners}\" | grep -oE 'pid=[0-9]+' | grep -vx 'pid=${pid}' || true)
    test -z \"\${unexpected}\"
  "
}
vip_pingable() { mgmt_sh "ping -c1 -W2 ${1:?} >/dev/null 2>&1"; }              # predicate
kafd_stop()    { local ip="${1:?}"; node_sh "${ip}" "systemctl stop keepafloatd@$(instance_for_ip "${ip}")"; }
kafd_start()   { local ip="${1:?}"; ensure_cluster_secret_on_node "${ip}" || return 1; node_sh "${ip}" "systemctl reset-failed keepafloatd@$(instance_for_ip "${ip}") 2>/dev/null; systemctl start keepafloatd@$(instance_for_ip "${ip}")"; }
kafd_restart() { local ip="${1:?}"; ensure_cluster_secret_on_node "${ip}" || return 1; node_sh "${ip}" "systemctl restart keepafloatd@$(instance_for_ip "${ip}")"; }

running_keepafloatd_buildid() {
  local ip="${1:?}" inst
  inst="$(instance_for_ip "${ip}")" || return 1
  node_sh "${ip}" "
    set -euo pipefail
    unit=keepafloatd@${inst}
    systemctl is-active --quiet \"\$unit\"
    pid=\$(systemctl show --property=MainPID --value \"\$unit\")
    test \"\$pid\" -gt 1
    file -L \"/proc/\$pid/exe\" | grep -oE 'BuildID\\[sha1\\]=[a-f0-9]+' | cut -d= -f2
  "
}

replace_keepafloatd_binary() {  # replace_keepafloatd_binary <node> <node-local source>
  local ip="${1:?}" source="${2:?}" inst
  [[ "${source}" =~ ^/[A-Za-z0-9_./-]+$ ]] || return 1
  inst="$(instance_for_ip "${ip}")" || return 1
  node_sh "${ip}" "
    set -euo pipefail
    unit=keepafloatd@${inst}
    systemctl stop \"\$unit\"
    test \"\$(systemctl is-active \"\$unit\" 2>/dev/null || true)\" = inactive
    test \"\$(systemctl show --property=MainPID --value \"\$unit\")\" = 0
    cp -f '${source}' /usr/bin/keepafloatd
    systemctl reset-failed \"\$unit\" 2>/dev/null || true
    systemctl start \"\$unit\"
    systemctl is-active --quiet \"\$unit\"
    pid=\$(systemctl show --property=MainPID --value \"\$unit\")
    test \"\$pid\" -gt 1
    installed=\$(sha256sum /usr/bin/keepafloatd | awk '{print \$1}')
    running=\$(sha256sum \"/proc/\$pid/exe\" | awk '{print \$1}')
    test \"\$running\" = \"\$installed\"
  "
}
# Hard kill (SIGKILL) - leaves the VIP orphaned on the kernel (no graceful unbind), simulating a
# silent death. We SIGKILL then `systemctl stop`: the unit has Restart=on-failure/RestartSec=1, so a
# bare SIGKILL is resurrected by systemd in ~1s (the node would never actually be "gone" and its VIP
# never stales out). `stop` after the process is already dead marks the unit inactive WITHOUT running
# the graceful unbind, so the node stays down and the orphan VIP remains - the intended fault.
kafd_kill() {
  local ip="${1:?}" inst unit
  inst="$(instance_for_ip "${ip}")" || return 1
  unit="$(shell_quote "keepafloatd@${inst}")"
  node_sh "${ip}" "systemctl kill -s SIGKILL ${unit} && \
    systemctl stop ${unit} && ! systemctl is-active --quiet ${unit}"
}

# Strip ANSI and pull last N journal lines for a node's instance.
kafd_log() {
  local ip="${1:?}" n="${2:-40}"
  node_sh "${ip}" "set -o pipefail; journalctl -u keepafloatd@$(instance_for_ip "${ip}") -n ${n} -o cat 2>/dev/null | sed -E 's/\x1b\[[0-9;]*m//g'"
}
kafd_log_since() {
  local ip="${1:?}" since="${2:?}"
  node_sh "${ip}" "set -o pipefail; journalctl -u keepafloatd@$(instance_for_ip "${ip}") --since '${since}' -o cat 2>/dev/null | sed -E 's/\x1b\[[0-9;]*m//g'"
}

journal_event_count() {  # journal_event_count <ip> <since> <extended-regex>
  local ip="${1:?}" since="${2:?}" pattern="${3:?}" since_q pattern_q count
  printf -v since_q '%q' "${since}"
  printf -v pattern_q '%q' "${pattern}"
  count="$(node_sh "${ip}" "set -o pipefail; journalctl -u keepafloatd@$(instance_for_ip "${ip}") --since ${since_q} -o cat 2>/dev/null | sed -E 's/\\x1b\\[[0-9;]*m//g' | awk -v pattern=${pattern_q} '\$0 ~ pattern { count++ } END { print count + 0 }'")" || return 1
  [[ "${count}" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "${count}"
}

journal_event_count_all() {  # journal_event_count_all <ip> <extended-regex>
  local ip="${1:?}" pattern="${2:?}" pattern_q count
  printf -v pattern_q '%q' "${pattern}"
  count="$(node_sh "${ip}" "set -o pipefail; journalctl -u keepafloatd@$(instance_for_ip "${ip}") -o cat 2>/dev/null | sed -E 's/\\x1b\\[[0-9;]*m//g' | awk -v pattern=${pattern_q} '\$0 ~ pattern { count++ } END { print count + 0 }'")" || return 1
  [[ "${count}" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "${count}"
}

journal_event_absent_on_all_nodes_since() {
  local since="${1:?}" pattern="${2:?}" ip count
  for ip in "${NODE_IPS[@]}"; do
    count="$(journal_event_count "${ip}" "${since}" "${pattern}")" || return 1
    [[ "${count}" == 0 ]] || return 1
  done
}

journal_event_seen_on_any_node_since() {
  local since="${1:?}" pattern="${2:?}" ip count seen=0
  for ip in "${NODE_IPS[@]}"; do
    count="$(journal_event_count "${ip}" "${since}" "${pattern}")" || return 1
    (( count > 0 )) && seen=1
  done
  [[ "${seen}" -eq 1 ]]
}

file_event_count() {  # file_event_count <ip> <path> <extended-regex>
  local ip="${1:?}" path="${2:?}" pattern="${3:?}" path_q pattern_q count
  printf -v path_q '%q' "${path}"
  printf -v pattern_q '%q' "${pattern}"
  count="$(node_sh "${ip}" "test -r ${path_q} && awk -v pattern=${pattern_q} '\$0 ~ pattern { count++ } END { print count + 0 }' ${path_q}")" || return 1
  [[ "${count}" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "${count}"
}

# ---------------------------------------------------------------------------
# VIP / holder inspection (kernel ground truth).
# ---------------------------------------------------------------------------
# Batched cluster snapshot: query each node ONCE (in parallel) for the VIPs it holds, into
# the global associative array _SNAP_VIPS_ON[ip]=" v1 v2 ...". One sweep replaces N×M calls.
declare -A _SNAP_VIPS_ON=()
snapshot_vips() {
  _SNAP_VIPS_ON=()
  local ip pids=() failed=0 output token
  local td; td="$(mktemp -d)"
  for ip in "${NODE_IPS[@]}"; do
    ( node_sh "${ip}" "set -o pipefail; ip -o -4 addr show dev ${IFACE} 2>/dev/null | awk '{print \$4}' | cut -d/ -f1 | tr '\n' ' '" > "${td}/${ip}" 2>/dev/null ) &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "${pid}" 2>/dev/null || failed=1; done
  if [[ "${failed}" -ne 0 ]]; then rm -rf "${td}"; return 1; fi
  for ip in "${NODE_IPS[@]}"; do
    output="$(<"${td}/${ip}")"
    for token in ${output}; do
      [[ "${token}" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] \
        || { rm -rf "${td}"; return 1; }
    done
    _SNAP_VIPS_ON["${ip}"]=" ${output} "
  done
  rm -rf "${td}"
}

node_has_vip_bound() {  # uses last snapshot; call snapshot_vips first
  local ip="${1:?}" vip="${2:?}"
  [[ "${_SNAP_VIPS_ON[${ip}]:-}" == *" ${vip} "* ]]
}

holder_for_vip() {  # uses last snapshot
  local vip="${1:?}" ip holders=()
  for ip in "${NODE_IPS[@]}"; do
    [[ "${_SNAP_VIPS_ON[${ip}]:-}" == *" ${vip} "* ]] && holders+=("${ip}")
  done
  case "${#holders[@]}" in
    0) printf 'none\n' ;;
    1) printf '%s\n' "${holders[0]}" ;;
    *) printf 'duplicate:%s\n' "$(IFS=,; echo "${holders[*]}")" ;;
  esac
}

current_assignments_summary() {
  snapshot_vips || return 1
  local parts=() vip
  for vip in "${VIPS[@]}"; do parts+=("${vip}=$(holder_for_vip "${vip}")"); done
  printf '%s\n' "${parts[*]}"
}

node_lacks_all_vips() {  # refreshes snapshot
  snapshot_vips || return 1
  local ip="${1:?}" vip
  for vip in "${VIPS[@]}"; do node_has_vip_bound "${ip}" "${vip}" && return 1; done
  return 0
}

all_cluster_vips_absent() {  # refreshes once; every configured VIP must be absent on every node
  snapshot_vips || return 1
  local ip vip
  for ip in "${NODE_IPS[@]}"; do
    for vip in "${VIPS[@]}"; do
      node_has_vip_bound "${ip}" "${vip}" && return 1
    done
  done
  return 0
}

all_vips_uniquely_held() {  # refreshes snapshot
  snapshot_vips || return 1
  local vip holder
  for vip in "${VIPS[@]}"; do
    holder="$(holder_for_vip "${vip}")"
    case "${holder}" in none|duplicate:*) return 1 ;; esac
  done
}

# During a release-acknowledged handoff, a brief ownerless interval is safe but a duplicate is
# never safe. Use this only for transition windows; steady-state checks must still require one
# holder through all_vips_uniquely_held/assert_unique_holders.
no_vip_is_duplicate() {
  snapshot_vips || return 1
  local vip
  for vip in "${VIPS[@]}"; do
    [[ "$(holder_for_vip "${vip}")" != duplicate:* ]] || return 1
  done
}

assert_unique_holders() {  # refreshes snapshot
  snapshot_vips || { fail "could not read VIP state from every node"; return 1; }
  local vip holder
  for vip in "${VIPS[@]}"; do
    holder="$(holder_for_vip "${vip}")"
    case "${holder}" in
      none)        fail "no holder has VIP ${vip}"; return 1 ;;
      duplicate:*) fail "DOUBLE-BIND for VIP ${vip}: ${holder#duplicate:}"; return 1 ;;
    esac
  done
}

# Count an arbitrary address/prefix on the real kernels. These helpers deliberately query every
# node before returning: finding one holder is insufficient evidence because another node may hold
# the same address at the same instant.
ipv4_bound_count() {  # ipv4_bound_count <address> [prefix] [interface]
  local address="${1:?}" prefix="${2:-32}" iface="${3:-${IFACE}}" ip td pids=() failed=0 value
  td="$(mktemp -d)"
  for ip in "${NODE_IPS[@]}"; do
    ( node_sh "${ip}" "set -o pipefail; ip -o -4 addr show dev ${iface} 2>/dev/null | awk -v needle=' ${address}/${prefix} ' 'index(\$0, needle) { count++ } END { print count + 0 }'" > "${td}/${ip}" 2>/dev/null ) &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "${pid}" 2>/dev/null || failed=1; done
  if [[ "${failed}" -ne 0 ]]; then rm -rf "${td}"; return 1; fi
  for ip in "${NODE_IPS[@]}"; do
    value="$(<"${td}/${ip}")"
    [[ "${value}" =~ ^[0-9]+$ ]] || { rm -rf "${td}"; return 1; }
  done
  awk '{ total += $1 } END { print total + 0 }' "${td}"/*
  rm -rf "${td}"
}

# Identify an arbitrary IPv4 holder from one batched cluster snapshot. Querying nodes in a serial
# loop can report no holder when ownership moves from a not-yet-scanned node to an already-scanned
# one between SSH calls.
ipv4_holder_on_interface() {  # ipv4_holder_on_interface <address> [prefix] [interface]
  local address="${1:?}" prefix="${2:-32}" iface="${3:-${IFACE}}" ip td pids=() holders=() failed=0 value
  td="$(mktemp -d)"
  for ip in "${NODE_IPS[@]}"; do
    ( node_sh "${ip}" "set -o pipefail; ip -o -4 addr show dev ${iface} 2>/dev/null | awk -v needle=' ${address}/${prefix} ' 'index(\$0, needle) { found=1 } END { print found + 0 }'" > "${td}/${ip}" 2>/dev/null ) &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "${pid}" 2>/dev/null || failed=1; done
  if [[ "${failed}" -ne 0 ]]; then rm -rf "${td}"; return 1; fi
  for ip in "${NODE_IPS[@]}"; do
    value="$(<"${td}/${ip}")"
    case "${value}" in
      0) ;;
      1) holders+=("${ip}") ;;
      *) rm -rf "${td}"; return 1 ;;
    esac
  done
  rm -rf "${td}"
  case "${#holders[@]}" in
    0) printf 'none\n' ;;
    1) printf '%s\n' "${holders[0]}" ;;
    *) printf 'duplicate:%s\n' "$(IFS=,; echo "${holders[*]}")" ;;
  esac
}

wait_for_ipv4_holder() {  # wait_for_ipv4_holder <timeout_s> <address> [prefix] [interface]
  local timeout="${1:?}" address="${2:?}" prefix="${3:-32}" iface="${4:-${IFACE}}"
  local deadline holder
  deadline=$(( $(date +%s) + timeout ))
  while (( $(date +%s) < deadline )); do
    holder="$(ipv4_holder_on_interface "${address}" "${prefix}" "${iface}")"
    if [[ "${holder}" != "none" && "${holder}" != duplicate:* ]]; then
      printf '%s\n' "${holder}"
      return 0
    fi
    sleep 1
  done
  return 1
}

ipv6_bound_count() {  # ipv6_bound_count <address> [prefix] [interface]
  local address="${1:?}" prefix="${2:-128}" iface="${3:-${IFACE}}" ip td pids=() failed=0 value
  td="$(mktemp -d)"
  for ip in "${NODE_IPS[@]}"; do
    ( node_sh "${ip}" "set -o pipefail; ip -o -6 addr show dev ${iface} 2>/dev/null | awk -v needle=' ${address}/${prefix} ' 'index(\$0, needle) { count++ } END { print count + 0 }'" > "${td}/${ip}" 2>/dev/null ) &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "${pid}" 2>/dev/null || failed=1; done
  if [[ "${failed}" -ne 0 ]]; then rm -rf "${td}"; return 1; fi
  for ip in "${NODE_IPS[@]}"; do
    value="$(<"${td}/${ip}")"
    [[ "${value}" =~ ^[0-9]+$ ]] || { rm -rf "${td}"; return 1; }
  done
  awk '{ total += $1 } END { print total + 0 }' "${td}"/*
  rm -rf "${td}"
}

ipv4_vip_uniquely_held() { [[ "$(ipv4_bound_count "$@")" -eq 1 ]]; }
ipv6_vip_uniquely_held() { [[ "$(ipv6_bound_count "$@")" -eq 1 ]]; }

node_has_any_vip() { ! node_lacks_all_vips "${1:?}"; }

# Even spread: each node holds floor..ceil of |VIPS|/N over the given node set. Refreshes snapshot.
even_over_nodes() {
  snapshot_vips || return 1
  local nodes=("$@") count vip holder n total k floor ceil
  declare -A count=()
  for n in "${nodes[@]}"; do count["${n}"]=0; done
  for vip in "${VIPS[@]}"; do
    holder="$(holder_for_vip "${vip}")"
    case "${holder}" in none|duplicate:*) return 1 ;; esac
    [[ -n "${count[${holder}]+x}" ]] || return 1
    count["${holder}"]=$(( count["${holder}"] + 1 ))
  done
  total="${#VIPS[@]}"; k="${#nodes[@]}"
  floor=$(( total / k )); ceil=$(( (total + k - 1) / k ))
  for n in "${nodes[@]}"; do
    (( count["${n}"] >= floor && count["${n}"] <= ceil )) || return 1
  done
  return 0
}

# VIPs reachable via ARP/ping from the mgmt node (proves the data path moved).
all_vips_pingable() {
  local vip
  for vip in "${VIPS[@]}"; do mgmt_sh "ping -c1 -W2 ${vip} >/dev/null 2>&1" || return 1; done
}
vip_mac() { local vip="${1:?}"; mgmt_sh "ping -c1 -W2 ${vip} >/dev/null 2>&1; ip neigh show ${vip} 2>/dev/null | grep -oE 'lladdr [0-9a-f:]+' | awk '{print \$2}'"; }
neighbor_mac() { local vip="${1:?}"; mgmt_sh "ip neigh show ${vip} 2>/dev/null | grep -oE 'lladdr [0-9a-f:]+' | awk '{print \$2}'"; }

# ---------------------------------------------------------------------------
# Leader inspection.
# ---------------------------------------------------------------------------
# The daemon logs every leader transition from its metrics watcher. Use the newest transition on
# each node; OpenRaft's debug-formatted LeaderId values also contain node ids, but term zero appears
# in ordinary membership state and is not a current-leader signal.
leader_seen_by() {
  local ip="${1:?}" log transition
  log="$(node_sh "${ip}" "
    set -o pipefail
    unit=keepafloatd@$(instance_for_ip "${ip}")
    invocation=\$(systemctl show --property=InvocationID --value \"\$unit\")
    test -n \"\$invocation\"
    journalctl -u \"\$unit\" _SYSTEMD_INVOCATION_ID=\"\$invocation\" \
      --grep='raft current leader is now' -o cat \
      2>/dev/null | sed -E 's/\\x1b\\[[0-9;]*m//g'
  ")" || return 1
  transition="$(printf '%s\n' "${log}" | sed -nE \
    -e 's/.*raft current leader is now Some\(([0-9]+)\).*/\1/p' \
    -e 's/.*raft current leader is now None.*/none/p' | tail -1)"
  [[ "${transition}" == "none" || "${transition}" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "${transition}"
}

# Read only transitions emitted after a fault was injected. This prevents an old journal line from
# making a leader-election assertion pass when no post-fault election actually happened.
leader_seen_by_since() {
  local ip="${1:?}" since="${2:?}" log transition
  log="$(node_sh "${ip}" "
    set -o pipefail
    unit=keepafloatd@$(instance_for_ip "${ip}")
    invocation=\$(systemctl show --property=InvocationID --value \"\$unit\")
    test -n \"\$invocation\"
    journalctl -u \"\$unit\" _SYSTEMD_INVOCATION_ID=\"\$invocation\" --since '${since}' \
      -o cat 2>/dev/null | sed -E 's/\\x1b\\[[0-9;]*m//g'
  ")" || return 1
  transition="$(printf '%s\n' "${log}" | sed -nE \
    -e 's/.*raft current leader is now Some\(([0-9]+)\).*/\1/p' \
    -e 's/.*raft current leader is now None.*/none/p' | tail -1)"
  [[ "${transition}" == "none" || "${transition}" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "${transition}"
}

# Return the leader reported by a majority of nodes.
cluster_leader_id() {
  local ip pid pids=() td failed=0 value id majority best_id="" best_count=0
  declare -A counts=()
  td="$(mktemp -d)"
  for ip in "${NODE_IPS[@]}"; do
    (
      status="$(kafd_active "${ip}")" || exit 1
      if [[ "${status}" == "active" ]]; then
        leader_seen_by "${ip}"
      else
        printf 'inactive\n'
      fi
    ) > "${td}/${ip}" 2>/dev/null &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "${pid}" 2>/dev/null || failed=1; done
  if [[ "${failed}" -ne 0 ]]; then rm -rf "${td}"; return 1; fi
  for ip in "${NODE_IPS[@]}"; do
    value="$(<"${td}/${ip}")"
    [[ "${value}" == "inactive" || "${value}" == "none" || "${value}" =~ ^[0-9]+$ ]] \
      || { rm -rf "${td}"; return 1; }
    if [[ "${value}" != "inactive" && "${value}" != "none" ]]; then
      local configured=0
      for id in "${NODE_RAFT_IDS[@]}"; do [[ "${id}" == "${value}" ]] && configured=1; done
      [[ "${configured}" -eq 1 ]] || { rm -rf "${td}"; return 1; }
      counts["${value}"]=$(( ${counts["${value}"]:-0} + 1 ))
    fi
  done
  rm -rf "${td}"
  majority=$(( ${#NODE_IPS[@]} / 2 + 1 ))
  for id in "${!counts[@]}"; do
    if (( counts["${id}"] > best_count )); then
      best_id="${id}"; best_count="${counts[${id}]}"
    fi
  done
  (( best_count >= majority )) || return 1
  printf '%s\n' "${best_id}"
}

single_agreed_leader() {
  cluster_leader_id >/dev/null
}

# Require every currently running voter to report the same configured leader from its current
# systemd invocation. This prevents a restarted but isolated process from satisfying a recovery
# oracle with leader evidence left by its previous process.
all_nodes_agree_on_leader() {
  local ip status leader agreed="" id configured
  for ip in "${NODE_IPS[@]}"; do
    status="$(kafd_active "${ip}")" || return 1
    [[ "${status}" == "active" ]] || return 1
    leader="$(leader_seen_by "${ip}")" || return 1
    [[ "${leader}" =~ ^[0-9]+$ ]] || return 1
    configured=0
    for id in "${NODE_RAFT_IDS[@]}"; do
      [[ "${id}" == "${leader}" ]] && configured=1
    done
    [[ "${configured}" -eq 1 ]] || return 1
    [[ -z "${agreed}" || "${agreed}" == "${leader}" ]] || return 1
    agreed="${leader}"
  done
  [[ -n "${agreed}" ]]
}

# ---------------------------------------------------------------------------
# Wait / poll.
# ---------------------------------------------------------------------------
wait_until() {  # wait_until <timeout_s> <predicate...>
  local timeout="${1:?}"; shift
  local deadline=$(( $(date +%s) + timeout ))
  until "$@"; do
    (( $(date +%s) >= deadline )) && return 1
    sleep 2
  done
}

wait_for_all() {  # wait_for_all <pid>...
  local pid failed=0
  for pid in "$@"; do
    wait "${pid}" || failed=1
  done
  return "${failed}"
}

now_ms() { date +%s%3N; }

# Return the smallest whole-second stale window whose normalized missed-probe threshold differs
# from `stale_secs`. Config identity fingerprints the effective threshold, not raw YAML text.
next_behavior_changing_stale_secs() {  # <interval_ms> <stale_secs>
  local interval_ms="${1:?}" stale_secs="${2:?}" current candidate
  (( interval_ms > 0 )) || return 1
  current=$(( stale_secs * 1000 / interval_ms ))
  candidate=$(( stale_secs + 1 ))
  while (( candidate * 1000 / interval_ms == current )); do
    candidate=$(( candidate + 1 ))
  done
  printf '%s\n' "${candidate}"
}

# Return the next unequal whole-second stale window with the same normalized missed-probe
# threshold, or fail when the interval makes every whole second behaviorally distinct.
next_behaviorally_equivalent_stale_secs() {  # <interval_ms> <stale_secs>
  local interval_ms="${1:?}" stale_secs="${2:?}" current candidate
  (( interval_ms > 0 )) || return 1
  current=$(( stale_secs * 1000 / interval_ms ))
  candidate=$(( stale_secs + 1 ))
  (( candidate * 1000 / interval_ms == current )) || return 1
  printf '%s\n' "${candidate}"
}

# Assert that a predicate stays true for the whole observation window, not merely at its end.
holds_for() {  # holds_for <duration_s> <predicate...>
  local duration="${1:?}"; shift
  "$@" || return 1
  (( duration == 0 )) && return 0
  local deadline=$(( $(date +%s) + duration ))
  while true; do
    sleep 1
    "$@" || return 1
    (( $(date +%s) >= deadline )) && return 0
  done
}

even_and_pingable_over_nodes() {
  local nodes=("$@")
  # Reachability is a separate remote observation and can straddle a safe ownerless election gap.
  # Bracket it with fresh ownership snapshots so a single pre-gap sample cannot report convergence.
  even_over_nodes "${nodes[@]}" &&
    all_vips_pingable &&
    even_over_nodes "${nodes[@]}"
}

wait_for_even() {  # wait_for_even <timeout_s> [nodes...]   (default: all)
  local timeout="${1:?}"; shift
  local nodes=("$@"); [[ ${#nodes[@]} -eq 0 ]] && nodes=("${NODE_IPS[@]}")
  wait_until "${timeout}" holds_for 3 even_and_pingable_over_nodes "${nodes[@]}" || {
    dump_diag; fail "VIPs not evenly distributed over [${nodes[*]}] (current: $(current_assignments_summary))"; return 1; }
}

steady_cluster_ready() {
  all_daemons_active &&
    all_nodes_agree_on_leader &&
    even_and_pingable_over_nodes "${NODE_IPS[@]}"
}

# A recovered voter need not reclaim a VIP when failback is disabled. Availability therefore means
# all daemons agree on a leader and every VIP remains uniquely reachable, not that the spread is
# even. Bracket the reachability probe with fresh ownership snapshots just as the even-spread oracle
# does, so an ownerless transition cannot be accepted from a stale pre-probe sample.
available_cluster_ready() {
  all_daemons_active &&
    all_nodes_agree_on_leader &&
    all_vips_uniquely_held &&
    all_vips_pingable &&
    all_vips_uniquely_held
}

wait_for_available_cluster() {
  wait_until 60 holds_for 5 available_cluster_ready || {
    dump_diag
    fail "cluster lacks sustained daemon/leader agreement, unique ownership, or VIP reachability"
    return 1
  }
  log "cluster available after a sustained joint daemon/leader/ownership/reachability check"
}

wait_for_steady_state() {
  wait_until 60 holds_for 5 steady_cluster_ready || {
    dump_diag
    fail "cluster lacks sustained leader agreement, unique even ownership, or VIP reachability"
    return 1
  }
  log "steady state OK after a sustained joint leader/ownership/reachability check"
}

# ---------------------------------------------------------------------------
# Health toggle for real service-failure scenarios. Current croit configs probe HAProxy's local
# health endpoint, which remains healthy when only the backend RGW unit is stopped. Stop both the
# local front end and its RGW backend so the configured probe and S3 data path fail together.
# ---------------------------------------------------------------------------
RGW_MUTATED=0
set_unhealthy() {  # fail the configured probe and local service path
  local ip="${1:?}"
  RGW_MUTATED=1
  node_sh "${ip}" "
    set -uo pipefail
    rgw_dir=\$(find /var/lib/ceph/radosgw -mindepth 1 -maxdepth 1 -type d \
      -name 'ceph-rgw.*' | sort -V | tail -n1) || exit 1
    test -n \"\$rgw_dir\" || exit 1
    unit=ceph-radosgw@\${rgw_dir##*/ceph-}
    failed=0
    systemctl stop haproxy || failed=1
    systemctl stop \"\$unit\" || failed=1
    exit \"\$failed\"
  "
}
set_unhealthy_async() {  # <node> [timed], without waiting for service stop completion
  local ip="${1:?}" timing="${2-}" command
  [[ -z "${timing}" || "${timing}" == timed ]] || return 1
  RGW_MUTATED=1
  command="set -euo pipefail
    rgw_dir=\$(find /var/lib/ceph/radosgw -mindepth 1 -maxdepth 1 -type d \
      -name 'ceph-rgw.*' | sort -V | tail -n1)
    test -n \"\$rgw_dir\"
    unit=ceph-radosgw@\${rgw_dir##*/ceph-}
    systemctl stop --no-block haproxy \"\$unit\""
  if [[ "${timing}" == timed ]]; then
    HEALTH_FAILURE_CONTEXT=""
    HEALTH_FAILURE_CONTEXT="$(timed_node_command "${ip}" "${command}")" || return 1
  else
    node_sh "${ip}" "${command}"
  fi
}
set_healthy() {
  local ip="${1:?}"; node_sh "${ip}" "set -euo pipefail; pkill -CONT radosgw 2>/dev/null || true; rgw_dir=\$(find /var/lib/ceph/radosgw -mindepth 1 -maxdepth 1 -type d -name 'ceph-rgw.*' | sort -V | tail -n1); test -n \"\$rgw_dir\"; unit=ceph-radosgw@\${rgw_dir##*/ceph-}; systemctl start \"\$unit\"; systemctl start haproxy; systemctl is-active --quiet \"\$unit\"; systemctl is-active --quiet haproxy"
}

service_path_healthy() {
  local ip="${1:?}"
  node_sh "${ip}" "set -euo pipefail; rgw_dir=\$(find /var/lib/ceph/radosgw -mindepth 1 -maxdepth 1 -type d -name 'ceph-rgw.*' | sort -V | tail -n1); test -n \"\$rgw_dir\"; unit=ceph-radosgw@\${rgw_dir##*/ceph-}; systemctl is-active --quiet \"\$unit\"; systemctl is-active --quiet haproxy; curl -sf --max-time 3 http://127.0.0.1:9400/healthz >/dev/null; ss -H -lntp 'sport = :80' | grep -q radosgw; curl -sf --max-time 3 http://127.0.0.1:80/ >/dev/null"
}

all_service_paths_healthy() {
  local ip
  for ip in "${NODE_IPS[@]}"; do service_path_healthy "${ip}" || return 1; done
}

# Deterministic probe control for timing/startup scenarios. The generated RGW probe remains the
# baseline; scenarios back up the entire config before replacing it with this sentinel command.
backup_cluster_configs() {  # backup_cluster_configs <tag>
  local tag="${1:?}" ip inst cfg backup failed=0
  local -a captured=()
  [[ "${tag}" =~ ^[a-zA-Z0-9_-]+$ ]] || return 1
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")" || { failed=1; break; }
    cfg="/etc/keepafloatd/config-${inst}.yaml"
    backup="$(shell_quote "${cfg}.${tag}-bak")"
    # Publish only a complete copy, without replacing an existing operator backup.
    if node_sh "${ip}" "
      set -euo pipefail
      test ! -e ${backup}
      temporary=\$(mktemp $(shell_quote "${cfg}.${tag}-XXXXXX"))
      trap 'rm -f -- \"\$temporary\"' EXIT
      cp -p -- $(shell_quote "${cfg}") \"\$temporary\"
      ln -- \"\$temporary\" ${backup}
    "; then
      captured+=("${ip}")
    else
      printf 'config capture failed on %s; inspect %s before retrying\n' \
        "${ip}" "${cfg}.${tag}-bak" >&2
      failed=1
      break
    fi
  done
  (( failed )) || return 0
  # A failed capture has not changed any original config. Remove only confirmed captures.
  for ip in "${captured[@]}"; do
    inst="$(instance_for_ip "${ip}")" || return 1
    cfg="/etc/keepafloatd/config-${inst}.yaml"
    if ! node_sh "${ip}" "rm -f -- $(shell_quote "${cfg}.${tag}-bak")"; then
      printf 'could not remove captured backup on %s: %s\n' "${ip}" "${cfg}.${tag}-bak" >&2
    fi
  done
  return 1
}

campaign_config_backups_absent() {
  local ip inst cfg tag command
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")" || return 1
    cfg="/etc/keepafloatd/config-${inst}.yaml"
    command="set -euo pipefail"$'\n'
    for tag in "${REALCLUSTER_CAMPAIGN_BACKUP_TAGS[@]}"; do
      command+="test ! -e ${cfg}.${tag}-bak"$'\n'
    done
    node_sh "${ip}" "${command}" || return 1
  done
}

campaign_runtime_artifacts_absent() {
  local ip inst
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")" || return 1
    node_sh "${ip}" "
      set -euo pipefail
      test ! -e /usr/bin/keepafloatd.d15-current
      test ! -e /usr/bin/keepafloatd.d16-current
      test ! -e /run/kafd-submit-blocker.pid
      test ! -e /run/kafd-submit-blocker.log
      test ! -e /run/kafd-ipfault
      test ! -e /run/keepafloatd-unhealthy
      test ! -e /run/kafd-notify.log
      test ! -e /usr/local/bin/kafd-notify.sh
      test ! -e /tmp/kafd-notify.log
      test ! -e /run/systemd/system/keepafloatd@${inst}.service.d/zz-kafd-ipfault.conf
    " || return 1
  done
}

restore_cluster_configs() {  # restore_cluster_configs <tag>; does not restart
  local tag="${1:?}" ip inst cfg failed=0
  [[ "${tag}" =~ ^[a-zA-Z0-9_-]+$ ]] || return 1
  # Retry must reach pending nodes even after earlier backups have been consumed.
  for ip in "${NODE_IPS[@]}"; do
    if ! inst="$(instance_for_ip "${ip}")"; then
      printf 'config restore failed on %s: instance lookup failed\n' "${ip}" >&2
      failed=1
      continue
    fi
    cfg="/etc/keepafloatd/config-${inst}.yaml"
    if ! node_sh "${ip}" "test -f $(shell_quote "${cfg}.${tag}-bak") && mv -- $(shell_quote "${cfg}.${tag}-bak") $(shell_quote "${cfg}")"; then
      printf 'config restore failed on %s: inspect %s\n' "${ip}" "${cfg}.${tag}-bak" >&2
      failed=1
    fi
  done
  return "${failed}"
}

# Remove only the disposable lifecycle-test addresses and the exact marker tables derived from the
# configured ownership protocols. Absence is success; unreadable kernel state, duplicate evidence,
# and failed deletes are errors so cleanup cannot silently leak state into the next scenario.
cleanup_ownership_test_artifacts() {
  local ip inst cfg protocol
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"
    cfg="/etc/keepafloatd/config-${inst}.yaml"
    protocol="$(node_sh "${ip}" "sed -n 's/^address_protocol:[[:space:]]*//p' ${cfg}")" \
      || return 1
    protocol="${protocol:-246}"
    [[ "${protocol}" =~ ^[0-9]+$ ]] && (( protocol >= 1 && protocol <= 255 )) || return 1
    [[ "${OWNERSHIP_FOREIGN_PROTOCOL}" =~ ^[0-9]+$ ]] \
      && (( OWNERSHIP_FOREIGN_PROTOCOL >= 1 && OWNERSHIP_FOREIGN_PROTOCOL <= 255 )) || return 1

    node_sh "${ip}" "
      set -euo pipefail
      remove_address() {
        family=\"\$1\"; address=\"\$2\"; prefix=\"\$3\"; device=\"\$4\"
        if ! ip link show dev \"\$device\" >/dev/null 2>&1; then return 0; fi
        count=\$(ip -N -j \"\$family\" addr show dev \"\$device\" to \"\$address/\$prefix\" |
          jq -er --arg address \"\$address\" --argjson prefix \"\$prefix\" \
            '[.[].addr_info[]? | select(.local == \$address and .prefixlen == \$prefix)] | length')
        case \"\$count\" in
          0) ;;
          1) ip \"\$family\" addr del \"\$address/\$prefix\" dev \"\$device\" ;;
          *) exit 1 ;;
        esac
      }
      remove_marker() {
        family=\"\$1\"; address=\"\$2\"; prefix=\"\$3\"; marker_protocol=\"\$4\"
        marker_table=\$((10000 + marker_protocol))
        protocol_hex=\$(printf '0x%02x' \"\$marker_protocol\")
        count=\$(ip -N -j \"\$family\" route show table all |
          jq -er --arg address \"\$address\" --arg suffix \"/\$prefix\" \
            --arg table \"\$marker_table\" --arg protocol \"\$marker_protocol\" \
            --arg protocol_hex \"\$protocol_hex\" \
            '[.[] | select(
              ((.type | tostring) == \"9\" or .type == \"throw\") and
              (.table | tostring) == \$table and
              (.dst == \$address or .dst == (\$address + \$suffix)) and
              ((.protocol | tostring) == \$protocol or
               (.protocol | tostring) == \$protocol_hex)
            )] | length')
        case \"\$count\" in
          0) ;;
          1) ip \"\$family\" route del table \"\$marker_table\" \
               throw \"\$address/\$prefix\" proto \"\$marker_protocol\" ;;
          *) exit 1 ;;
        esac
      }
      remove_address -4 ${MUTATION_TEST_VIP} 32 ${IFACE}
      remove_address -4 ${OWNERSHIP_ADMIN_VIP} 32 ${IFACE}
      remove_address -4 ${OWNERSHIP_FOREIGN_VIP} 32 ${IFACE}
      remove_address -4 ${CIDR_TEST_VIP} ${CIDR_TEST_PREFIX} ${IFACE}
      remove_address -4 ${VLAN_TEST_VIP} 32 ${IFACE}.${VLAN_TEST_ID}
      remove_address -6 ${IPV6_TEST_VIP} 128 ${IFACE}
      remove_marker -4 ${MUTATION_TEST_VIP} 32 ${protocol}
      remove_marker -4 ${OWNERSHIP_FOREIGN_VIP} 32 ${OWNERSHIP_FOREIGN_PROTOCOL}
      remove_marker -4 ${CIDR_TEST_VIP} ${CIDR_TEST_PREFIX} ${protocol}
      remove_marker -4 ${VLAN_TEST_VIP} 32 ${protocol}
      remove_marker -6 ${IPV6_TEST_VIP} 128 ${protocol}
      if ip link show dev ${IFACE}.${VLAN_TEST_ID} >/dev/null 2>&1; then
        ip link del ${IFACE}.${VLAN_TEST_ID}
      fi
      ! ip link show dev ${IFACE}.${VLAN_TEST_ID} >/dev/null 2>&1
    " || return 1
  done
}

ownership_markers_absent() {
  local ip
  for ip in "${NODE_IPS[@]}"; do
    node_sh "${ip}" "
      set -euo pipefail
      ip -N -j -4 route show table all | jq -e \
        --arg a '${MUTATION_TEST_VIP}' --arg b '${OWNERSHIP_FOREIGN_VIP}' \
        --arg c '${CIDR_TEST_VIP}' --arg c_suffix '/${CIDR_TEST_PREFIX}' \
        --arg d '${VLAN_TEST_VIP}' \
        '[.[] | select(
          ((.type | tostring) == \"9\" or .type == \"throw\") and
          ((.table | tostring | tonumber) >= 10001 and
           (.table | tostring | tonumber) <= 10255) and
          (.dst == \$a or .dst == (\$a + \"/32\") or
           .dst == \$b or .dst == (\$b + \"/32\") or
           .dst == \$c or .dst == (\$c + \$c_suffix) or
           .dst == \$d or .dst == (\$d + \"/32\"))
        )] | length == 0' >/dev/null
      ip -N -j -6 route show table all | jq -e \
        --arg a '${IPV6_TEST_VIP}' \
        '[.[] | select(
          ((.type | tostring) == \"9\" or .type == \"throw\") and
          ((.table | tostring | tonumber) >= 10001 and
           (.table | tostring | tonumber) <= 10255) and
          (.dst == \$a or .dst == (\$a + \"/128\"))
        )] | length == 0' >/dev/null
    " || return 1
  done
}

ownership_test_artifacts_absent() {
  ownership_markers_absent || return 1
  local ip
  for ip in "${NODE_IPS[@]}"; do
    node_sh "${ip}" "
      set -euo pipefail
      ipv4_count=\$(ip -N -j -4 addr show | jq -er \
        --arg a '${MUTATION_TEST_VIP}' --arg b '${OWNERSHIP_ADMIN_VIP}' \
        --arg c '${OWNERSHIP_FOREIGN_VIP}' --arg d '${CIDR_TEST_VIP}' \
        --arg e '${VLAN_TEST_VIP}' \
        '[.[].addr_info[]? | select(
          .local == \$a or .local == \$b or .local == \$c or .local == \$d or .local == \$e
        )] | length')
      ipv6_count=\$(ip -N -j -6 addr show | jq -er --arg a '${IPV6_TEST_VIP}' \
        '[.[].addr_info[]? | select(.local == \$a)] | length')
      test \"\$ipv4_count\" -eq 0
      test \"\$ipv6_count\" -eq 0
      ! ip link show dev ${IFACE}.${VLAN_TEST_ID} >/dev/null 2>&1
    " || return 1
  done
}

configure_sentinel_health() {  # configure_sentinel_health <interval_ms> <timeout_ms> <stale_secs>
  local interval="${1:?}" probe_timeout="${2:?}" stale="${3:?}" ip inst cfg
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"; cfg="/etc/keepafloatd/config-${inst}.yaml"
    node_sh "${ip}" "sed -i 's#^  command:.*#  command:#; /^  command:/,/^  interval_ms:/ {/^[[:space:]][[:space:]][[:space:]][[:space:]]- /d;}' ${cfg}; sed -i '/^  command:/a\\    - \"/bin/bash\"\\n    - \"-c\"\\n    - \"test ! -e /run/keepafloatd-unhealthy\"' ${cfg}; sed -i 's/^  interval_ms: .*/  interval_ms: ${interval}/; s/^  timeout_ms: .*/  timeout_ms: ${probe_timeout}/; s/^  stale_secs: .*/  stale_secs: ${stale}/' ${cfg}" || return 1
  done
}

set_cluster_scalar() {  # set_cluster_scalar <top-level-yaml-key> <yaml-value>
  local key="${1:?}" value="${2:?}" ip inst cfg
  [[ "${key}" =~ ^[a-z_]+$ ]] || return 1
  for ip in "${NODE_IPS[@]}"; do
    inst="$(instance_for_ip "${ip}")"; cfg="/etc/keepafloatd/config-${inst}.yaml"
    node_sh "${ip}" "sed -i '/^${key}:/d' ${cfg}; printf '\\n${key}: ${value}\\n' >> ${cfg}" || return 1
  done
}

sentinel_fail() { node_sh "${1:?}" "touch /run/keepafloatd-unhealthy"; }
sentinel_recover() { node_sh "${1:?}" "rm -f /run/keepafloatd-unhealthy"; }

timed_node_command() {
  local ip="${1:?}" command="${2:?}" inst context boot invocation started extra
  inst="$(instance_for_ip "${ip}")" || return 1
  context="$(node_sh "${ip}" "
    set -euo pipefail
    invocation=\$(systemctl show --property=InvocationID --value $(shell_quote "keepafloatd@${inst}"))
    [[ \"\$invocation\" =~ ^[a-f0-9]{32}$ ]]
    context=\$(python3 -c 'import pathlib, sys, time, uuid
boot = uuid.UUID(pathlib.Path(\"/proc/sys/kernel/random/boot_id\").read_text().strip()).hex
started = time.monotonic_ns() // 1000
print(boot, sys.argv[1], started)' \"\$invocation\")
    bash -c $(shell_quote "${command}") >&2
    printf '%s\\n' \"\$context\"
  ")" || return 1
  read -r boot invocation started extra <<< "${context}" || return 1
  [[ "${boot}" =~ ^[a-f0-9]{32}$ && "${invocation}" =~ ^[a-f0-9]{32}$ &&
    "${started}" =~ ^[0-9]+$ && -z "${extra}" &&
    "${context}" == "${boot} ${invocation} ${started}" ]] || return 1
  printf '%s\n' "${context}"
}

timed_sentinel_fail() {
  timed_node_command "${1:?}" 'touch /run/keepafloatd-unhealthy'
}

timed_sentinel_recover() {
  timed_node_command "${1:?}" 'rm -f /run/keepafloatd-unhealthy'
}

vip_event_elapsed_ms() {  # <node> <timing-context> <bound|unbound> <vip>...
  local ip="${1:?}" context="${2-}" action="${3:?}" boot invocation started extra inst records
  shift 3
  [[ "${action}" == bound || "${action}" == unbound ]] && (( $# > 0 )) || return 1
  read -r boot invocation started extra <<< "${context}" || return 1
  [[ "${boot}" =~ ^[a-f0-9]{32}$ && "${invocation}" =~ ^[a-f0-9]{32}$ &&
    "${started}" =~ ^[0-9]+$ && -z "${extra}" &&
    "${context}" == "${boot} ${invocation} ${started}" ]] || return 1
  inst="$(instance_for_ip "${ip}")" || return 1
  records="$(node_sh "${ip}" "journalctl --no-pager -o short-monotonic \
    -u $(shell_quote "keepafloatd@${inst}") \
    _BOOT_ID=${boot} _SYSTEMD_INVOCATION_ID=${invocation}")" || return 1
  # Compare node-local event times so slow SSH or polling cannot hide an early transition.
  printf '%s\n' "${records}" | awk -v started="${started}" \
    -v action="${action}" -v vips="$*" -v iface="${IFACE}" '
    BEGIN { count = split(vips, requested, " ") }
    {
      gsub(/\033\[[0-9;]*m/, "")
      matched = 0
      for (i = 1; i <= count; i++) {
        message = action " " requested[i] "/32 on " iface
        pos = index($0, message)
        if (pos && (pos == 1 || substr($0, pos - 1, 1) ~ /[[:space:]]/) &&
            substr($0, pos) == message) matched = 1
      }
      if (!matched) next
      if ($0 !~ /^\[[[:space:]]*[0-9]+\.[0-9]+\]/) { invalid = 1; next }
      stamp = $0
      sub(/^\[[[:space:]]*/, "", stamp)
      sub(/\].*$/, "", stamp)
      split(stamp, parts, ".")
      if (length(parts[2]) != 6) { invalid = 1; next }
      event = parts[1] * 1000000 + parts[2]
      if (event >= started && (!found || event < first)) { first = event; found = 1 }
    }
    END {
      if (invalid || !found) exit 1
      printf "%.0f\n", int((first - started) / 1000)
    }'
}

vip_release_elapsed_ms() {
  vip_event_elapsed_ms "${1:?}" "${3-}" unbound "${2:?}"
}

vip_bind_elapsed_ms() {
  vip_event_elapsed_ms "${1:?}" "${2-}" bound "${VIPS[@]}"
}
clear_health_sentinels() {
  local ip
  for ip in "${NODE_IPS[@]}"; do sentinel_recover "${ip}" || return 1; done
}

post_rgw_stable() {
  all_daemons_active &&
    all_nodes_agree_on_leader &&
    all_vips_uniquely_held &&
    all_service_paths_healthy
}

# croit may asynchronously redeploy the HA group after an RGW stop/start. The delayed job can
# rewrite the generated config or explicitly stop all keepafloatd units roughly 100s later. Own
# that external tail inside the scenario which mutated RGW, then require a fresh quiet window.
settle_after_rgw_recovery() {
  local reform=0
  holds_for 130 post_rgw_stable || reform=1
  prepare_cluster_secret || return 1
  [[ "${REALCLUSTER_SECRET_CHANGED}" -eq 0 ]] || reform=1
  if [[ "${reform}" -eq 1 ]]; then
    log "post-RGW controller action observed; reforming after config repair"
    clean_reform || return 1
    wait_for_steady_state || return 1
  fi
  holds_for 30 post_rgw_stable
}

# ---------------------------------------------------------------------------
# Network partition (iptables) - drop raft+submit traffic between a node and its peers.
# ---------------------------------------------------------------------------
partition_input_from() {  # partition_input_from <node> <source peer>
  local ip="${1:?}" peer="${2:?}"
  node_sh "${ip}" "
    set -e
    iptables -A INPUT -s ${peer} -p tcp -m multiport --dports ${PARTITION_PORTS} \
      -m comment --comment keepafloatd-realcluster -j DROP
  "
}

partition_node() {  # isolate <ip> from the other two nodes (raft 9210 / submit 9211)
  local ip="${1:?}" peer
  for peer in "${NODE_IPS[@]}"; do
    [[ "${peer}" == "${ip}" ]] && continue
    node_sh "${ip}" "
      set -e
      iptables -A INPUT -s ${peer} -p tcp -m multiport --dports ${PARTITION_PORTS} \
        -m comment --comment keepafloatd-realcluster -j DROP
      iptables -A OUTPUT -d ${peer} -p tcp -m multiport --dports ${PARTITION_PORTS} \
        -m comment --comment keepafloatd-realcluster -j DROP
    " || return 1
  done
}
heal_node() {
  local ip="${1:?}" peer
  for peer in "${NODE_IPS[@]}"; do
    [[ "${peer}" == "${ip}" ]] && continue
    node_sh "${ip}" "
      set -euo pipefail
      while true; do
        if iptables -C INPUT -s ${peer} -p tcp -m multiport --dports ${PARTITION_PORTS} \
          -m comment --comment keepafloatd-realcluster -j DROP 2>/dev/null
        then
          iptables -D INPUT -s ${peer} -p tcp -m multiport --dports ${PARTITION_PORTS} \
            -m comment --comment keepafloatd-realcluster -j DROP
        else
          status=\$?
          test \"\$status\" -eq 1
          break
        fi
      done
      while true; do
        if iptables -C OUTPUT -d ${peer} -p tcp -m multiport --dports ${PARTITION_PORTS} \
          -m comment --comment keepafloatd-realcluster -j DROP 2>/dev/null
        then
          iptables -D OUTPUT -d ${peer} -p tcp -m multiport --dports ${PARTITION_PORTS} \
            -m comment --comment keepafloatd-realcluster -j DROP
        else
          status=\$?
          test \"\$status\" -eq 1
          break
        fi
      done
    " || return 1
  done
  partition_rules_absent_on_node "${ip}"
}
heal_all() { local ip; for ip in "${NODE_IPS[@]}"; do heal_node "${ip}" || return 1; done; }

partition_rules_absent_on_node() {
  local ip="${1:?}"
  node_sh "${ip}" "
    set -euo pipefail
    input_rules=\$(iptables -S INPUT)
    output_rules=\$(iptables -S OUTPUT)
    [[ \"\$input_rules\" != *keepafloatd-realcluster* ]]
    [[ \"\$output_rules\" != *keepafloatd-realcluster* ]]
  "
}

partition_rules_absent() {
  local ip
  for ip in "${NODE_IPS[@]}"; do
    partition_rules_absent_on_node "${ip}" || return 1
  done
}

# Print the node IPs except the given one (for survivor-set assertions).
nodes_except() {
  local exclude="${1:?}" ip
  for ip in "${NODE_IPS[@]}"; do [[ "${ip}" == "${exclude}" ]] || printf '%s\n' "${ip}"; done
}

# Predicate: every VIP has exactly one LIVE holder (a node other than <dead>). After a SIGKILL the
# dead node may retain an orphan in its kernel, but duplicates among survivors are always unsafe.
# Refreshes the snapshot.
vips_uniquely_served_by_live() {
  local dead="${1:?}" vip ip holders
  snapshot_vips || return 1
  for vip in "${VIPS[@]}"; do
    holders=0
    for ip in "${NODE_IPS[@]}"; do
      [[ "${ip}" == "${dead}" ]] && continue
      if [[ "${_SNAP_VIPS_ON[${ip}]:-}" == *" ${vip} "* ]]; then
        holders=$((holders + 1))
      fi
    done
    [[ "${holders}" -eq 1 ]] || return 1
  done
  return 0
}

# Nopreempt/failback-disabled operation preserves safe incumbents after a fault; it does not promise
# an even redistribution. Require the excluded node to stay empty and bracket reachability with
# fresh cluster-wide uniqueness snapshots so neither an orphan nor a transient gap can pass.
live_service_without() {
  local excluded="${1:?}"
  node_lacks_all_vips "${excluded}" &&
    all_vips_uniquely_held &&
    all_vips_pingable &&
    node_lacks_all_vips "${excluded}" &&
    all_vips_uniquely_held
}

wait_for_live_service_without() {
  local timeout="${1:?}" excluded="${2:?}"
  wait_until "${timeout}" holds_for 5 live_service_without "${excluded}" || {
    dump_diag
    fail "VIP service is not uniquely reachable outside ${excluded}"
    return 1
  }
}

# ---------------------------------------------------------------------------
# S3 through a VIP (real RGW data path). Uses s3cmd on the mgmt node, path-style, no-ssl.
# ---------------------------------------------------------------------------
ensure_s3_credentials() {
  [[ -n "${S3_ACCESS}" && -n "${S3_SECRET}" ]] && return 0
  local credentials
  credentials="$(node_sh "${NODE_IPS[0]}" "set -o pipefail; rgw_dir=\$(find /var/lib/ceph/radosgw -mindepth 1 -maxdepth 1 -type d -name 'ceph-rgw.*' | sort -V | tail -n1); test -n \"\$rgw_dir\"; client=client.\${rgw_dir##*/ceph-}; admin=(radosgw-admin --name \"\$client\" --keyring \"\$rgw_dir/keyring\"); if ! \"\${admin[@]}\" user info --uid kafdtest >/dev/null 2>&1; then \"\${admin[@]}\" user create --uid kafdtest --display-name 'keepafloatd real-cluster test' >/dev/null || exit; fi; \"\${admin[@]}\" user info --uid kafdtest | python3 -c 'import json,sys; k=json.load(sys.stdin)[\"keys\"][0]; print(k[\"access_key\"]+\"\\t\"+k[\"secret_key\"])'")" || return 1
  IFS=$'\t' read -r S3_ACCESS S3_SECRET <<< "${credentials}"
  [[ -n "${S3_ACCESS}" && -n "${S3_SECRET}" ]]
}

shell_quote() { printf '%q' "${1-}"; }

_s3() {  # _s3 <vip> <s3cmd-args...>
  local vip="${1:?}"; shift
  ensure_s3_credentials || return 1
  local access secret
  access="$(shell_quote "${S3_ACCESS}")"; secret="$(shell_quote "${S3_SECRET}")"
  mgmt_sh "s3cmd --access_key=${access} --secret_key=${secret} --host=${vip} --host-bucket=${vip} --no-ssl $*"
}
s3_ensure_bucket() {
  local vip="${1:?}"
  _s3 "${vip}" "info s3://${S3_BUCKET}" >/dev/null 2>&1 && return 0
  _s3 "${vip}" "mb s3://${S3_BUCKET}" >/dev/null 2>&1
}
s3_put() {  # s3_put <vip> <content>
  local vip="${1:?}" content="${2:?}"
  ensure_s3_credentials || return 1
  local access secret body
  access="$(shell_quote "${S3_ACCESS}")"; secret="$(shell_quote "${S3_SECRET}")"; body="$(shell_quote "${content}")"
  mgmt_sh "printf '%s' ${body} > /tmp/kafd-s3-put.txt && s3cmd --access_key=${access} --secret_key=${secret} --host=${vip} --host-bucket=${vip} --no-ssl put /tmp/kafd-s3-put.txt s3://${S3_BUCKET}/obj >/dev/null 2>&1"
}
s3_get() {  # s3_get <vip> -> prints object content
  local vip="${1:?}"
  _s3 "${vip}" "get s3://${S3_BUCKET}/obj -" 2>/dev/null
}
s3_get_ok() {  # predicate: object readable through <vip> and matches <expected>
  local vip="${1:?}" expected="${2:?}"
  [[ "$(s3_get "${vip}")" == "${expected}" ]]
}

# ---------------------------------------------------------------------------
# Diagnostics + restore-to-baseline.
# ---------------------------------------------------------------------------
dump_diag() {
  log "--- DIAGNOSTICS ---"
  log "assignments: $(current_assignments_summary)"
  local ip
  for ip in "${NODE_IPS[@]}"; do
    log "node ${ip} ($(instance_for_ip "${ip}")): active=$(kafd_active "${ip}" 2>/dev/null)"
    kafd_log "${ip}" 6 | sed "s/^/    ${ip}| /"
  done
}

# A clean full reform: stop every instance, then start them all near-simultaneously, so any
# incarnation split left by partial reforms (e.g. partition/secret scenarios) collapses to one fresh
# incarnation. Used by restore_baseline when a gentle restore doesn't reach steady state.
clean_reform() {
  local ip
  # Repair cluster-wide config first, stop the entire old incarnation, then start deterministically.
  # Concurrent two-hop SSH starts are unreliable on this jump-host topology; a first node can wait
  # uninitialized while the second establishes the majority, so sequential starts are safe and do
  # not create a partial old/new incarnation.
  prepare_cluster_secret || return 1
  for ip in "${NODE_IPS[@]}"; do kafd_stop "${ip}" || return 1; done
  sleep 2
  for ip in "${NODE_IPS[@]}"; do kafd_start "${ip}" || return 1; done
  wait_until 30 all_daemons_active
}

restore_baseline() {
  log "restoring baseline..."
  heal_all || return 1
  prepare_cluster_secret || return 1
  local ip
  for ip in "${NODE_IPS[@]}"; do set_healthy "${ip}" || return 1; done
  for ip in "${NODE_IPS[@]}"; do
    kafd_active "${ip}" 2>/dev/null | grep -q '^active' || kafd_start "${ip}" || return 1
  done
  # Try a gentle restore first; if the cluster is wedged (e.g. an incarnation split from a
  # partition/secret scenario), force a clean full reform and try once more.
  if wait_for_steady_state; then
    log "baseline restored (gentle)"
    return 0
  fi
  local attempt
  for attempt in 1 2; do
    log "gentle restore failed; forcing clean full reform ${attempt}/2"
    clean_reform || return 1
    if wait_for_steady_state; then
      log "baseline restored after clean reform ${attempt}/2"
      return 0
    fi
  done
  return 1
}
