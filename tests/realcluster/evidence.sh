#!/usr/bin/env bash
REALCLUSTER_CASCADING_ROUNDS=6

evidence_failure() {  # <predicate> <node> <observed status> [safe fields...]
  local predicate="$1" ip="$2" status="$3" now
  shift 3
  now="$(date +%s)" || now=unavailable
  printf 'evidence failure: predicate=%s node=%s status=%s controller_s=%s' "$predicate" "$ip" "$status" "$now"
  (( $# == 0 )) || printf ' %s' "$@"
  printf '\n'
} >&2

evidence_same_invocation() {  # <predicate> <node> <expected context> <observed context> [safe fields...]
  local predicate="$1" ip="$2" expected="$3" observed="$4"
  shift 4
  [[ "${expected% *}" != "${observed% *}" ]] || return 0
  [[ "$expected" =~ ^[a-f0-9]{32}\ [a-f0-9]{32}\ [0-9]+$ ]] || expected=invalid
  [[ "$observed" =~ ^[a-f0-9]{32}\ [a-f0-9]{32}\ [0-9]+$ ]] || observed=invalid
  evidence_failure "$predicate" "$ip" 1 "expected_context=$expected" "observed_context=$observed" "$@"
  return 1
}

activation_service_failure() {
  local predicate="$1" ip="$2" status="$3" state followup_status=0
  evidence_failure "$predicate" "$ip" "$status"
  # This later read describes recovery state, not the cause of the failed predicate.
  state="$(kafd_active "$ip")" || followup_status=$?
  case "$state" in
    active|inactive|failed|activating|deactivating|reloading|maintenance|refreshing|unknown) ;;
    *) state=unrecognized;;
  esac
  printf 'activation service followup: node=%s status=%s followup_service_state=%s\n' \
    "$ip" "$followup_status" "$state" >&2
}

# Journal evidence is scoped to the service that experienced the fault.
journal_evidence() {  # <mode> <node> <boot invocation start-us> [targets...]
  local mode="${1:?}" ip="${2:?}" context="${3-}" boot invocation started extra inst records status
  shift 3
  read -r boot invocation started extra <<< "${context}" || {
    evidence_failure journal.context "$ip" "$?" "mode=$mode"; return 1;
  }
  [[ "${boot}" =~ ^[a-f0-9]{32}$ && "${invocation}" =~ ^[a-f0-9]{32}$ &&
    "${started}" =~ ^[0-9]+$ && -z "${extra}" ]] || {
    evidence_failure journal.context "$ip" 1 "mode=$mode"; return 1;
  }
  inst="$(instance_for_ip "${ip}")" || {
    evidence_failure journal.instance "$ip" "$?" "mode=$mode"; return 1;
  }
  records="$(node_sh "${ip}" "journalctl -q --no-pager --all -o json \
    --output-fields=MESSAGE,_BOOT_ID,_SYSTEMD_INVOCATION_ID,__MONOTONIC_TIMESTAMP \
    -u $(shell_quote "keepafloatd@${inst}") _BOOT_ID=${boot} \
    _SYSTEMD_INVOCATION_ID=${invocation}")" || {
    evidence_failure journal.read "$ip" "$?" "mode=$mode" "boot=$boot" "invocation=$invocation"; return 1;
  }
  if python3 "${HERE}/journal-evidence.py" "${mode}" "${boot}" "${invocation}" \
    "${started}" "$@" <<< "${records}"; then return 0; else status=$?; fi
  if [[ "$status" != 2 ]]; then
    evidence_failure journal.parse "$ip" "$status" "mode=$mode" \
      "boot=$boot" "invocation=$invocation" "after_us=$started"
  fi
  return "$status"
}

restarted_service_context() {
  local ip="${1:?}" original="${2:?}" current old_boot old_inv old_time boot inv now
  [[ "${original}" =~ ^[a-f0-9]{32}\ [a-f0-9]{32}\ [0-9]+$ ]] || return 1
  read -r old_boot old_inv old_time <<< "${original}"
  current="$(timed_node_command "${ip}" :)" || return 1
  read -r boot inv now <<< "${current}"
  [[ "${boot}" == "${old_boot}" && "${inv}" != "${old_inv}" &&
    "${now}" -ge "${old_time}" ]] || return 1
  printf '%s %s %s\n' "${boot}" "${inv}" "${old_time}"
}

startup_orphan_reclaimed() {
  local context
  context="$(restarted_service_context "${1:?}" "${2:?}")" || return 1
  journal_evidence startup "$1" "${context}" "${IFACE}" "${3:?}"
}

incarnation_reset_observed() {
  restarted_service_context "${1:?}" "${2:?}" >/dev/null || return 1
  journal_evidence incarnation "$1" "$2"
}

node_admitted_replica() {  # <node> <pinned context>
  local ip="${1:?}" context="${2:?}" current physical="" index replica
  for index in "${!NODE_IPS[@]}"; do
    [[ "${NODE_IPS[$index]}" != "$ip" ]] || physical="${NODE_RAFT_IDS[$index]}"
  done
  [[ -n "$physical" ]] && node_active "$ip" || return 1
  current="$(timed_node_command "$ip" :)" || return 1
  [[ "${current% *}" == "${context% *}" ]] || return 1
  replica="$(journal_evidence admitted "$ip" "${context% *} 0" "$physical")" || return 1
  current="$(timed_node_command "$ip" :)" || return 1
  [[ "${current% *}" == "${context% *}" ]] && node_active "$ip" || return 1
  printf '%s\n' "$replica"
}

permission_expiry_observed() {  # <node> <original context> <original VIP>...
  local ip="${1:?}" context="${2:?}"
  shift 2
  (( $# > 0 )) || return 1
  restarted_service_context "$ip" "$context" >/dev/null || return 1
  journal_evidence permission-expiry "$ip" "$context" "$IFACE" "$@" || return 1
  node_lacks_all_vips "$ip"
}

# Unlike a global survey, an isolated fresh process need not have any leader log yet.
surviving_peers_agree_on_leader() {  # <surviving node>...
  local ip context current leader agreed="" index physical="" leader_in_subset=0
  local -A seen=()
  (( $# > ${#NODE_IPS[@]} / 2 )) || return 1
  for ip in "$@"; do
    [[ -z "${seen[$ip]:-}" ]] || return 1
    seen["$ip"]=1
    physical=""
    for index in "${!NODE_IPS[@]}"; do
      [[ "${NODE_IPS[$index]}" != "$ip" ]] || physical="${NODE_RAFT_IDS[$index]}"
    done
    [[ -n "$physical" ]] && node_active "$ip" || return 1
    context="$(timed_node_command "$ip" :)" || return 1
    leader="$(leader_seen_by "$ip")" || return 1
    replica_is_configured "$leader" || return 1
    [[ "$(replica_physical_id "$leader")" != "$physical" ]] || leader_in_subset=1
    current="$(timed_node_command "$ip" :)" || return 1
    [[ "${current% *}" == "${context% *}" ]] && node_active "$ip" || return 1
    [[ -z "$agreed" || "$agreed" == "$leader" ]] || return 1
    agreed="$leader"
  done
  (( leader_in_subset )) || return 1
  printf '%s\n' "$agreed"
}

fresh_join_observed() {  # <node> <new context> <old replica> <peer context>...
  local ip="${1:?}" context="${2:?}" old="${3:?}" replica peer pinned current status found=0
  shift 3
  (( $# >= 4 && $# % 2 == 0 )) || return 1
  replica="$(node_admitted_replica "$ip" "$context")" || return 1
  [[ "$replica" != "$old" && "${replica%%:*}" == "${old%%:*}" ]] || return 1
  while (( $# )); do
    peer="$1"; pinned="$2"; shift 2
    current="$(timed_node_command "$peer" :)" || return 1
    [[ "${current% *}" == "${pinned% *}" ]] && node_active "$peer" || return 1
    if journal_evidence promotion "$peer" "$pinned" "$replica" >/dev/null; then
      found=1
    else
      status=$?
      [[ "$status" == 2 ]] || return 1
    fi
    current="$(timed_node_command "$peer" :)" || return 1
    [[ "${current% *}" == "${pinned% *}" ]] && node_active "$peer" || return 1
  done
  current="$(timed_node_command "$ip" :)" || return 1
  [[ "${current% *}" == "${context% *}" ]] && node_active "$ip" && (( found ))
}

config_fence_observed() {
  local ip="${1:?}" context="${2:?}"
  shift 2
  restarted_service_context "${ip}" "${context}" >/dev/null || return 1
  journal_evidence config "${ip}" "${context}" "${IFACE}" "$@"
}

node_snapshot_cycles() {
  local ip="${1:?}" original="${2:?}" current count
  current="$(timed_node_command "${ip}" :)" || return 1
  [[ "${current% *}" == "${original% *}" ]] || return 1
  count="$(journal_evidence snapshots "${ip}" "${original}")" || return 1
  current="$(timed_node_command "${ip}" :)" || return 1
  [[ "${current% *}" == "${original% *}" ]] || return 1
  printf '%s\n' "${count}"
}

# Read the whole current invocation, never timing left by a previous daemon.
node_startup_budget() {  # <node> <convergence seconds> [pinned context]
  local ip="${1:?}" base="${2:?}" context="${3-}" current milliseconds status
  [[ "$base" =~ ^[0-9]{1,6}$ ]] || {
    evidence_failure startup-budget.base "$ip" 1; return 1;
  }
  [[ -n "$context" ]] || context="$(timed_node_command "$ip" :)" || {
    evidence_failure startup-budget.context-before "$ip" "$?"; return 1;
  }
  if milliseconds="$(journal_evidence startup-budget "$ip" "${context% *} 0")"; then :; else
    status=$?
    [[ "$status" == 2 ]] || evidence_failure startup-budget.journal "$ip" "$status"
    return "$status"
  fi
  [[ "$milliseconds" =~ ^[1-9][0-9]{0,11}$ ]] || {
    evidence_failure startup-budget.value "$ip" 1; return 1;
  }
  current="$(timed_node_command "$ip" :)" || {
    evidence_failure startup-budget.context-after "$ip" "$?"; return 1;
  }
  evidence_same_invocation startup-budget.invocation "$ip" "$context" "$current" || return 1
  printf '%s\n' "$((10#$base + (milliseconds + 999) / 1000))"
}

scenario_timeout_seconds() {
  local scenario="${1:?}" base=1800 ip wait_seconds maximum=0 status
  if [[ -v REALCLUSTER_SCENARIO_TIMEOUT ]]; then
    [[ "$REALCLUSTER_SCENARIO_TIMEOUT" =~ ^[1-9][0-9]{0,5}$ ]] || {
      evidence_failure scenario-budget.override cluster 1; return 1;
    }
    printf '%s\n' "$REALCLUSTER_SCENARIO_TIMEOUT"
    return 0
  fi
  if [[ "$scenario" != C4_cascading_failures ]]; then
    printf '%s\n' "$base"
    return 0
  fi
  (( ${#NODE_IPS[@]} > 0 )) || {
    evidence_failure scenario-budget.nodes cluster 1; return 1;
  }
  for ip in "${NODE_IPS[@]}"; do
    if wait_seconds="$(node_startup_budget "$ip" 0)"; then :; else
      status=$?
      evidence_failure scenario-budget.policy "$ip" "$status"; return 1;
    fi
    [[ "$wait_seconds" =~ ^[1-9][0-9]{0,9}$ ]] || {
      evidence_failure scenario-budget.value "$ip" 1; return 1;
    }
    (( wait_seconds <= maximum )) || maximum="$wait_seconds"
  done
  # The six sequential restart fences consume time before convergence can be tested.
  printf '%s\n' "$((base + REALCLUSTER_CASCADING_ROUNDS * maximum))"
}

node_activation_ready() {  # <node> <pinned context>
  local ip="${1:?}" context="${2:?}" current after physical="" index replica status
  for index in "${!NODE_IPS[@]}"; do
    [[ "${NODE_IPS[$index]}" != "$ip" ]] || physical="${NODE_RAFT_IDS[$index]}"
  done
  [[ -n "$physical" ]] || { evidence_failure activation.physical "$ip" 1; return 1; }
  node_active "$ip" || { activation_service_failure activation.active-before "$ip" "$?"; return 1; }
  current="$(timed_node_command "$ip" :)" || {
    evidence_failure activation.context-before "$ip" "$?"; return 1;
  }
  evidence_same_invocation activation.invocation-before "$ip" "$context" "$current" || return 1
  if replica="$(journal_evidence activation "$ip" "${context% *} 0" "${current##* }" "$physical")"; then :; else
    status=$?
    [[ "$status" == 2 ]] || evidence_failure activation.journal "$ip" "$status" "now_us=${current##* }"
    return "$status"
  fi
  replica_is_configured "$replica" || { evidence_failure activation.replica "$ip" "$?"; return 1; }
  after="$(timed_node_command "$ip" :)" || {
    evidence_failure activation.context-after "$ip" "$?"; return 1;
  }
  evidence_same_invocation activation.invocation-after "$ip" "$context" "$after" || return 1
  node_active "$ip" || {
    status=$?
    activation_service_failure activation.active-after "$ip" "$status"
    return "$status"
  }
}

# Keep checking kernel uniqueness throughout quarantine and activation, including
# nodes outside the restarting subset. A restart invalidates the pinned evidence.
wait_for_startup_activation() {  # <convergence seconds> <node>...
  local base="${1:?}" ip context current budget maximum=0 ready deadline status pending
  shift
  (( $# > 0 )) || { evidence_failure wait.nodes cluster 1; return 1; }
  local -A contexts=()
  [[ "$base" =~ ^[1-9][0-9]{0,5}$ ]] || { evidence_failure wait.base cluster 1; return 1; }
  deadline=$(( $(date +%s) + base ))
  for ip in "$@"; do
    context="$(timed_node_command "$ip" :)" || {
      evidence_failure wait.pin-context "$ip" "$?" "deadline_s=$deadline"; return 1;
    }
    contexts["$ip"]="$context"
    while :; do
      current="$(timed_node_command "$ip" :)" || {
        evidence_failure wait.policy.context "$ip" "$?" "deadline_s=$deadline"; return 1;
      }
      evidence_same_invocation wait.policy.invocation "$ip" "$context" "$current" "deadline_s=$deadline" || return 1
      if budget="$(node_startup_budget "$ip" "$base" "$context")"; then break; else status=$?; fi
      [[ "$status" == 2 ]] || {
        evidence_failure wait.policy.budget "$ip" "$status" "deadline_s=$deadline"; return 1;
      }
      (( $(date +%s) < deadline )) || {
        evidence_failure wait.policy.deadline "$ip" "$status" "deadline_s=$deadline" "base_s=$base"; return 1;
      }
      no_vip_is_duplicate || {
        evidence_failure wait.policy.uniqueness cluster "$?" "deadline_s=$deadline"; return 1;
      }
      sleep 2
    done
    (( budget <= maximum )) || maximum="$budget"
  done
  deadline=$(( $(date +%s) + maximum ))
  while :; do
    no_vip_is_duplicate || {
      evidence_failure wait.activation.uniqueness-before cluster "$?" "deadline_s=$deadline"; return 1;
    }
    ready=1
    pending=""
    for ip in "$@"; do
      current="$(timed_node_command "$ip" :)" || {
        evidence_failure wait.activation.context "$ip" "$?" "deadline_s=$deadline"; return 1;
      }
      evidence_same_invocation wait.activation.invocation "$ip" "${contexts[$ip]}" "$current" "deadline_s=$deadline" || return 1
      if node_activation_ready "$ip" "${contexts[$ip]}" >/dev/null; then
        :
      else
        status=$?
        [[ "$status" == 2 ]] || {
          evidence_failure wait.activation.ready "$ip" "$status" "deadline_s=$deadline" "budget_s=$maximum"; return 1;
        }
        ready=0
        pending+="${pending:+,}$ip"
      fi
    done
    no_vip_is_duplicate || {
      evidence_failure wait.activation.uniqueness-after cluster "$?" "deadline_s=$deadline"; return 1;
    }
    (( ready )) && return 0
    (( $(date +%s) < deadline )) || {
      evidence_failure wait.activation.deadline cluster 2 "deadline_s=$deadline" \
        "budget_s=$maximum" "pending_nodes=$pending"; return 1;
    }
    sleep 2
  done
}
