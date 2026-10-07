#!/usr/bin/env bash

# Callers provide compose, wait_until, service_logs_contain, fail and
# wait_for_even_over_nodes; this helper does not set up a test topology.

# Startup alone includes quarantine and the first-admission effect fence.
# Read the daemon's policy rather than duplicating production constants here.
startup_budget_from_log() {
  local base_seconds="${1:?convergence seconds required}" milliseconds
  milliseconds="$(sed -E $'s/\033\\[[0-9;]*[mK]//g' | awk '
    /runtime admission timing/ {
      value = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^startup_safety_wait_ms=/) {
          value = substr($i, length("startup_safety_wait_ms=") + 1)
        }
      }
    }
    END { print value }
  ')" || return 1
  [[ "${milliseconds}" =~ ^(0|[1-9][0-9]{0,11})$ ]] || return 1
  printf '%s\n' "$((base_seconds + (milliseconds + 999) / 1000))"
}

service_startup_budget() {
  local service="${1:?service required}" base="${2:?convergence seconds required}"
  local logs
  logs="$(compose logs --no-color "${service}")" || return 1
  printf '%s\n' "${logs}" | startup_budget_from_log "${base}"
}

startup_budget_seconds() {
  local base="${1:?convergence seconds required}"
  shift
  local maximum=0 service budget
  for service in "$@"; do
    wait_until 5 service_logs_contain "${service}" 'runtime admission timing' || {
      fail "${service} did not report its admission startup policy"
      return 1
    }
    budget="$(service_startup_budget "${service}" "${base}")" || {
      fail "${service} reported invalid admission startup timing"
      return 1
    }
    ((budget <= maximum)) || maximum="${budget}"
  done
  ((maximum > 0)) || return 1
  printf '%s\n' "${maximum}"
}

wait_for_startup_over_nodes() {
  local base="${1:?convergence seconds required}"
  shift
  local budget
  budget="$(startup_budget_seconds "${base}" "$@")" || return 1
  wait_for_even_over_nodes "${budget}" "$@"
}
