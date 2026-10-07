#!/usr/bin/env bash
# Three real candidate daemons on the lab kernel; VIP effects are dry-run in a private namespace.
SCENARIO_NAME=D26_isolated_health_proof
source "$(dirname "$0")/../scenario.sh"
scenario_start "blocked health probe fences an isolated leader; a fresh process rejoins"

payload="$(tar -C "${HERE}/../e2e/scripts" -czf - \
  isolated-health-proof.sh isolated-health-proof.py | base64 -w0)"

run_native_proof_regression() {
  (
    # SSH operations keep their original bound; only the whole probe includes startup fences.
    local SSH_COMMAND_TIMEOUT=95 work started deadline policy="" reply kind value extra
    started=$(date +%s)
    deadline=$((started + 95))
    work=$(node_sh "${NODE_IPS[0]}" "
      set -euo pipefail
      work=\$(mktemp -d /run/kafd-proof-campaign.XXXXXX)
      trap 'rm -rf -- \"\$work\"' ERR
      printf '%s' '${payload}' | base64 -d | tar -xzf - -C \"\$work\"
      python3 \"\$work/isolated-health-proof.py\" --proof-control preflight
      printf '%s\\n' \"\$work\"
    ") || return 1
    [[ "$work" =~ ^/run/kafd-proof-campaign\.[A-Za-z0-9]+$ ]] || return 1
    cleanup_native_proof() {
      local result=$?
      trap - EXIT
      node_sh "${NODE_IPS[0]}" "
        # proof-cleanup: these files and the private namespace belong only to this run.
        set -euo pipefail
        work='$work'
        python3 \"\$work/isolated-health-proof.py\" --proof-control cleanup \"\$work\"
      " || result=1
      exit "$result"
    }
    trap cleanup_native_proof EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    node_sh "${NODE_IPS[0]}" "
      set -euo pipefail
      work='$work'
      export TMPDIR=\"\$work\" KEEP_AFLOATD_BIN=/usr/bin/keepafloatd KAFD_PROOF_CONTROL_DIR=\"\$work\"
      nohup setsid bash -c '
        set -euo pipefail
        python3 \"\$1/isolated-health-proof.py\" --proof-control record-process \"\$1\" \"\$\$\"
        bash \"\$1/isolated-health-proof.sh\" &
        child=\$!
        if wait \"\$child\"; then status=0; else status=\$?; fi
        printf \"%s\\n\" \"\$status\" > \"\$1/status.tmp\"
        mv \"\$1/status.tmp\" \"\$1/status\"
      ' proof \"\$work\" >\"\$work/output\" 2>&1 </dev/null &
      printf '%s\\n' \"\$!\" > \"\$work/pid\"
    " || return 1
    while (( $(date +%s) < deadline )); do
      reply=$(node_sh "${NODE_IPS[0]}" "
        # proof-poll: the candidate publishes policy outside its disposable logs.
        set -euo pipefail
        work='$work'
        if test -f \"\$work/status\"; then
          printf 'DONE %s\\n' \"\$(cat \"\$work/status\")\"
        else
          if test -f \"\$work/process.json\" && ! python3 \"\$work/isolated-health-proof.py\" --proof-control alive \"\$work\"; then
            test -f \"\$work/status\" || exit 1
          fi
          if test -f \"\$work/status\"; then printf 'DONE %s\\n' \"\$(cat \"\$work/status\")\"
          elif test -f \"\$work/policy\"; then printf 'RUNNING %s\\n' \"\$(cat \"\$work/policy\")\"
          else printf 'PENDING\\n'; fi
        fi
      ") || return 1
      [[ "$reply" != *$'\n'* ]] || return 1
      read -r kind value extra <<< "$reply"
      [[ -z "$extra" ]] || return 1
      case "$kind" in
        DONE)
          [[ "$value" =~ ^(0|[1-9][0-9]{0,2})$ ]] && ((10#$value <= 255)) || return 1
          node_sh "${NODE_IPS[0]}" "# proof-output
            cat '$work/output'" || return 1
          [[ "$value" != 0 || -n "$policy" ]] || return 1
          return "$((10#$value))"
          ;;
        RUNNING)
          [[ "$value" =~ ^[1-9][0-9]{0,11}$ ]] || return 1
          [[ -z "$policy" || "$policy" == "$value" ]] || return 1
          policy="$value"
          # Match the existing Python work budget and two startup fences, plus SSH/cleanup grace.
          deadline=$((started + 95 + 2 * ((10#$policy + 999) / 1000)))
          ;;
        PENDING) [[ -z "$value" && -z "$policy" ]] || return 1;;
        *) return 1;;
      esac
      sleep 2
    done
    printf 'isolated health proof exceeded its startup-policy budget\n' >&2
    return 1
  )
}

check "actual candidate fences a blocked-probe leader and its fresh process rejoins" \
  run_native_proof_regression
check "the real service cluster remains uniquely available" wait_for_available_cluster
scenario_end
