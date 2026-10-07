#!/usr/bin/env bash
set -euo pipefail

case " $* " in
  *' addr del '*|*' route del '*)
    if [[ -f "${KEEPAFLOATD_STOP_FIXTURE}/delay" ]]; then
      sleep 0.1
      read -r elapsed _ </proc/uptime
      printf '%s %s %s\n' "${PPID}" "${elapsed}" "$*" >>"${KEEPAFLOATD_STOP_FIXTURE}/delete-progress"
    fi
    if [[ -f "${KEEPAFLOATD_STOP_FIXTURE}/stall-once" ]]; then
      mv "${KEEPAFLOATD_STOP_FIXTURE}/stall-once" "${KEEPAFLOATD_STOP_FIXTURE}/stall-used"
      printf '%s\n' "${PPID}" >"${KEEPAFLOATD_STOP_FIXTURE}/stalled-pid"
      kill -STOP "${PPID}"
    fi
    ;;
esac
exec "${KEEPAFLOATD_REAL_IP:?}" "$@"
