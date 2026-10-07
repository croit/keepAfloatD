#!/bin/sh
set -eu

proc_root=${1:-/proc}
[ -d "$proc_root" ] || exit 1
for proc in "$proc_root"/[0-9]*; do
  [ -d "$proc" ] || continue
  if ! status=$(cat "$proc/status" 2>/dev/null); then
    [ ! -d "$proc" ] || exit 1
    continue
  fi
  # A paused parent cannot reap exited children, which cannot mutate VIPs.
  printf '%s\n' "$status" | awk '
    /^Name:/ { name = $2 }
    /^State:/ { state = $2 }
    END {
      if (name == "" || state == "") exit 1
      if (name == "ip" && state != "Z" && state != "X") exit 1
    }
  ' || exit 1
done
