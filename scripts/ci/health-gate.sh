#!/usr/bin/env bash
set -euo pipefail

state_file="${1:?usage: health-gate.sh <state-file>}"

if [[ -f "${state_file}" ]]; then
  exit 0
fi

exit 1
