#!/bin/sh
set -eu

toggle_file="${1:?usage: health.sh <toggle-file>}"

if [ -f "${toggle_file}" ]; then
  exit 1
fi

exit 0
