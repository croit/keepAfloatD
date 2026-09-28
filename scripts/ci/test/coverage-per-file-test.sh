#!/usr/bin/env sh
set -eu

HERE="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
GATE="${HERE}/coverage-per-file.sh"

passing='<coverage><class filename="src/exact.rs" line-rate="0.8"><class filename="src/high.rs" line-rate="1"></coverage>'
printf '%s\n' "${passing}" | "${GATE}" - >/dev/null

failing='<coverage><class filename="src/low.rs" line-rate="0.799"></coverage>'
if output="$(printf '%s\n' "${failing}" | "${GATE}" - 2>&1)"; then
  echo "coverage gate accepted a production file below 80%" >&2
  exit 1
fi
case "${output}" in
  *"src/low.rs"*"79.90%"*) ;;
  *) echo "coverage gate did not identify the failing file and rate: ${output}" >&2; exit 1 ;;
esac

if output="$(printf '%s\n' '<coverage></coverage>' | "${GATE}" - 2>&1)"; then
  echo "coverage gate accepted a report without production files" >&2
  exit 1
fi
case "${output}" in
  *"no production Rust files"*) ;;
  *) echo "coverage gate did not reject an empty report clearly: ${output}" >&2; exit 1 ;;
esac

echo "Per-file coverage gate regression checks passed"
