#!/usr/bin/env sh
set -eu

REPORT="${1:-coverage/cobertura.xml}"
MINIMUM="${COVERAGE_MIN_PER_FILE:-0.8}"

grep -o '<class [^>]*>' "${REPORT}" | awk -v minimum="${MINIMUM}" '
function attribute(text, name, marker, rest, end) {
  marker = name "=\""
  if (index(text, marker) == 0) {
    return ""
  }
  rest = substr(text, index(text, marker) + length(marker))
  end = index(rest, "\"")
  return end == 0 ? "" : substr(rest, 1, end - 1)
}

{
  file = attribute($0, "filename")
  rate = attribute($0, "line-rate")
  if (file ~ /^src\/.*\.rs$/) {
    seen += 1
    if (rate == "") {
      printf "coverage gate: missing line-rate for %s\n", file > "/dev/stderr"
      failed += 1
    } else if ((rate + 0) < minimum) {
      printf "coverage gate: %s is %.2f%%, below %.2f%%\n", file, rate * 100, minimum * 100 > "/dev/stderr"
      failed += 1
    }
  }
}

END {
  if (seen == 0) {
    print "coverage gate: no production Rust files in report" > "/dev/stderr"
    exit 1
  }
  if (failed != 0) {
    exit 1
  }
  printf "Per-file coverage gate passed: %d production Rust files >= %.2f%%\n", seen, minimum * 100
}
'
