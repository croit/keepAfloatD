#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

check_budget() {
  local vips="$1" base="$2" expected="$3" actual
  actual="$(E2E_VIPS="${vips}" bash -c '
    source "$1/lib.sh"
    cleanup_budget_seconds "$2"
  ' bash "${HERE}" "${base}")"
  [[ "${actual}" == "${expected}" ]] || {
    printf 'budget: expected %s, got %s\n' "${expected}" "${actual}" >&2
    return 1
  }
  printf 'ok - %s seconds with %s configured VIPs gives %s\n' \
    "${base}" "$(wc -w <<<"${vips}")" "${actual}"
}

check_budget '192.0.2.1' 20 32
check_budget '192.0.2.1 192.0.2.2' 20 43
check_budget '192.0.2.1 192.0.2.2 192.0.2.3' 20 55
check_budget '192.0.2.1 192.0.2.2 192.0.2.3' 25 60
check_budget '192.0.2.1 192.0.2.2 192.0.2.3' 30 65
check_budget '192.0.2.1 192.0.2.2 192.0.2.3' 90 125
check_budget '192.0.2.1 192.0.2.2 192.0.2.3' 100 135
check_budget '192.0.2.1 192.0.2.2 192.0.2.3 192.0.2.4' 45 91

# Exercise argument bounds without starting pressure workers or opening sockets.
python3 -B - "${HERE}/connection-pressure.py" <<'PY'
import contextlib
import importlib.util
import io
import sys
from unittest.mock import AsyncMock, patch

spec = importlib.util.spec_from_file_location("pressure", sys.argv[1])
pressure = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pressure)
base = ["pressure", "--endpoint", "fixture,192.0.2.1,1,raft",
        "--status", "unused-status", "--stop", "unused-stop"]
cases = [(90, 32, True), (125, 9, True), (125, 64, True),
         (126, 32, False), (0, 32, False), (125, 8, False), (125, 65, False)]
for duration, connections, accepted in cases:
    run = AsyncMock()
    with patch.object(pressure, "run", run), patch.object(
        sys, "argv", base + ["--duration", str(duration), "--connections", str(connections)]
    ), contextlib.redirect_stderr(io.StringIO()):
        try:
            pressure.main()
        except SystemExit as error:
            assert not accepted and error.code == 2
        else:
            assert accepted
        assert run.await_count == int(accepted)
    print(f"ok - pressure duration={duration} connections={connections} accepted={accepted}")
PY

# Load only pure observation helpers, never daemon or namespace setup.
python3 -B - "${HERE}/isolated-health-proof.py" <<'PY'
import ast
import pathlib
import re
import sys
from types import SimpleNamespace

tree = ast.parse(pathlib.Path(sys.argv[1]).read_text())
names = {"assert_unique_states", "observe_isolated_failure", "safety_wait"}
definitions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
namespace = {"time": None, "re": re}
exec(compile(ast.Module(body=definitions, type_ignores=[]), sys.argv[1], "exec"), namespace)
observe = namespace["observe_isolated_failure"]
assert namespace["safety_wait"]("runtime admission timing startup_safety_wait_ms=93500") == 93.5
for bad in ("", "runtime admission timing startup_safety_wait_ms=1.5",
            "runtime admission timing startup_safety_wait_ms=invalid"):
    try:
        namespace["safety_wait"](bad)
    except ValueError:
        pass
    else:
        raise AssertionError("accepted malformed startup timing")

cases = [
    ("late successor", 2, 35, None, None, 4, 1, None),
    ("early successor still observes self-fence deadline", 2, 3, None, None, 4, 1, None),
    ("late self-fence", 8, 35, None, None, 9, 1, "did not fence"),
    ("missing successor", 2, 50, None, None, 4, 1, "did not take over"),
    ("duplicate survivor holders", 2, 35, 20, None, 4, 1, "overlaps"),
    ("majority exit during extended wait", 2, 35, None, 20, 4, 1, "majority daemon"),
    ("victim exits without cleanup", 8, 35, None, None, 4, 1, "did not fence"),
    ("victim never exits", 2, 35, None, None, None, 1, "did not stop"),
    ("unexpected victim exit", 2, 35, None, None, 4, 2, "unexpected status"),
]
for name, withdrawal, takeover, duplicate_at, majority_exit, victim_exit, code, failure in cases:
    clock = SimpleNamespace(now=0)
    def sleep(seconds):
        clock.now += seconds
    def states():
        holders = {1: set(), 2: {"b"}, 3: {"c"}}
        if clock.now < withdrawal:
            holders[1].add("a")
        if clock.now >= takeover or (duplicate_at is not None and clock.now >= duplicate_at):
            holders[2].add("a")
        if duplicate_at is not None and clock.now >= duplicate_at:
            holders[3].add("a")
        return holders
    processes = {
        1: SimpleNamespace(poll=lambda: code if victim_exit is not None and clock.now >= victim_exit else None),
        2: SimpleNamespace(poll=lambda: 1 if majority_exit is not None and clock.now >= majority_exit else None),
        3: SimpleNamespace(poll=lambda: None),
    }
    try:
        observe(processes, states, 1, "a",
                SimpleNamespace(monotonic=lambda: clock.now, sleep=sleep))
    except AssertionError as error:
        assert failure and failure in str(error), (name, str(error))
    else:
        assert failure is None, name
        assert max(7, takeover, victim_exit) <= clock.now < max(7, takeover, victim_exit) + 0.1
    if name == "late self-fence":
        assert 7 <= clock.now < 7.1, clock.now
    if duplicate_at is not None or majority_exit is not None:
        assert 20 <= clock.now < 20.1, clock.now
    if name in ("missing successor", "victim never exits"):
        assert 42 <= clock.now < 42.1, clock.now
    if name in ("victim exits without cleanup", "unexpected victim exit"):
        assert 4 <= clock.now < 4.1, clock.now
    print(f"ok - observation fixture: {name}")
PY
