#!/usr/bin/env python3
"""Local protocol and scenario regressions; no network or daemon execution."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

HERE = Path(__file__).resolve().parent


class ReplicaStatusTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("deadline_probe", HERE / "raft-deadline-probe.py")
        self.probe = importlib.util.module_from_spec(spec)
        # Fixture configs are JSON, so these network-contract checks need no PyYAML installation.
        with patch.dict(sys.modules, {"yaml": types.SimpleNamespace(safe_load=json.load)}):
            spec.loader.exec_module(self.probe)

    def run_status(self, leader):
        probe = self.probe
        config = {"node_id": 1, "cluster_secret": "test-only-common-secret-0123456789",
                  "peers": [{"id": i, "raft_address": f"192.0.2.{i}:9210"}
                            for i in (1, 2, 3)]}
        response = {"initialized": True, "member_count": 3,
                    "current_leader": leader, "config_fingerprint": {"version": 1}}
        stream = MagicMock()
        output = io.StringIO()
        with patch("builtins.open", return_value=io.StringIO(json.dumps(config))), \
                patch.object(probe.socket, "socket", return_value=stream), \
                patch.object(probe.auth_wire, "client") as auth, \
                patch.object(probe, "status", return_value=response), \
                contextlib.redirect_stdout(output):
            probe.run("unused.yaml", 2, status_only=True)
        stream.bind.assert_called_once_with(("192.0.2.1", 0))
        stream.connect.assert_called_once_with(("192.0.2.2", 9210))
        stream.settimeout.assert_called_once_with(2)
        auth.assert_called_once_with(stream, 1, 2, config["cluster_secret"])
        return json.loads(output.getvalue())

    def test_exact_replica_leader_is_accepted_without_erasing_boot(self):
        leader = "0000000000000002:" + "ab" * 32
        self.assertEqual(self.run_status(leader)["leader"], leader)

    def test_malformed_or_unconfigured_replica_is_rejected(self):
        for leader in (2, None, "2", "0000000000000002:" + "ab" * 31,
                       "0000000000000002:" + "AB" * 32,
                       "0000000000000004:" + "ab" * 32):
            with self.subTest(leader=leader), self.assertRaises((ValueError, RuntimeError)):
                self.run_status(leader)


class ConfigMajorityTests(unittest.TestCase):
    def run_scenario(self, activation_ok=True):
        script = (HERE / "scenarios/D19_config_identity.sh").read_text()
        script = "\n".join(line for line in script.splitlines() if not line.startswith("source "))
        fixture = r'''
set -euo pipefail
NODE_IPS=(node1 node2 node3); VIPS=(192.0.2.10); activated=0; fenced=0
scenario_start() { :; }; evid() { :; }; snapshot_vips() { :; }
backup_cluster_configs() { :; }; restore_cluster_configs() { :; }
holder_for_vip() { echo node1; }; nodes_except() { echo 'node2 node3'; }
instance_for_ip() { echo "$1"; }; node_has_vip_bound() { return 0; }
node_sh() { if [[ "$*" == *interval_ms* ]]; then echo 500; else echo 3; fi; }
next_behavior_changing_stale_secs() { echo 4; }
timed_node_command() { echo exact-original-context; }
kafd_restart() { :; }; sleep() { :; }; clean_reform() { :; }
node_active() { :; }; node_lacks_all_vips() { :; }
journal_event_count() { echo 0; }; wait_for_available_cluster() { :; }
check() { shift; "$@"; }; check_eq() { [[ "$2" == "$3" ]]; }
wait_until() { shift; "$@"; }; holds_for() { shift; "$@"; }
wait_for_startup_activation() {
  [[ "$*" == '30 node2 node3' ]] || return 1
  [[ "$ACTIVATION_OK" == 1 ]] || return 1
  activated=1
}
wait_for_live_service_without() {
  [[ "$*" == '90 node1' && "$activated" == 1 ]]
}
config_fence_observed() {
  [[ "$*" == 'node1 exact-original-context 192.0.2.10' ]] || return 1
  fenced=1
}
scenario_end() { [[ "$activated" == 1 && "$fenced" == 1 ]]; }
'''
        return subprocess.run(["bash", "-c", f"ACTIVATION_OK={int(activation_ok)}\n" + fixture + script],
                              capture_output=True, text=True, timeout=3)

    def test_new_majority_activates_before_service_and_original_config_fence_remains(self):
        result = self.run_scenario()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_failed_activation_cannot_pass(self):
        self.assertNotEqual(self.run_scenario(False).returncode, 0)


class IsolatedWrapperTests(unittest.TestCase):
    def run_wrapper(self, mode):
        script = (HERE / "scenarios/D26_isolated_health_proof.sh").read_text()
        function = "run_native_proof_regression() {" + script.split(
            "run_native_proof_regression() {", 1)[1].split("\n}\n", 1)[0] + "\n}\n"
        fixture = r'''
set -uo pipefail
NODE_IPS=(node1); SSH_COMMAND_TIMEOUT=60; payload=fixture; clock=0
date() { echo "$clock"; }
sleep() { clock=$((clock + 50)); }
node_sh() {
  [[ "$SSH_COMMAND_TIMEOUT" == 95 && "$1" == node1 ]] || return 88
  bash -n <<< "$2" || return 87
  if [[ "$2" == *'proof-poll'* ]]; then
    echo "POLL $clock" >&2
    case "$MODE" in
      missing) echo PENDING; return;;
      changed) if ((clock > 0)); then echo 'RUNNING 155000'; return; fi;;
      malformed) echo 'RUNNING nope'; return;;
      transport) return 255;;
      hanging) echo 'RUNNING 154149'; return;;
      premature) echo 'DONE 0'; return;;
      status-noncanonical) echo 'DONE 000'; return;;
      status-malformed) echo 'DONE 256'; return;;
      multiline) printf 'RUNNING 154149\nRUNNING 154150\n'; return;;
      extra) echo 'RUNNING 154149 extra'; return;;
      disappeared) if ((clock > 0)); then echo PENDING; return; fi;;
    esac
    if ((clock >= 200)); then
      if [[ "$MODE" == failure ]]; then echo 'DONE 1'; else echo 'DONE 0'; fi
    else echo 'RUNNING 154149'; fi
  elif [[ "$2" == *'proof-cleanup'* ]]; then
    echo CLEANUP >&2
    [[ "$MODE" != cleanup-error ]] || return 255
  elif [[ "$2" == *'proof-output'* ]]; then
    echo fixture-proof-output
  elif [[ "$2" == *'mktemp -d'* ]]; then
    echo /run/kafd-proof-campaign.fixture
  elif [[ "$2" == *'nohup setsid'* ]]; then
    [[ "$MODE" != launch-error ]] || return 255
  else return 89; fi
}
'''
        return subprocess.run(["bash", "-c", f"MODE={mode}\n" + fixture + function +
                               '\nrun_native_proof_regression; result=$?\n'
                               '[[ "$SSH_COMMAND_TIMEOUT" == 60 ]] || exit 90\nexit "$result"'],
                              capture_output=True, text=True, timeout=3)

    def test_policy_extends_whole_probe_not_individual_ssh_deadlines(self):
        result = self.run_wrapper("good")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("POLL 200", result.stderr)
        self.assertIn("CLEANUP", result.stderr)
        self.assertIn("fixture-proof-output", result.stdout)

    def test_failure_missing_or_changed_policy_fails_closed_and_cleans_up(self):
        for mode in ("failure", "missing", "changed", "malformed", "transport",
                     "hanging", "premature", "status-noncanonical", "status-malformed", "multiline", "extra",
                     "disappeared", "cleanup-error", "launch-error"):
            with self.subTest(mode=mode):
                result = self.run_wrapper(mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("CLEANUP", result.stderr)


class RemoteControlFlowTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="proof-remote-fixture-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.work = self.root / "kafd-proof-campaign.fixture"
        self.work.mkdir()
        self.netns = self.root / "netns"
        self.netns.mkdir()
        self.events = self.root / "events"
        self.events.touch()
        self.proc = self.root / "proc"
        self.proc.mkdir()
        self.shim = self.root / "control-stub.py"
        self.shim.write_text(r'''
import os, pathlib, sys, subprocess
source = pathlib.Path(os.environ['PROOF_HELPER']).read_text()
exec(compile(source.split("if len(sys.argv) > 1", 1)[0], '<proof-control>', 'exec'))
PROC_ROOT = pathlib.Path(os.environ['FAKE_ROOT']) / 'proc'
NETNS_ROOT = pathlib.Path(os.environ['FAKE_ROOT']) / 'netns'
events = pathlib.Path(os.environ['EVENTS'])
def event(message):
 with events.open('a') as stream: stream.write(message + '\n')
def fake_run(args, **kwargs):
 event('IP ' + ' '.join(args[1:]))
 assert args[:2] == ['ip', 'netns'], args
 namespace = NETNS_ROOT / args[3]
 if args[2] == 'pids':
  if os.environ['MODE'] == 'read-error': raise sp.CalledProcessError(42, args)
  return sp.CompletedProcess(args, 0, '999\n')
 if args[2] == 'del': namespace.unlink()
 elif args[2] == 'add': namespace.touch(exist_ok=False)
 else: raise AssertionError(args)
 return sp.CompletedProcess(args, 0)
sp.run = fake_run
def fake_pidfd_open(pid):
 if os.environ['MODE'] == 'pid-reuse-during-open':
  stat = PROC_ROOT / str(pid) / 'stat'
  prefix, fields = stat.read_text().rsplit(') ', 1)
  fields = fields.split()
  fields[19] = str(int(fields[19]) + 1)
  stat.write_text(prefix + ') ' + ' '.join(fields))
 return 10000 + pid
os.pidfd_open = fake_pidfd_open
if os.environ['MODE'] == 'no-pidfd-api': del os.pidfd_open
if os.environ['MODE'] == 'denied-pidfd':
 def denied_pidfd(pid): raise PermissionError('pidfd open denied')
 os.pidfd_open = denied_pidfd
real_close = os.close
os.close = lambda fd: real_close(fd) if fd < 10000 else None
signal.pidfd_send_signal = lambda fd, sig: event(f'SIGNAL {fd-10000} {sig}')
if os.environ['MODE'] == 'no-signal-api': del signal.pidfd_send_signal
if os.environ['MODE'] == 'denied-signal':
 def denied_signal(fd, sig): raise PermissionError('pidfd signal denied')
 signal.pidfd_send_signal = denied_signal
select.select = lambda read, write, error, timeout: (read, [], [])
if os.environ['MODE'] == 'finish-race':
 (pathlib.Path(sys.argv[-1]) / 'status').write_text('0\n')
 raise SystemExit(1)
proof_control(*sys.argv[3:])
''')
        script = (HERE / "scenarios/D26_isolated_health_proof.sh").read_text()
        function = "run_native_proof_regression() {" + script.split(
            "run_native_proof_regression() {", 1)[1].split("\n}\n", 1)[0] + "\n}\n"
        fixture = r'''
NODE_IPS=(node1); payload=fixture
node_sh() {
  if [[ "$2" == *proof-cleanup* ]]; then printf '%s' "$2" > "$CAPTURE/cleanup";
  elif [[ "$2" == *proof-poll* ]]; then printf '%s' "$2" > "$CAPTURE/poll"; echo 'DONE 1';
  elif [[ "$2" == *proof-output* ]]; then :;
  elif [[ "$2" == *'mktemp -d'* ]]; then echo /run/kafd-proof-campaign.fixture;
  else printf '%s' "$2" > "$CAPTURE/start"; fi
}
'''
        subprocess.run(["bash", "-c", fixture + function + "\nrun_native_proof_regression"],
                       env=dict(os.environ, CAPTURE=str(self.root)), capture_output=True,
                       text=True, timeout=3, check=False)

    def remote(self, operation, mode="good"):
        command = (self.root / operation).read_text()
        command = command.replace("/run/kafd-proof-campaign.fixture", str(self.work))
        command = command.replace("/run/netns/", str(self.netns) + "/")
        stubs = r'''
python3() { "$REAL_PYTHON" "$CONTROL_SHIM" "$@"; }
kill() { printf 'KILL %s\n' "$*" >> "$EVENTS"; }
sleep() { :; }
rm() { printf 'REMOVE %s\n' "$*" >> "$EVENTS"; }
ip() {
  printf 'IP %s\n' "$*" >> "$EVENTS"
  if [[ "$*" == 'netns pids '* ]]; then
    [[ "$MODE" != read-error ]] || return 42
    echo 999
  fi
}
'''
        return subprocess.run(["bash", "-c", stubs + command], capture_output=True,
                              text=True, timeout=3,
                              env=dict(os.environ, EVENTS=str(self.events), MODE=mode,
                                       REAL_PYTHON=sys.executable, CONTROL_SHIM=str(self.shim),
                                       PROOF_HELPER=str(HERE.parent / "e2e/scripts/isolated-health-proof.py"),
                                       FAKE_ROOT=str(self.root)))

    def process(self, pid=444, start=7, group=444):
        directory = self.proc / str(pid)
        directory.mkdir(exist_ok=True)
        fields = ["S", "1", str(group), str(group)] + ["0"] * 15 + [str(start)]
        (directory / "stat").write_text(f"{pid} (fixture process) " + " ".join(fields))
        return dict(pid=pid, group=group, session=group, start=start)

    def supervisor(self):
        (self.work / "process.json").write_text(json.dumps(self.process()))

    def control(self, action, mode="good"):
        return subprocess.run(
            [sys.executable, str(self.shim), "helper", "--proof-control", action, str(self.work)],
            capture_output=True, text=True, timeout=3,
            env=dict(os.environ, EVENTS=str(self.events), MODE=mode,
                     PROOF_HELPER=str(HERE.parent / "e2e/scripts/isolated-health-proof.py"),
                     FAKE_ROOT=str(self.root)))

    def test_unsupported_pidfd_fails_before_creating_namespace(self):
        for mode in ("no-pidfd-api", "no-signal-api", "denied-pidfd", "denied-signal"):
            with self.subTest(mode=mode):
                self.events.write_text("")
                (self.netns / ("kafd-proof-" + self.work.name)).unlink(missing_ok=True)
                (self.work / "namespace.json").unlink(missing_ok=True)
                result = self.control("create-namespace", mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("IP netns add", self.events.read_text())
                self.assertFalse((self.work / "namespace.json").exists())

    def test_supported_pidfd_preflight_creates_namespace_receipt(self):
        result = self.control("create-namespace")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("IP netns add", self.events.read_text())
        self.assertRegex(self.events.read_text(), r"SIGNAL [0-9]+ 0")
        self.assertTrue((self.work / "namespace.json").exists())

    def namespace(self):
        namespace = self.netns / ("kafd-proof-" + self.work.name)
        namespace.touch()
        identity = namespace.stat()
        (self.work / "namespace.json").write_text(json.dumps(dict(
            name=namespace.name, identity=[identity.st_dev, identity.st_ino])))
        self.process(999)
        (self.proc / "999/ns").mkdir()
        os.link(namespace, self.proc / "999/ns/net")
        return namespace

    def test_remote_cleanup_never_claims_preexisting_namespace_from_child_pid(self):
        (self.work / "status").write_text("1\n")
        (self.work / "namespace-pid").write_text("444\n")
        (self.netns / "kafd-proof-444").touch()
        result = self.remote("cleanup")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("KILL", self.events.read_text())
        self.assertNotIn("netns del", self.events.read_text())
        self.assertTrue((self.netns / "kafd-proof-444").exists())

    def test_remote_cleanup_propagates_namespace_pid_read_error(self):
        (self.work / "status").write_text("1\n")
        self.namespace()
        result = self.remote("cleanup", "read-error")
        self.assertNotEqual(result.returncode, 0, self.events.read_text())
        self.assertIn("exit status 42", result.stderr)
        self.assertNotIn("netns del", self.events.read_text())

    def test_remote_cleanup_never_signals_unverified_process_generation(self):
        (self.work / "pid").write_text("444\n")
        result = self.remote("cleanup")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("KILL", self.events.read_text())
        self.assertNotIn("SIGNAL", self.events.read_text())

    def test_remote_poll_keeps_published_policy_after_python_removes_logs(self):
        (self.work / "pid").write_text("444\n")
        self.supervisor()
        (self.work / "policy").write_text("154149\n")
        logs = self.work / "kafd-isolated-health-fixture"
        logs.mkdir()
        log = logs / "1.log"
        log.write_text("INFO runtime admission timing startup_safety_wait_ms=154149\n")
        self.assertEqual(self.remote("poll").stdout.strip(), "RUNNING 154149")
        log.unlink()
        logs.rmdir()
        self.assertEqual(self.remote("poll").stdout.strip(), "RUNNING 154149")

    def test_reused_supervisor_pid_is_not_signalled(self):
        self.supervisor()
        self.process(start=9)
        result = self.remote("cleanup")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("generation changed", result.stderr)
        self.assertNotIn("SIGNAL", self.events.read_text())

    def test_pid_reuse_while_pinning_supervisor_never_signals(self):
        self.supervisor()
        result = self.remote("cleanup", "pid-reuse-during-open")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("generation changed while pinning", result.stderr)
        self.assertNotIn("SIGNAL", self.events.read_text())

    def test_pid_reuse_while_pinning_namespace_process_never_signals(self):
        (self.work / "status").write_text("0\n")
        self.namespace()
        result = self.remote("cleanup", "pid-reuse-during-open")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("process no longer belongs", result.stderr)
        self.assertNotIn("SIGNAL", self.events.read_text())
        self.assertNotIn("netns del", self.events.read_text())

    def test_namespace_without_creation_receipt_is_never_deleted(self):
        (self.work / "status").write_text("1\n")
        namespace = self.netns / ("kafd-proof-" + self.work.name)
        namespace.touch()
        result = self.remote("cleanup")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no creation receipt", result.stderr)
        self.assertEqual(self.events.read_text(), "")

    def test_replaced_namespace_is_not_deleted_or_signalled(self):
        (self.work / "status").write_text("1\n")
        namespace = self.namespace()
        namespace.rename(self.root / "old-namespace")
        namespace.touch()
        result = self.remote("cleanup")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("namespace identity changed", result.stderr)
        self.assertEqual(self.events.read_text(), "")

    def test_owned_namespace_is_cleaned_using_pinned_process(self):
        (self.work / "status").write_text("0\n")
        namespace = self.namespace()
        result = self.remote("cleanup")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("SIGNAL 999 9", self.events.read_text())
        self.assertFalse(namespace.exists())
        self.assertFalse(self.work.exists())

    def test_owned_supervisor_is_signalled_using_verified_handles(self):
        self.supervisor()
        result = self.remote("cleanup")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.events.read_text(), "SIGNAL 444 15\n")

    def test_missing_supervisor_cannot_skip_owned_namespace_cleanup(self):
        self.supervisor()
        (self.proc / "444/stat").unlink()
        namespace = self.namespace()
        result = self.remote("cleanup")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("SIGNAL 999 9", self.events.read_text())
        self.assertFalse(namespace.exists())
        self.assertTrue(self.work.exists(), "failed cleanup preserves its diagnostics")

    def test_status_published_during_supervisor_check_wins(self):
        self.supervisor()
        result = self.remote("poll", "finish-race")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "DONE 0")

    def test_poll_before_supervisor_receipt_is_pending(self):
        result = self.remote("poll")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "PENDING")


if __name__ == "__main__":
    unittest.main()
