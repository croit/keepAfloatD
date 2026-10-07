#!/usr/bin/env python3
"""Local journal oracle regressions. No cluster, process or clock access."""
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import unittest

spec = importlib.util.spec_from_file_location(
    "evidence", Path(__file__).with_name("journal-evidence.py")
)
evidence = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence)

BOOT = "a" * 32
INV = "b" * 32
VIP = "198.51.100.1"
IFACE = "eth0"
FATAL = "ERROR keepafloatd::raft::control: cluster configuration mismatch confirmed; shutting down safely"
RESET = ("ERROR keepafloatd::raft::control: stale cluster incarnation confirmed; "
         "shutting down safely before rejoining with fresh state")
REPLICA = '0000000000000001:' + 'ab' * 32


def record(message, stamp=200, **fields):
    return dict(_BOOT_ID=BOOT, _SYSTEMD_INVOCATION_ID=INV,
                __MONOTONIC_TIMESTAMP=str(stamp), MESSAGE=message, **fields)


def vip_event(action, vip=VIP, iface=IFACE):
    return f"INFO keepafloatd::vip: {action} {vip}/32 on {iface}"


def cycle(index, stamp):
    return [record("INFO openraft::core::raft_core: sm::StateMachine command done: "
                   f"BuildSnapshotDone: {{snapshot_id: snapshot-T1-N{REPLICA}.{index}, "
                   f"last_log:T1-N{REPLICA}.{index}, last_membership: {{}}}}", stamp),
            record("INFO openraft::engine::handler::log_handler: purge log, "
                   f"last_purged: None, purge_upto: T1-N{REPLICA}.{index-1000}", stamp+1)]


class EvidenceTests(unittest.TestCase):
    def test_permission_expiry_requires_exact_terminal_cause_and_owned_vip_cleanup(self):
        terminal = 'Error: runtime admission task: runtime admission expired terminally'
        released = record(vip_event('unbound'), 200)
        self.check('permission-expiry', [released, record(terminal, 201)], IFACE, VIP)
        self.check('permission-expiry', [record(terminal, 200), record(vip_event('unbound'), 201)], IFACE, VIP)
        for cause in ['Error: network task failed', RESET, 'WARN runtime admission expired terminally',
                      terminal + ' maybe', 'Error: runtime admission task: runtime admission is sealed']:
            with self.subTest(cause=cause), self.assertRaises(ValueError):
                self.check('permission-expiry', [released, record(cause, 201)], IFACE, VIP)
        for rows in [[record(terminal)], [released],
                     [released, record(terminal, 201), record(vip_event('bound'), 202)],
                     [record(vip_event('unbound', '198.51.100.2')), record(terminal, 201)]]:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                self.check('permission-expiry', rows, IFACE, VIP)
        with self.assertRaises(ValueError):
            self.check('permission-expiry', [released, record(terminal, 201)], IFACE, VIP, '198.51.100.2')

    def test_permission_expiry_accepts_actual_multiline_shutdown_error(self):
        root = 'runtime admission task: runtime admission expired terminally'
        message = ('Error: additional daemon shutdown failures: Raft control task shutdown: '
                   'runtime admission task: ' + root + '\n\nCaused by:\n    ' + root)
        self.check('permission-expiry', [record(vip_event('unbound')), record(message, 201)], IFACE, VIP)
        renewal = 'Error: runtime admission task: runtime admission expired during renewal'
        self.check('permission-expiry', [record(vip_event('unbound')), record(renewal, 201)], IFACE, VIP)

    def test_permission_expiry_rejects_other_invocation_and_prefault_records(self):
        terminal = record('Error: runtime admission task: runtime admission expired terminally', 201)
        released = record(vip_event('unbound'), 200)
        for replacement in [dict(terminal, _SYSTEMD_INVOCATION_ID='c' * 32),
                            dict(terminal, _BOOT_ID='c' * 32)]:
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                self.check('permission-expiry', [released, replacement], IFACE, VIP)
        for rows in [[dict(released, __MONOTONIC_TIMESTAMP='99'), terminal],
                     [dict(terminal, __MONOTONIC_TIMESTAMP='99'), released]]:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                self.check('permission-expiry', rows, IFACE, VIP)

    def test_admitted_identity_and_promotion_require_exact_boot(self):
        admission = 'INFO keepafloatd::raft::admission::runtime::driver: runtime admission acquired replica=' + REPLICA
        self.assertEqual(self.check('admitted', [record(admission)], '1'), REPLICA)
        for rows, physical in [([], '1'), ([record(admission)] * 2, '1'), ([record(admission)], '2')]:
            with self.subTest(rows=rows, physical=physical), self.assertRaises(ValueError):
                self.check('admitted', rows, physical)
        promotion = 'INFO keepafloatd::raft::store::membership: committed learner promotion consumer=' + REPLICA
        self.check('promotion', [record(promotion)], REPLICA)
        for message in [promotion.replace(REPLICA, REPLICA.replace('ab', 'cd')), promotion + ' failed',
                        promotion.replace('committed', 'pending'),
                        promotion.replace('::store::membership:', '::admission::runtime::management:'),
                        promotion.replace('::store::membership:', '::admission::runtime::recovery:')]:
            with self.subTest(message=message), self.assertRaises(ValueError):
                self.check('promotion', [record(message)], REPLICA)

    def test_leader_requires_exact_latest_boot_identity(self):
        leader = 'raft current leader is now Some(ReplicaId { physical_id: 1, boot_nonce: [' + ', '.join(['171'] * 32) + '] })'
        self.assertEqual(evidence.leader_transition([leader]), REPLICA)
        self.assertEqual(evidence.leader_transition([leader, 'raft current leader is now None']), 'none')
        for invalid in ['Some(1)', 'Some(ReplicaId { physical_id: 1, boot_nonce: [171] })',
                        leader.split('now ', 1)[1].replace('171', '256'), 'invalid']:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                evidence.leader_transition([leader, 'raft current leader is now ' + invalid])
        self.assertEqual(evidence.physical_id(REPLICA), 1)
        with self.assertRaises(ValueError):
            evidence.physical_id('1')

    def test_startup_policy_and_activation_are_separate(self):
        policy = record('INFO keepafloatd::raft: runtime admission timing quarantine_ms=13000 vip_activation_ms=79000 startup_safety_wait_ms=92000', 100)
        admitted = record('INFO keepafloatd::raft::admission::runtime::driver: runtime admission acquired replica=' + REPLICA, 20000000)
        self.assertEqual(self.check('startup-budget', [policy]), 92000)
        with self.assertRaises(ValueError):
            self.check('activation', [policy], '999999999', '1')
        with self.assertRaises(ValueError):
            self.check('activation', [policy, admitted], '98999999', '1')
        self.assertEqual(self.check('activation', [policy, admitted], '99000000', '1'), REPLICA)
        for rows in [[policy, policy], [dict(policy, MESSAGE=policy['MESSAGE'].replace('92000', '1'))],
                     [dict(policy, MESSAGE=policy['MESSAGE'].replace('92000', '-1'))],
                     [dict(policy, MESSAGE=policy['MESSAGE'].replace('92000', '999999999999999999'))]]:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                self.check('startup-budget', rows)
        for bad in [dict(admitted, _SYSTEMD_INVOCATION_ID='c' * 32),
                    dict(admitted, MESSAGE=admitted['MESSAGE'].replace(REPLICA, '1')),
                    dict(admitted, MESSAGE=admitted['MESSAGE'].replace(REPLICA, '0000000000000002:' + 'ab' * 32))]:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.check('activation', [policy, bad], '999999999', '1')

    def test_pending_cli_is_distinct_from_invalid_journal(self):
        policy = record('INFO keepafloatd::raft: runtime admission timing quarantine_ms=13000 vip_activation_ms=79000 startup_safety_wait_ms=92000')
        for raw, expected in [('', 2), ('{', 1), (json.dumps(policy), 0),
                              (json.dumps(dict(policy, _BOOT_ID='c'*32)), 1)]:
            with self.subTest(raw=raw):
                result = subprocess.run(
                    [sys.executable, '-B', spec.origin, 'startup-budget', BOOT, INV, '0'],
                    input=raw, text=True, capture_output=True, timeout=2, check=False
                )
                self.assertEqual(result.returncode, expected, result.stderr)

    def test_startup_evidence_rejects_ambiguous_admission_and_bad_targets(self):
        policy = record('INFO keepafloatd::raft: runtime admission timing quarantine_ms=13000 vip_activation_ms=79000 startup_safety_wait_ms=92000', 100)
        admitted = record('INFO keepafloatd::raft::admission::runtime::driver: runtime admission acquired replica=' + REPLICA, 200)
        for targets in [(), ('1',), ('-1', '1'), ('100000000', 'wrong')]:
            with self.subTest(targets=targets), self.assertRaises(ValueError):
                self.check('activation', [policy, admitted], *targets)
        with self.assertRaises(ValueError):
            self.check('activation', [policy, admitted, admitted], '100000000', '1')
        with self.assertRaises(ValueError):
            self.check('activation', [admitted, dict(policy, __MONOTONIC_TIMESTAMP='300')], '100000000', '1')

    def test_snapshot_does_not_accept_physical_only_or_malformed_identity(self):
        for identity in ['1', REPLICA[:-1], REPLICA.upper()]:
            rows = [dict(row, MESSAGE=row['MESSAGE'].replace(REPLICA, identity))
                    for row in cycle(4999, 200)]
            self.assertEqual(self.check('snapshots', rows), 0)

    def check(self, mode, rows, *args):
        stream = io.StringIO("\n".join(json.dumps(row) for row in rows))
        return evidence.verify(mode, stream, BOOT, INV, 100, *args)

    def test_startup_requires_success_for_exact_orphan(self):
        good = vip_event("startup_cleanup: reclaimed orphan")
        self.check("startup", [record(good)], IFACE, VIP)
        for message in [good.replace(VIP, "198.51.100.2"),
                        good.replace("eth0", "eth1"),
                        good.replace("/32", "/24"),
                        "ERROR startup_cleanup: spawn ip del failed",
                        f"DEBUG keepafloatd::vip: startup_cleanup: {VIP}/32 on eth0 not present (ok)",
                        f"INFO keepafloatd::vip: dry-run: would reclaim {VIP}/32 on eth0 if present",
                        good + " failed"]:
            with self.subTest(message=message), self.assertRaises(ValueError):
                self.check("startup", [record(message)], IFACE, VIP)

    def test_incarnation_requires_confirmed_fatal_category(self):
        self.check("incarnation", [record(RESET)])
        for message in ["WARN unrelated epoch message", FATAL,
                        "WARN keepafloatd::raft: stale cluster incarnation: 1/3 strikes",
                        RESET.replace("ERROR", "WARN"), RESET + " failed"]:
            with self.subTest(message=message), self.assertRaises(ValueError):
                self.check("incarnation", [record(message)])

    def test_fatal_cleanup_accepts_both_safe_release_orders(self):
        for rows in [[record(FATAL, 200), record(vip_event("unbound"), 201)],
                     [record(vip_event("unbound"), 200), record(FATAL, 201)]]:
            self.check("config", rows, IFACE, VIP)

    def test_fatal_cleanup_requires_every_owned_vip_and_no_rebind(self):
        good = [record(vip_event("unbound")), record(FATAL, 201)]
        bad = [[record(vip_event("unbound", "198.51.100.2")), record(FATAL, 201)],
               [record(FATAL)], [record(vip_event("unbound"))],
               good + [record(vip_event("bound"), 202)],
               [record(vip_event("unbound")), record(vip_event("bound"), 201), record(FATAL, 202)]]
        for rows in bad:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                self.check("config", rows, IFACE, VIP)
        with self.assertRaises(ValueError):
            self.check("config", good, IFACE, VIP, "198.51.100.2")

    def test_snapshot_cycles_are_unique_completed_and_ordered(self):
        rows = cycle(4999, 200) + cycle(9999, 300)
        self.assertEqual(self.check("snapshots", rows), 2)
        self.assertEqual(self.check("snapshots", cycle(4999, 200)+cycle(4999, 300)), 1)
        self.assertEqual(self.check("snapshots", [record(rows[1]["MESSAGE"], 200),
                                                  record(rows[0]["MESSAGE"], 201)]), 0)
        self.assertEqual(self.check("snapshots", [rows[0], rows[2]]), 0)
        self.assertEqual(self.check("snapshots", [record(
            "INFO openraft::engine::handler::snapshot_handler: push snapshot building command"
        )]*12), 0)

    def test_snapshot_purge_cannot_cover_future_or_repeated_indices(self):
        rows = cycle(4999, 200)
        rows[1]["MESSAGE"] = rows[1]["MESSAGE"].replace("3999", "5999")
        self.assertEqual(self.check("snapshots", rows), 0)
        rows = cycle(4999, 200) + cycle(4999, 300)
        self.assertEqual(self.check("snapshots", rows), 1)

    def test_snapshot_window_may_start_before_the_first_event(self):
        self.assertEqual(self.check("snapshots", []), 0)

    def test_snapshot_completed_before_the_window_does_not_count(self):
        rows = cycle(4999, 50)
        rows[1]["__MONOTONIC_TIMESTAMP"] = "200"
        self.assertEqual(self.check("snapshots", rows), 0)

    def test_out_of_order_monotonic_records_fail_closed(self):
        with self.assertRaises(ValueError):
            self.check("config", [record(FATAL, 201), record(vip_event("unbound"), 200)], IFACE, VIP)

    def test_snapshot_errors_fail_even_after_enough_cycles(self):
        for error in ["Defensive", "LogIndexNotFound", "quit RaftCore"]:
            with self.subTest(error=error), self.assertRaises(ValueError):
                self.check("snapshots", cycle(4999, 200) + cycle(9999, 300)
                           + [record(error, 400)])

    def test_unreadable_stale_and_cross_process_evidence_is_rejected(self):
        good = record(vip_event("startup_cleanup: reclaimed orphan"))
        for field, value in [("_BOOT_ID", "c"*32),
                             ("_SYSTEMD_INVOCATION_ID", "c"*32),
                             ("__MONOTONIC_TIMESTAMP", "nope"),
                             ("MESSAGE", ["ambiguous"]), ("__MONOTONIC_TIMESTAMP", "50")]:
            row = dict(good, **{field: value})
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                self.check("startup", [row], IFACE, VIP)
        for raw in ["", "{", "[]", json.dumps(good) + "\n{", "null"]:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                evidence.verify("startup", io.StringIO(raw), BOOT, INV, 100, IFACE, VIP)

    def test_ansi_and_large_input_are_fully_consumed(self):
        good = record("\x1b[32m" + vip_event("startup_cleanup: reclaimed orphan") + "\x1b[0m")
        self.check("startup", [good] + [record("unrelated"*1000)]*100, IFACE, VIP)
        with self.assertRaises(ValueError):
            self.check("startup", [good, {"MESSAGE": "truncated"}], IFACE, VIP)


if __name__ == "__main__":
    unittest.main()
