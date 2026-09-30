#!/usr/bin/env python3
"""Local journal oracle regressions. No cluster, process or clock access."""
import importlib.util
import io
import json
from pathlib import Path
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
FATAL = "ERROR keepafloatd::raft: cluster configuration mismatch confirmed; shutting down safely"
RESET = ("ERROR keepafloatd::raft: stale cluster incarnation confirmed; "
         "shutting down safely before rejoining with fresh state")


def record(message, stamp=200, **fields):
    return dict(_BOOT_ID=BOOT, _SYSTEMD_INVOCATION_ID=INV,
                __MONOTONIC_TIMESTAMP=str(stamp), MESSAGE=message, **fields)


def vip_event(action, vip=VIP, iface=IFACE):
    return f"INFO keepafloatd::vip: {action} {vip}/32 on {iface}"


def cycle(index, stamp):
    return [record("INFO openraft::core::raft_core: sm::StateMachine command done: "
                   f"BuildSnapshotDone: {{snapshot_id: snapshot-T1-N1.{index}, "
                   f"last_log:T1-N1.{index}, last_membership: {{}}}}", stamp),
            record("INFO openraft::engine::handler::log_handler: purge log, "
                   f"last_purged: None, purge_upto: T1-N1.{index-1000}", stamp+1)]


class EvidenceTests(unittest.TestCase):
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
