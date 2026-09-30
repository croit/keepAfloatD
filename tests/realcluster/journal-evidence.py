#!/usr/bin/env python3
"""Validate scenario evidence from one boot and one systemd invocation."""
import json
import re
import sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
CONFIG_FENCE = ("ERROR keepafloatd::raft: cluster configuration mismatch "
                "confirmed; shutting down safely")
EPOCH_FENCE = ("ERROR keepafloatd::raft: stale cluster incarnation confirmed; "
               "shutting down safely before rejoining with fresh state")
SNAPSHOT = re.compile(
    r"^INFO openraft::core::raft_core: sm::StateMachine command done: "
    r"BuildSnapshotDone: \{snapshot_id: ([^,]+), last_log:T[0-9]+-N[0-9]+\.([0-9]+),"
)
PURGE = re.compile(
    r"^INFO openraft::engine::handler::log_handler: purge log, .*"
    r"purge_upto: T[0-9]+-N[0-9]+\.([0-9]+)$"
)


def journal_rows(stream, boot, invocation, after):
    if not all(re.fullmatch(r"[a-f0-9]{32}", x) for x in (boot, invocation)):
        raise ValueError("invalid journal scope")
    if after < 0:
        raise ValueError("invalid start timestamp")
    rows = []
    previous = -1
    for line in stream:
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            raise ValueError("journal record is not an object")
        if row.get("_BOOT_ID") != boot or row.get("_SYSTEMD_INVOCATION_ID") != invocation:
            raise ValueError("journal record belongs to another process or boot")
        stamp = row.get("__MONOTONIC_TIMESTAMP")
        message = row.get("MESSAGE")
        if not isinstance(stamp, str) or not re.fullmatch(r"[0-9]+", stamp):
            raise ValueError("invalid monotonic timestamp")
        if not isinstance(message, str):
            raise ValueError("missing or ambiguous journal message")
        if int(stamp) < previous:
            raise ValueError("journal timestamps are out of order")
        previous = int(stamp)
        if int(stamp) >= after:
            rows.append((int(stamp), ANSI.sub("", message).strip()))
    return rows


def snapshot_cycles(rows):
    completed = set()
    latest = paired = purged = -1
    count = 0
    for _, message in rows:
        if any(error in message for error in ("Defensive", "LogIndexNotFound", "quit RaftCore")):
            raise ValueError("Raft failure during endurance")
        snapshot = SNAPSHOT.match(message)
        if snapshot and snapshot[1] not in completed:
            completed.add(snapshot[1])
            latest = max(latest, int(snapshot[2]))
        purge = PURGE.fullmatch(message)
        if purge:
            index = int(purge[1])
            if purged < index <= latest and latest > paired:
                count += 1
                paired = latest
            purged = max(purged, index)
    return count


def verify(mode, stream, boot, invocation, after, *targets):
    rows = journal_rows(stream, boot, invocation, after)
    messages = [message for _, message in rows]
    if mode == "snapshots":
        return snapshot_cycles(rows)
    if mode == "incarnation":
        if EPOCH_FENCE not in messages:
            raise ValueError("missing confirmed incarnation reset")
        return "confirmed incarnation fence in original process"
    if len(targets) < 2:
        raise ValueError("interface and VIP required")
    iface, *vips = targets
    if mode == "startup":
        for vip in vips:
            expected = f"INFO keepafloatd::vip: startup_cleanup: reclaimed orphan {vip}/32 on {iface}"
            if expected not in messages:
                raise ValueError(f"missing successful orphan reclamation for {vip}")
        return "exact orphan reclaimed by the restarted process"
    if mode != "config" or CONFIG_FENCE not in messages:
        raise ValueError("missing confirmed config fence in original process")
    fatal = messages.index(CONFIG_FENCE)
    orders = set()
    for vip in vips:
        bound = True
        released = None
        for index, message in enumerate(messages):
            if message == f"INFO keepafloatd::vip: unbound {vip}/32 on {iface}":
                bound = False
                released = index
            elif message == f"INFO keepafloatd::vip: bound {vip}/32 on {iface}":
                if index > fatal:
                    raise ValueError(f"VIP rebound after fatal fence: {vip}")
                bound = True
        if bound or released is None:
            raise ValueError(f"missing final release of original VIP: {vip}")
        orders.add("before" if released < fatal else "after")
    return "original VIP release observed " + "/".join(sorted(orders)) + " fatal fence"


if __name__ == "__main__":
    try:
        mode, boot, invocation, after, *targets = sys.argv[1:]
        print(verify(mode, sys.stdin, boot, invocation, int(after), *targets))
    except (ValueError, TypeError) as error:
        print(f"invalid scenario evidence: {error}", file=sys.stderr)
        sys.exit(1)
