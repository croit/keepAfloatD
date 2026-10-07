#!/usr/bin/env python3
"""Validate scenario evidence from one boot and one systemd invocation."""
import json
import re
import sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
CONFIG_FENCE = ("ERROR keepafloatd::raft::control: cluster configuration mismatch "
                "confirmed; shutting down safely")
EPOCH_FENCE = ("ERROR keepafloatd::raft::control: stale cluster incarnation confirmed; "
               "shutting down safely before rejoining with fresh state")
REPLICA = r"[0-9a-f]{16}:[0-9a-f]{64}"
SNAPSHOT = re.compile(
    r"^INFO openraft::core::raft_core: sm::StateMachine command done: "
    r"BuildSnapshotDone: \{snapshot_id: ([^,]+), last_log:T[0-9]+-N" + REPLICA + r"\.([0-9]+),"
)
PURGE = re.compile(
    r"^INFO openraft::engine::handler::log_handler: purge log, .*"
    r"purge_upto: T[0-9]+-N" + REPLICA + r"\.([0-9]+)$"
)
POLICY = re.compile(
    r"INFO keepafloatd::raft: runtime admission timing "
    r"quarantine_ms=([0-9]{1,12}) vip_activation_ms=([0-9]{1,12}) "
    r"startup_safety_wait_ms=([0-9]{1,12})"
)
ADMITTED = re.compile(
    r"INFO keepafloatd::raft::admission::runtime::driver: runtime admission acquired replica=(" + REPLICA + r")"
)
TERMINAL_EXPIRY = re.compile(
    r"(?:Error: )?(?:[0-9]+: )?runtime admission task: runtime admission expired (?:terminally|during renewal)"
)
PROMOTED = re.compile(
    r"INFO keepafloatd::raft::store::membership: committed learner promotion consumer=(" + REPLICA + r")"
)


class EvidencePending(ValueError):
    """A readable current invocation has not reached the required lifecycle step."""


def physical_id(replica):
    if not re.fullmatch(REPLICA, replica):
        raise ValueError("invalid exact replica identity")
    return int(replica[:16], 16)


def leader_transition(lines):
    latest = None
    for line in lines:
        line = ANSI.sub("", line).strip()
        if "raft current leader is now " in line:
            latest = line.split("raft current leader is now ", 1)[1]
    if latest == "None":
        return "none"
    match = re.fullmatch(
        r"Some\(ReplicaId \{ physical_id: ([0-9]{1,20}), boot_nonce: \[([0-9, ]+)\] \}\)", latest or ""
    )
    if not match:
        raise ValueError("missing or malformed latest leader identity")
    physical = int(match[1])
    nonce = match[2].split(", ")
    if physical >= 1 << 64 or len(nonce) != 32 or any(
        not re.fullmatch(r"[0-9]{1,3}", byte) or int(byte) > 255 for byte in nonce
    ):
        raise ValueError("invalid physical identity or boot nonce")
    return f"{physical:016x}:" + "".join(f"{int(byte):02x}" for byte in nonce)


def startup_evidence(mode, rows, targets):
    policies = [(stamp, POLICY.fullmatch(message)) for stamp, message in rows
                if "runtime admission timing" in message]
    if not policies:
        raise EvidencePending("startup policy has not been published")
    if len(policies) != 1 or not policies[0][1]:
        raise ValueError("missing or ambiguous startup policy")
    policy_stamp, policy = policies[0]
    quarantine, activation, total = map(int, policy.groups())
    if min(quarantine, activation) <= 0 or quarantine + activation != total:
        raise ValueError("inconsistent startup policy")
    if mode == "startup-budget":
        return total
    if len(targets) != 2 or not all(re.fullmatch(r"[0-9]+", x) for x in targets):
        raise ValueError("current monotonic time and physical identity required")
    now, expected = map(int, targets)
    admissions = [(stamp, ADMITTED.fullmatch(message)) for stamp, message in rows
                  if "runtime admission acquired" in message]
    if not admissions:
        raise EvidencePending("runtime admission is still pending")
    if len(admissions) != 1 or not admissions[0][1]:
        raise ValueError("missing or ambiguous runtime admission")
    stamp, admitted = admissions[0]
    if stamp < policy_stamp or physical_id(admitted[1]) != expected:
        raise ValueError("admission belongs to another node or predates startup")
    if now < stamp + activation * 1000:
        raise EvidencePending("runtime VIP activation is still pending")
    return admitted[1]


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
    if mode in ("startup-budget", "activation"):
        return startup_evidence(mode, rows, targets)
    messages = [message for _, message in rows]
    if mode == "admitted":
        if len(targets) != 1 or not re.fullmatch(r"[0-9]+", targets[0]):
            raise ValueError("physical identity required")
        admissions = [ADMITTED.fullmatch(message) for message in messages
                      if "runtime admission acquired" in message]
        if len(admissions) != 1 or not admissions[0]:
            raise ValueError("missing or ambiguous admitted boot")
        replica = admissions[0][1]
        if physical_id(replica) != int(targets[0]):
            raise ValueError("admitted boot belongs to another physical node")
        return replica
    if mode == "promotion":
        if len(targets) != 1:
            raise ValueError("exact promoted boot required")
        physical_id(targets[0])
        if not any((match := PROMOTED.fullmatch(message)) and match[1] == targets[0]
                   for message in messages):
            raise EvidencePending("exact learner promotion has not been observed")
        return targets[0]
    if mode == "permission-expiry":
        if len(targets) < 2:
            raise ValueError("interface and originally held VIPs required")
        if not any(TERMINAL_EXPIRY.fullmatch(line.strip())
                   for message in messages for line in message.splitlines()):
            raise ValueError("missing terminal permission expiry in original process")
        iface, *vips = targets
        for vip in vips:
            released = False
            for message in messages:
                if message == f"INFO keepafloatd::vip: unbound {vip}/32 on {iface}":
                    released = True
                elif message == f"INFO keepafloatd::vip: bound {vip}/32 on {iface}":
                    raise ValueError(f"VIP rebound after isolation: {vip}")
            if not released:
                raise ValueError(f"missing original VIP release: {vip}")
        return "terminal permission expiry and original VIP cleanup confirmed"
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
        if sys.argv[1:] == ["leader-text"]:
            print(leader_transition(sys.stdin))
        elif len(sys.argv) == 3 and sys.argv[1] == "physical-id":
            print(physical_id(sys.argv[2]))
        else:
            mode, boot, invocation, after, *targets = sys.argv[1:]
            print(verify(mode, sys.stdin, boot, invocation, int(after), *targets))
    except EvidencePending:
        sys.exit(2)
    except (ValueError, TypeError) as error:
        print(f"invalid scenario evidence: {error}", file=sys.stderr)
        sys.exit(1)
