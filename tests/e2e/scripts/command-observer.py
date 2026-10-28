#!/usr/bin/env python3
"""Bounded, read-only PID-namespace sampling; observations are not exit events."""
import argparse
import errno
import json
import math
import os
from pathlib import Path
import re
import resource
import select
import signal
import sys
import time

INTERVAL = 0.020
LIMIT = 2 * 1024 * 1024
RESERVE = 16384
MAX_TRACKED = 256
MAX_FRAME_THREADS = 16
FIELDS = {"ppid": 1, "minflt": 7, "majflt": 9, "utime_ticks": 11,
          "stime_ticks": 12, "threads": 17, "starttime": 19, "rss_pages": 21}


def read(path, limit=4096):
    with path.open(encoding="utf-8", errors="replace") as stream:
        text = stream.read(limit + 1)
    if len(text) > limit:
        raise ValueError("proc_read_limit")
    return text.strip()


def parse_stat(text):
    left, right = text.find("("), text.rfind(")")
    if left < 1 or right <= left:
        raise ValueError("malformed_stat")
    fields = text[right + 1:].split()
    if len(fields) < 22 or fields[0] not in "RSDZTtXxKWPI" or len(fields[0]) != 1:
        raise ValueError("malformed_stat")
    sample = {key: int(fields[index]) for key, index in FIELDS.items()}
    sample.update(pid=int(text[:left].strip()), state=fields[0])
    if sample["pid"] <= 0 or any(value < 0 for value in sample.values() if isinstance(value, int)):
        raise ValueError("malformed_stat")
    return text[left + 1:right], sample


def stat(path):
    try:
        item = parse_stat(read(path / "stat"))
        if item[1]["pid"] != int(path.name):
            raise ValueError("stat_pid_mismatch")
        return item
    except OSError as error:
        if error.errno in (errno.ENOENT, errno.ESRCH):
            return None
        raise


def identity(sample):
    return sample["pid"], sample["starttime"]


def optional(path, name):
    try:
        text = read(path / name)
        if name == "schedstat":
            values = [int(value) for value in text.split()]
            if len(values) != 3 or any(value < 0 for value in values):
                return {"unavailable": "malformed"}
            if not any(values):
                return {"unavailable": "zero_or_disabled"}
            return dict(zip(("cpu_ns", "runqueue_ns", "slices"), values))
        if text == "0" or not text:
            return {"unavailable": "zero_or_restricted"}
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]{0,79}", text):
            return {"unavailable": "unrecognized"}
        return {"symbol": text}
    except OSError as error:
        return {"unavailable": "read_error", "errno": error.errno}
    except ValueError:
        return {"unavailable": "malformed"}


def details(path, expected):
    values = {name: optional(path, name) for name in ("schedstat", "wchan")}
    after = stat(path)
    if after is None or identity(after[1]) != expected:
        return {"unavailable": "identity_changed_during_read"}
    return values


def observer_cgroup(proc_root):
    # Only a proven private cgroup namespace root identifies this observer.
    try:
        if proc_root != Path("/proc") or read(proc_root / "self/cgroup") != "0::/":
            return {"unavailable": "observer_cgroup_not_resolved"}
        if os.readlink(proc_root / "self/ns/cgroup") == os.readlink(proc_root / "1/ns/cgroup"):
            return {"unavailable": "observer_cgroup_namespace_not_private"}
        mounts = read(proc_root / "self/mountinfo", 65536).splitlines()
        if not any(line.split()[3:5] == ["/", "/sys/fs/cgroup"] and
                   " - cgroup2 " in line for line in mounts):
            return {"unavailable": "observer_cgroup_mount_not_resolved"}
        allowed = {"usage_usec", "user_usec", "system_usec", "nr_periods",
                   "nr_throttled", "throttled_usec"}
        values = {}
        for line in read(Path("/sys/fs/cgroup/cpu.stat")).splitlines():
            key, value = line.split()
            if key in allowed:
                values[key] = int(value)
        if not values or any(value < 0 for value in values.values()):
            raise ValueError("malformed_cgroup")
        return {"scope": "observer_cgroup_not_node", "counters": values}
    except (OSError, ValueError) as error:
        return {"unavailable": "read_error", "errno": getattr(error, "errno", None)}


def discover_roots(root, root_pid, excluded):
    processes, candidates, gaps = {}, [], 0
    paths = [root / str(root_pid)] if root_pid else root.iterdir()
    for count, path in enumerate(paths):
        if count >= 4096:
            raise ValueError("namespace_scan_limit")
        if not path.name.isdecimal() or int(path.name) == excluded:
            continue
        item = stat(path)
        if item is None:
            gaps += 1
            continue
        processes[item[1]["pid"]] = (path, item[1])
        if item and (root_pid or item[0] == "keepafloatd"):
            if len(candidates) >= MAX_TRACKED:
                raise ValueError("tracked_identity_limit")
            candidates.append(item[1]["pid"])
    if root_pid:
        return [(path, identity(sample)) for path, sample in processes.values()], gaps
    candidate_pids, roots = set(candidates), []
    for candidate in candidates:
        pid, lineage = candidate, {}
        while pid:
            if pid in lineage or pid not in processes or len(lineage) >= MAX_TRACKED:
                gaps += 1
                break
            if pid != candidate and pid in candidate_pids:
                break
            path, sample = processes[pid]
            lineage[pid] = (path, sample)
            pid = sample["ppid"]
        else:
            # Recheck ancestry so a reused PID or changed parent cannot select a root.
            for ancestor, (path, sample) in lineage.items():
                current = stat(path)
                if (current is None or identity(current[1]) != identity(sample) or
                        current[1]["ppid"] != sample["ppid"] or
                        (ancestor == candidate and current[0] != "keepafloatd")):
                    gaps += 1
                    break
            else:
                path, sample = processes[candidate]
                roots.append((path, identity(sample)))
    return roots, gaps


def snapshot(root, root_pid, excluded):
    roots, gaps = discover_roots(root, root_pid, excluded)
    pending = [(path, tag, None, tag, None) for path, tag in roots]
    result, seen = {}, set()
    while pending:
        path, expected, parent, daemon, parent_pid = pending.pop()
        item = stat(path)
        if (item is None or identity(item[1]) != expected or
                (parent_pid is not None and item[1]["ppid"] != parent_pid)):
            gaps += 1
            continue
        if expected in seen or expected[0] == excluded:
            continue
        seen.add(expected)
        try:
            for thread in (path / "task").iterdir():
                if not thread.name.isdecimal():
                    continue
                task = stat(thread)
                if task is None:
                    gaps += 1
                    continue
                comm, sample = task
                key = identity(sample)
                children = read(thread / "children").split()
                descendants = []
                for child in children:
                    if not child.isdecimal():
                        raise ValueError("malformed_children")
                    child_path = root / child
                    child_stat = stat(child_path)
                    if int(child) != excluded and child_stat:
                        if child_stat[1]["ppid"] != expected[0]:
                            gaps += 1
                            continue
                        if len(pending) + len(descendants) >= MAX_TRACKED:
                            raise ValueError("pending_identity_limit")
                        descendants.append((child_path, identity(child_stat[1]), key, daemon, expected[0]))
                after_task, after_process = stat(thread), stat(path)
                if (after_task is None or identity(after_task[1]) != key or
                        after_process is None or identity(after_process[1]) != expected):
                    gaps += 1
                    continue
                if key not in result and len(result) >= MAX_TRACKED:
                    raise ValueError("tracked_identity_limit")
                result[key] = {"stat": sample, "path": thread, "parent": parent,
                               "daemon": daemon, "role": "daemon" if parent is None else "child",
                               "kind": "ip" if comm == "ip" else "other"}
                pending.extend(descendants)
        except OSError as error:
            if error.errno not in (errno.ENOENT, errno.ESRCH):
                raise
            gaps += 1
    return result, gaps


def daemon_summary(current):
    totals = dict.fromkeys(("utime_ticks", "stime_ticks", "minflt", "majflt"), 0)
    states, scheduler = {}, {"cpu_ns": 0, "runqueue_ns": 0, "slices": 0}
    present = missing = 0
    for key, value in current.items():
        if value["role"] != "daemon":
            continue
        sample = value["stat"]
        for field in totals:
            totals[field] += sample[field]
        states[sample["state"]] = states.get(sample["state"], 0) + 1
        counters = details(value["path"], key).get("schedstat", {})
        if "cpu_ns" not in counters:
            missing += 1
        else:
            present += 1
            for field in scheduler:
                scheduler[field] += counters[field]
    return dict(stat_totals=totals, states=states, schedstat_totals=scheduler if present else None,
                schedstat_available=present, schedstat_unavailable=missing,
                scope="current_daemon_threads_lifetime_counters")


def daemon_frame(current, daemon, parent):
    threads = [(key, value) for key, value in current.items()
               if value["role"] == "daemon" and value["daemon"] == daemon]
    threads.sort(key=lambda item: (item[0] != parent, item[0]))
    return {"omitted_threads": max(0, len(threads) - MAX_FRAME_THREADS), "threads": [
        dict(identity=key, stat=value["stat"], details=details(value["path"], key))
        for key, value in threads[:MAX_FRAME_THREADS]]}


def self_usage():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return dict(self_cpu_s=usage.ru_utime + usage.ru_stime,
                self_peak_rss_kib=usage.ru_maxrss / (1024 if sys.platform == "darwin" else 1))


class Output:
    def __init__(self):
        self.used = 0
        self.fd = sys.stdout.fileno()
        os.set_blocking(self.fd, False)

    def emit(self, event, final=False, **fields):
        record = dict(event=event, monotonic_ns=time.monotonic_ns(),
                      realtime_ns=time.time_ns(), **fields)
        data = (json.dumps(record, separators=(",", ":"), ensure_ascii=True) + "\n").encode()
        if self.used + len(data) > LIMIT - (0 if final else RESERVE):
            raise ValueError("output_budget")
        deadline = time.monotonic() + 0.1
        while data:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([], [self.fd], [], remaining)[1]:
                raise TimeoutError("output_backpressure")
            try:
                written = os.write(self.fd, data)
            except BlockingIOError:
                continue
            self.used += written
            data = data[written:]


def observe(args, output):
    stopped = [0]
    for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(number, lambda received, frame: stopped.__setitem__(0, received))
    start = previous = time.monotonic()
    heartbeat = start
    deadline = start + args.duration
    tracked, daemon_tags = {}, set()
    polls = gaps = late_samples = 0
    last_gap_ns = None
    max_gap = 0.0
    ready = False
    reason, error_type, error_number = "duration", None, None
    try:
        output.emit("start", interval_ms=20, duration_s=args.duration, max_tracked=MAX_TRACKED,
                    output_limit=LIMIT, clock_ticks=os.sysconf("SC_CLK_TCK"),
                    sampling_may_miss_children=True, observed_age_is_not_exec_duration=True)
        while not stopped[0] and time.monotonic() < deadline:
            now = time.monotonic()
            gap = now - previous
            max_gap = max(max_gap, gap)
            if gap > 2 * INTERVAL:
                late_samples += 1
                last_gap_ns = int(now * 1e9)
            previous = now
            current, lost = snapshot(args.proc_root, args.root_pid, os.getpid())
            gaps += lost
            polls += 1
            tags = {value["daemon"] for value in current.values()}
            if tags != daemon_tags:
                output.emit("daemon_change", previous=sorted(daemon_tags), current=sorted(tags))
                daemon_tags = tags
            if tags and not ready:
                output.emit("ready", daemons=sorted(tags), identities=len(current))
                ready = True
            for key in tracked.keys() - current.keys():
                old = tracked[key]
                if old["stage"]:
                    output.emit("child_disappeared", identity=key, parent=old["parent"],
                                daemon=old["daemon"], last_seen_ns=old["last_seen_ns"],
                                last_stat=old["stat"], meaning="not_observed_not_exit_proof")
            following = {}
            for key, value in current.items():
                if value["role"] != "child":
                    continue
                old = tracked.get(key)
                if old is None or (old["parent"], old["daemon"]) != (value["parent"], value["daemon"]):
                    old = dict(first=now, first_stat=value["stat"], stage=0)
                age = now - old["first"]
                stage = old["stage"]
                if stage < 2 and age >= (0.1, 0.2)[stage]:
                    output.emit("slow_child", identity=key, parent=value["parent"],
                                daemon=value["daemon"], kind=value["kind"], threshold_ms=(100, 200)[stage],
                                observed_age_ms=round(age * 1000, 3), stat=value["stat"],
                                first_stat=old["first_stat"], sample_gap_ms=round(gap * 1000, 3),
                                details=details(value["path"], key),
                                daemon_frame=daemon_frame(current, value["daemon"], value["parent"]))
                    stage += 1
                following[key] = dict(first=old["first"], first_stat=old["first_stat"], stage=stage,
                                      parent=value["parent"],
                                      daemon=value["daemon"], stat=value["stat"], last_seen_ns=int(now * 1e9))
            tracked = following
            if now >= heartbeat:
                output.emit("heartbeat", ready=ready, waiting_for_daemon=not tags, polls=polls,
                            identities=len(current), read_races=gaps, max_gap_ms=round(max_gap * 1000, 3),
                            late_samples=late_samples, last_gap_monotonic_ns=last_gap_ns,
                            daemon=daemon_summary(current), observer_cgroup=observer_cgroup(args.proc_root),
                            **self_usage())
                heartbeat = now + 5
            time.sleep(max(0, min(deadline - time.monotonic(), INTERVAL - (time.monotonic() - now))))
        if stopped[0]:
            reason = "signal"
    except (OSError, ValueError) as error:
        reason, error_type = "failure", type(error).__name__
        error_number = getattr(error, "errno", None)
        if isinstance(error, ValueError) and str(error) in ("output_budget", "tracked_identity_limit",
                                                         "pending_identity_limit", "namespace_scan_limit"):
            error_type = str(error)
    output.emit("final", final=True, reason=reason, ready=ready, signal=stopped[0],
                error=error_type, errno=error_number, polls=polls, read_races=gaps,
                max_gap_ms=round(max_gap * 1000, 3), late_samples=late_samples,
                last_gap_monotonic_ns=last_gap_ns, bytes_before_final=output.used, **self_usage())
    return 1 if reason == "failure" or not ready else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=1800)
    parser.add_argument("--root-pid", type=int)
    parser.add_argument("--proc-root", type=Path, default=Path("/proc"), help="fixture proc tree only")
    args = parser.parse_args()
    if not math.isfinite(args.duration) or not 0 < args.duration <= 1800:
        parser.error("duration must be positive and at most 1800 seconds")
    if args.root_pid is not None and (args.root_pid <= 0 or args.root_pid == os.getpid()):
        parser.error("root PID must identify another process")
    try:
        return observe(args, Output())
    except (OSError, ValueError):
        return 1


if __name__ == "__main__":
    sys.exit(main())
