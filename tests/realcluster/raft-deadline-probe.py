#!/usr/bin/env python3
"""Exercise authenticated Raft read deadlines without writing cluster state or exposing secrets."""

import json
import select
import socket
import struct
import sys
import time

import yaml


def endpoint(value):
    host, port = value.rsplit(":", 1)
    return host.strip("[]"), int(port)


def read_exact(stream, size):
    result = bytearray()
    while len(result) < size:
        part = stream.recv(size - len(result))
        if not part:
            raise RuntimeError("authenticated status connection closed unexpectedly")
        result.extend(part)
    return bytes(result)


def status(stream, node_id, fingerprint):
    body = json.dumps({
        "probe_from": node_id,
        "config_fingerprint": fingerprint,
        "supports_cancellation_safe_rpc_v1": True,
    }).encode()
    stream.sendall(struct.pack("!I", len(body)) + body)
    size = struct.unpack("!I", read_exact(stream, 4))[0]
    if size > 65536:
        raise RuntimeError("status response exceeds its protocol cap")
    response = json.loads(read_exact(stream, size))
    if not response.get("initialized") or response.get("current_leader") is None:
        raise RuntimeError("target does not report an initialized cluster with a leader")
    return response


def run(config_path, target_id, status_only=False):
    with open(config_path, encoding="utf-8") as source:
        config = yaml.safe_load(source)
    node_id = config["node_id"]
    if node_id == target_id:
        raise ValueError("probe must use a different configured peer's source identity")
    peers = {peer["id"]: peer for peer in config["peers"]}
    source_host, _ = endpoint(peers[node_id]["raft_address"])
    target_host, target_port = endpoint(peers[target_id]["raft_address"])
    heartbeat = config.get("raft", {}).get("heartbeat_interval_ms", 250) / 1000
    idle_timeout = max(5, 2 * heartbeat)
    if idle_timeout > 30:
        raise ValueError("heartbeat exceeds this bounded test's 30-second idle budget")
    secret = config["cluster_secret"].encode()
    handshake = struct.pack("!QI", node_id, len(secret)) + secret + b"\x02"

    def connect():
        family = socket.AF_INET6 if ":" in source_host else socket.AF_INET
        stream = socket.socket(family, socket.SOCK_STREAM)
        stream.settimeout(2)
        try:
            stream.bind((source_host, 0))
            stream.connect((target_host, target_port))
            stream.sendall(handshake)
            return stream
        except BaseException:
            stream.close()
            raise

    with connect() as discovery:
        discovered = status(discovery, node_id, None)
        fingerprint = discovered.get("config_fingerprint")
    if not fingerprint:
        raise RuntimeError("target must advertise config identity for authenticated pressure")
    if discovered.get("member_count") != len(peers) or discovered["current_leader"] not in peers:
        raise RuntimeError("target reports a different voter roster or an unknown leader")
    if status_only:
        print(json.dumps({"source_id": node_id, "target_id": target_id,
                          "leader": discovered["current_leader"], "member_count": len(peers)}), flush=True)
        return

    streams = {}
    try:
        # Start the idle socket last so earlier authentication round trips do not consume its
        # measured idle budget. Frame budgets begin when their first bytes are sent below.
        for mode in ("prefix", "body", "trickle", "idle"):
            stream = connect()
            streams[mode] = stream
            status(stream, node_id, fingerprint)
        started = time.monotonic()
        streams["prefix"].sendall(b"\x00")
        streams["body"].sendall(struct.pack("!I", 65536) + b"x")
        streams["trickle"].sendall(b"\x00")
        pending = dict(streams)
        elapsed = {}
        trickled = False
        deadline = started + idle_timeout + 2
        while pending and time.monotonic() < deadline:
            now = time.monotonic()
            if not trickled and now - started >= 3:
                if "trickle" not in pending:
                    raise RuntimeError("trickled frame was rejected before exercising its deadline")
                streams["trickle"].sendall(b"\x00\x00\x10x")
                trickled = True
            ready, _, _ = select.select(list(pending.values()), [], [], .2)
            for mode, stream in list(pending.items()):
                if stream not in ready:
                    continue
                try:
                    data = stream.recv(1)
                except (ConnectionResetError, BrokenPipeError):
                    data = b""
                if data:
                    raise RuntimeError(f"unexpected data on stalled {mode} connection")
                elapsed[mode] = time.monotonic() - started
                minimum = idle_timeout if mode == "idle" else 5
                if elapsed[mode] < minimum - .5:
                    raise RuntimeError(f"{mode} closed before its expected deadline")
                if elapsed[mode] > minimum + 1.5:
                    raise RuntimeError(f"{mode} exceeded its deadline")
                del pending[mode]
                print(f"{mode}: server closed at {elapsed[mode]:.2f}s", flush=True)
        if pending:
            raise RuntimeError(f"deadline failed to reclaim: {', '.join(pending)}")
        with connect() as recovered:
            response = status(recovered, node_id, fingerprint)
        print(json.dumps({"result": "pass", "source_id": node_id, "target_id": target_id,
                          "closed_seconds": elapsed, "leader": response["current_leader"]}), flush=True)
    finally:
        for stream in streams.values():
            stream.close()


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4) or (len(sys.argv) == 4 and sys.argv[3] != "--status-only"):
        raise SystemExit("usage: raft-deadline-probe.py CONFIG TARGET_NODE_ID [--status-only]")
    run(sys.argv[1], int(sys.argv[2]), len(sys.argv) == 4)
