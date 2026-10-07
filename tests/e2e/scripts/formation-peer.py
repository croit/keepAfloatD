"""A discovery-only peer that disappears before sending any Raft log."""

import auth_wire
import hashlib
import hmac
import json
import pathlib
import secrets
import socket
import struct
import sys
import threading

BOOT = secrets.token_bytes(32)
REPLICA = f"{1:016x}:{BOOT.hex()}"
HISTORY = [f"{node:016x}:{secrets.token_hex(32)}" for node in (1, 2, 3)]
FINGERPRINTS = {}


def encode(value):
    return json.dumps(value, separators=(",", ":")).encode()


def record_tag(secret, role, sender, recipient, binding, payload):
    body = encode([binding, payload])
    transcript = b"keepafloatd-admission-record\0" + bytes((3, 1, role))
    for replica in (sender, recipient):
        physical, boot = replica.split(":")
        transcript += int(physical, 16).to_bytes(8, "big") + bytes.fromhex(boot)
    transcript += len(body).to_bytes(8, "big") + body
    return hmac.new(secret, transcript, hashlib.sha256).digest()


def handshake(stream, secret):
    request, peer = auth_wire.receive(stream, 1, 1, 1)
    response = auth_wire.hello(1, peer, 1, 2, boot_nonce=BOOT)
    stream.sendall(response + auth_wire.proof(secret, 2, request, response))
    auth_wire.verify(stream, secret, 1, request, response)
    stream.sendall(auth_wire.proof(secret, 3, request, response))
    fields = auth_wire.HELLO.unpack(request)
    replica = f"{peer:016x}:{fields[-1].hex()}" if fields[-2] else None
    binding = hashlib.sha256(b"keepafloatd-admission-channel\0" + bytes((3,))
                             + request + response).digest()
    return peer, replica, list(binding)


def management(request, secret, peer, replica, binding):
    if (request["sender"] != replica or request["recipient"] != REPLICA
            or request["binding"] != binding):
        raise ValueError("management boot or channel mismatch")
    payload = request["payload"]
    # Re-serialize the typed fields in the Rust struct's declaration order.
    payload = {"nonce": payload["nonce"], "action": payload["action"]}
    expected = record_tag(secret, 12, replica, REPLICA, binding, payload)
    if not hmac.compare_digest(bytes(request["tag"]), expected):
        raise ValueError("management signature mismatch")
    action = payload["action"]
    if isinstance(action, dict) and set(action) == {"PrepareJoin"}:
        # A verified follow-up proves the daemon consumed our signed history.
        pathlib.Path(f"/shared/formation-discovered-{peer}").touch()
    if action != "Discover":
        return None  # This peer supplies history, never permission or a Raft log.
    result = {"nonce": payload["nonce"], "result": {"Discovery": {
        "genesis": {"config": FINGERPRINTS[peer], "epoch": 1, "voters": HISTORY},
        "voters": HISTORY,
    }}}
    record = {"sender": REPLICA, "recipient": replica, "binding": binding,
              "payload": result,
              "tag": list(record_tag(secret, 13, REPLICA, replica, binding, result))}
    return encode({"admission_control": record})


def read_exact(stream, count):
    data = bytearray()
    while len(data) < count:
        chunk = stream.recv(count - len(data))
        if not chunk:
            raise EOFError("peer disconnected")
        data.extend(chunk)
    return bytes(data)


def answer(stream, secret):
    with stream:
        stream.settimeout(1)
        try:
            node_id, replica, binding = handshake(stream, secret)
            if node_id not in (2, 3):
                raise ValueError("unexpected peer")
            size = struct.unpack("!I", read_exact(stream, 4))[0]
            if size > 65536:
                raise ValueError("oversized discovery request")
            envelope = json.loads(read_exact(stream, size), object_pairs_hook=auth_wire._unique_fields)
            if set(envelope) == {"admission_control"}:
                response = management(envelope["admission_control"], secret, node_id, replica, binding)
                if response is not None:
                    stream.sendall(struct.pack("!I", len(response)) + response)
                return
            if set(envelope) != {"status"}:
                return
            request = envelope["status"]
            if request.get("probe_from") != node_id:
                raise ValueError("expected status probe, never a Raft RPC")
            FINGERPRINTS[node_id] = request["config_fingerprint"]
            response = auth_wire.encode_rpc("status", {
                "initialized": True,
                "current_leader": HISTORY[0],
                "member_count": 3,
                "cluster_epoch": 1,
                "config_fingerprint": request["config_fingerprint"],
                "supports_failover_semantics_v2": True,
                "supports_config_identity_v1": True,
                "config_identity_enforced": False,
            })
            stream.sendall(struct.pack("!I", len(response)) + response)
        except (OSError, EOFError):
            pass  # Reconnect workers may abandon an unused stream.
        except (ValueError, KeyError) as error:
            print(f"invalid discovery request: {error}", flush=True)


address, secret = sys.argv[1:]
host, port = address.rsplit(":", 1)
with socket.socket() as server:
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, int(port)))
    server.listen()
    server.settimeout(0.2)
    pathlib.Path("/shared/formation-ready").touch()
    while not pathlib.Path("/shared/formation-stop").exists():
        try:
            stream, _ = server.accept()
        except TimeoutError:
            continue
        threading.Thread(target=answer, args=(stream, secret.encode()), daemon=True).start()
pathlib.Path("/shared/formation-stopped").touch()
