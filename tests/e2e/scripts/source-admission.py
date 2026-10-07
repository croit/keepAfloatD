"""Check separate source phases using status-only sockets from a stopped test peer."""

import auth_wire
import contextlib
import socket
import struct
import sys


def read_exact(stream, count):
    data = bytearray()
    while len(data) < count:
        chunk = stream.recv(count - len(data))
        if not chunk:
            raise EOFError("status connection closed")
        data.extend(chunk)
    return bytes(data)


def status(stream, peer_id, fingerprint=None):
    body = auth_wire.encode_rpc("status", {
        "probe_from": peer_id,
        "config_fingerprint": fingerprint,
        "supports_cancellation_safe_rpc_v1": True,
    })
    stream.sendall(struct.pack("!I", len(body)) + body)
    size = struct.unpack("!I", read_exact(stream, 4))[0]
    if size > 65536:
        raise AssertionError("oversized status response")
    response = auth_wire.decode_rpc(read_exact(stream, size), "status")
    if not response.get("initialized"):
        raise AssertionError("target lost its initialized cluster")
    return response


def assert_closed(stream):
    try:
        response = stream.recv(1)
    except (ConnectionResetError, BrokenPipeError):
        return
    if response:
        raise AssertionError("excess connection returned unexpected data")


def main():
    source, target, peer_id, target_id, secret = sys.argv[1:]
    host, port = target.rsplit(":", 1)
    peer_id = int(peer_id)
    secret = secret.encode()
    target_id = int(target_id)
    with contextlib.ExitStack() as sockets:
        def connect():
            stream = sockets.enter_context(socket.socket())
            stream.settimeout(1)
            stream.bind((source, 0))
            stream.connect((host, int(port)))
            return stream

        probe = connect()
        auth_wire.client(probe, peer_id, target_id, secret)
        fingerprint = status(probe, peer_id)["config_fingerprint"]
        if fingerprint is None:
            raise AssertionError("target did not advertise its configuration identity")
        probe.shutdown(socket.SHUT_WR)
        assert_closed(probe)

        def authenticate():
            stream = connect()
            auth_wire.client(stream, peer_id, target_id, secret)
            status(stream, peer_id, fingerprint)
            return stream

        authenticated = [authenticate()]
        pending = [connect() for _ in range(7)]
        authenticated.append(authenticate())
        print("authenticated sockets do not occupy handshake slots", flush=True)
        authenticated.extend(authenticate() for _ in range(6))

        # The eighth pending socket proves that promotion released its pre-auth slot.
        pending.append(connect())
        assert_closed(connect())
        # Even with the pre-auth phase full, established sockets must keep serving.
        for stream in authenticated:
            status(stream, peer_id, fingerprint)

        # A valid handshake cannot bypass the independent eight-slot authenticated cap.
        rejected = pending.pop()
        auth_wire.client(rejected, peer_id, target_id, secret)
        assert_closed(rejected)
        replacement = connect()
        auth_wire.client(replacement, peer_id, target_id, secret)
        assert_closed(replacement)
        retired = authenticated.pop()
        retired.shutdown(socket.SHUT_WR)
        assert_closed(retired)
        authenticated.append(authenticate())
        for stream in authenticated:
            status(stream, peer_id, fingerprint)
        print("both source phases reject excess sockets without leaking capacity", flush=True)


if __name__ == "__main__":
    main()
