"""Mutual HMAC-SHA256 handshake matching src/auth.rs. No record protection."""
import hashlib
import hmac
import json
import secrets
import struct

MAGIC = b"KAFDAUTH"
VERSION = 3
DOMAIN = b"keepafloatd-mutual-auth\0"
HELLO = struct.Struct("!8sBBBBQQ32sB16sB32s")
OPERATIONS = frozenset(("status", "pre_vote", "append_entries", "install_snapshot", "vote"))


def encode_rpc(operation, payload):
    if operation not in OPERATIONS:
        raise ValueError("unknown RPC operation")
    return json.dumps({operation: payload}, separators=(",", ":")).encode()


def _unique_fields(pairs):
    fields = {}
    for key, value in pairs:
        if key in fields:
            raise ValueError("duplicate RPC field")
        fields[key] = value
    return fields


def decode_rpc(body, operation):
    envelope = json.loads(body, object_pairs_hook=_unique_fields)
    if (operation not in OPERATIONS or not isinstance(envelope, dict)
            or len(envelope) != 1 or operation not in envelope):
        raise ValueError("unexpected RPC operation")
    return envelope[operation]


def read_exact(stream, count):
    result = bytearray()
    while len(result) < count:
        part = stream.recv(count - len(result))
        if not part:
            raise EOFError("authentication connection closed")
        result.extend(part)
    return bytes(result)


def hello(node, target, listener, role, epoch=None, supports_v2=True, boot_nonce=None):
    if boot_nonce is not None and len(boot_nonce) != 32:
        raise ValueError("boot nonce must contain exactly 32 bytes")
    return HELLO.pack(MAGIC, VERSION, listener, role, 6 | int(supports_v2),
                      node, target, secrets.token_bytes(32), int(epoch is not None),
                      (epoch or 0).to_bytes(16, "big"), int(boot_nonce is not None),
                      boot_nonce if boot_nonce is not None else bytes(32))


def receive(stream, listener, role, target):
    prefix = read_exact(stream, 8)
    if prefix != MAGIC:
        raise ValueError("legacy authentication rejected")
    version_byte = read_exact(stream, 1)
    if version_byte != bytes((VERSION,)):
        raise ValueError("unsupported authentication version")
    raw = prefix + version_byte + read_exact(stream, HELLO.size - 9)
    fields = HELLO.unpack(raw)
    (magic, version, channel, sender_role, flags, node, destination, nonce,
     present, epoch, boot_present, boot_nonce) = fields
    if (version != VERSION or channel != listener or sender_role != role
            or flags not in (6, 7) or destination != target or present not in (0, 1)
            or (not present and epoch != bytes(16))
            or boot_present not in (0, 1)
            or (not boot_present and boot_nonce != bytes(32))):
        raise ValueError("invalid authentication metadata")
    return raw, node


def proof(secret, role, client, server):
    if isinstance(secret, str):
        secret = secret.encode()
    if not secret:
        raise ValueError("missing cluster secret")
    return hmac.new(secret, DOMAIN + bytes((VERSION, role)) + client + server,
                    hashlib.sha256).digest()


def verify(stream, secret, role, client, server):
    if not hmac.compare_digest(read_exact(stream, 32), proof(secret, role, client, server)):
        raise ValueError("authentication proof mismatch")


def client(stream, node, target, secret, listener=1, boot_nonce=None):
    request = hello(node, target, listener, 1, boot_nonce=boot_nonce)
    stream.sendall(request)
    response, peer = receive(stream, listener, 2, node)
    if peer != target:
        raise ValueError("authentication responder mismatch")
    verify(stream, secret, 2, request, response)
    stream.sendall(proof(secret, 1, request, response))
    verify(stream, secret, 3, request, response)
    return peer


def server(stream, node, secret, listener=1, boot_nonce=None):
    request, peer = receive(stream, listener, 1, node)
    response = hello(node, peer, listener, 2, boot_nonce=boot_nonce)
    stream.sendall(response + proof(secret, 2, request, response))
    verify(stream, secret, 1, request, response)
    stream.sendall(proof(secret, 3, request, response))
    return peer
