"""Safe socketpair checks for the public wire helper."""
import socket
import threading
import unittest
from unittest.mock import patch

import auth_wire as auth


class AuthenticationTests(unittest.TestCase):
    def test_short_previous_version_hello_is_rejected_without_waiting(self):
        a, b = socket.socketpair()
        with a, b:
            a.settimeout(0.1)
            b.settimeout(0.1)
            a.sendall(b"KAFDAUTH\x02")
            with self.assertRaises(ValueError):
                auth.receive(b, 1, 1, 2)

    def test_requires_version_three_and_exact_boot_field(self):
        self.assertEqual(auth.VERSION, 3)
        self.assertEqual(auth.HELLO.size, 110)

    def test_boot_presence_is_canonical_and_every_byte_is_authenticated(self):
        boot = bytes(range(32))
        raw = auth.hello(1, 2, 1, 1, boot_nonce=boot)
        self.assertEqual(raw[77], 1)
        self.assertEqual(raw[78:], boot)
        absent = auth.hello(1, 2, 1, 1)
        self.assertEqual(absent[77:], bytes(33))
        for present, stored in ((0, boot), (2, bytes(32))):
            malformed = bytearray(raw)
            malformed[77] = present
            malformed[78:] = stored
            a, b = socket.socketpair()
            with a, b:
                a.settimeout(1)
                b.settimeout(1)
                a.sendall(malformed)
                with self.assertRaises(ValueError):
                    auth.receive(b, 1, 1, 2)
        for invalid in (b"", bytes(31), bytes(33)):
            with self.assertRaises(ValueError):
                auth.hello(1, 2, 1, 1, boot_nonce=invalid)
        tag = auth.proof(b"test-key", 1, raw, absent)
        for offset in range(77, 110):
            changed = bytearray(raw)
            changed[offset] ^= 1
            self.assertNotEqual(tag, auth.proof(b"test-key", 1, changed, absent))

    def test_tagged_status_vector_and_strict_operation(self):
        vector = b'{"status":{"probe_from":2}}'
        self.assertEqual(auth.encode_rpc("status", {"probe_from": 2}), vector)
        self.assertEqual(auth.decode_rpc(vector, "status"), {"probe_from": 2})
        for body in (
                b'{"probe_from":2}', b'{"other":{}}',
                b'{"status":{},"status":{}}', b'{"status":{},"vote":{}}',
                b'{"vote":{}}', b'[]', b'null'):
            with self.subTest(body=body), self.assertRaises(ValueError):
                auth.decode_rpc(body, "status")

    def test_all_operation_envelopes(self):
        for operation in ("status", "pre_vote", "append_entries", "install_snapshot", "vote"):
            vector = ('{"' + operation + '":{"value":1}}').encode()
            self.assertEqual(auth.encode_rpc(operation, {"value": 1}), vector)
            self.assertEqual(auth.decode_rpc(vector, operation), {"value": 1})
        with self.assertRaises(ValueError):
            auth.encode_rpc("unknown", {})
        with self.assertRaises(ValueError):
            auth.decode_rpc(b'{"unknown":{}}', "unknown")

    def test_version_one_rejected_for_both_roles_and_listeners(self):
        for listener in (1, 2):
            for role in (1, 2):
                a, b = socket.socketpair()
                with a, b:
                    a.settimeout(1)
                    b.settimeout(1)
                    legacy = bytearray(auth.hello(1, 2, listener, role))
                    legacy[8] = 1
                    a.sendall(legacy)
                    with self.assertRaises(ValueError):
                        auth.receive(b, listener, role, 2)

    def test_mutual_both_listeners(self):
        for listener in (1, 2):
            a, b = socket.socketpair()
            with a, b:
                a.settimeout(1)
                b.settimeout(1)
                result = []
                def accept():
                    result.append(auth.server(b, 2, b"test-key", listener))
                worker = threading.Thread(target=accept)
                worker.start()
                self.assertEqual(auth.client(a, 1, 2, b"test-key", listener), 2)
                worker.join(2)
                self.assertFalse(worker.is_alive())
                self.assertEqual(result, [1])

    def test_transcript_role_and_nonce_binding(self):
        with patch.object(auth.secrets, "token_bytes", return_value=bytes([1]) * 32):
            client = auth.hello(1, 2, 1, 1, 9)
        with patch.object(auth.secrets, "token_bytes", return_value=bytes([2]) * 32):
            server = auth.hello(2, 1, 1, 2)
        tag = auth.proof(b"test-key", 1, client, server)
        self.assertEqual(tag.hex(), "d7ea2643ac34c4c244ab7f3aef92bb05b505edd9267fca83286fe456050635f7")
        for role in (2, 3):
            self.assertNotEqual(tag, auth.proof(b"test-key", role, client, server))
        for offset in range(len(client)):
            modified = bytearray(client)
            modified[offset] ^= 1
            self.assertNotEqual(tag, auth.proof(b"test-key", 1, modified, server))
        self.assertEqual(auth.HELLO.size, 110)
        self.assertNotIn(b"test-key", client + server + tag)

    def test_wrong_key_and_legacy_fail(self):
        a, b = socket.socketpair()
        with a, b:
            a.sendall(bytes(8))
            with self.assertRaises(ValueError):
                auth.server(b, 2, b"test-key")
        with self.assertRaises(ValueError):
            auth.proof(b"", 1, b"", b"")


if __name__ == "__main__":
    unittest.main()
