"""Pure fixtures for discovery records, without listeners or daemon processes."""
import ast
import copy
import pathlib
import unittest
from unittest.mock import patch

source = pathlib.Path(__file__).with_name("formation-peer.py")
tree = ast.parse(source.read_text())
definitions = [item for item in tree.body
               if isinstance(item, (ast.Import, ast.ImportFrom, ast.FunctionDef))
               or (isinstance(item, ast.Assign)
                   and all(isinstance(target, ast.Name) and target.id.isupper()
                           for target in item.targets))]
fixture = {"__name__": "formation_fixture"}
exec(compile(ast.Module(body=definitions, type_ignores=[]), str(source), "exec"), fixture)


class DiscoveryRecords(unittest.TestCase):
    def setUp(self):
        self.secret = b"local-discovery-fixture-secret-012345"
        self.sender = f"{2:016x}:{'04' * 32}"
        self.binding = [8] * 32
        fixture["FINGERPRINTS"][2] = {"version": 1, "digest": [6] * 32}
        payload = {"nonce": [7] * 32, "action": "Discover"}
        self.request = {"sender": self.sender, "recipient": fixture["REPLICA"],
                        "binding": self.binding, "payload": payload,
                        "tag": list(fixture["record_tag"](
                            self.secret, 12, self.sender, fixture["REPLICA"],
                            self.binding, payload))}

    def answer(self, request):
        with patch.object(pathlib.Path, "touch"):
            return fixture["management"](request, self.secret, 2, self.sender, self.binding)

    def test_discovery_binds_challenge_boots_and_channel(self):
        reply = fixture["json"].loads(self.answer(self.request))["admission_control"]
        self.assertEqual(reply["payload"]["nonce"], self.request["payload"]["nonce"])
        self.assertEqual(reply["recipient"], self.sender)
        self.assertEqual(reply["binding"], self.binding)
        self.assertEqual(reply["tag"], list(fixture["record_tag"](
            self.secret, 13, fixture["REPLICA"], self.sender,
            self.binding, reply["payload"])))
        self.assertEqual(reply["payload"]["result"]["Discovery"]["genesis"]["voters"], fixture["HISTORY"])

    def test_tampering_never_produces_a_response(self):
        for field in ("sender", "recipient", "binding", "nonce", "tag"):
            request = copy.deepcopy(self.request)
            if field in ("sender", "recipient"):
                request[field] = f"{3:016x}:{'09' * 32}"
            elif field == "nonce":
                request["payload"]["nonce"][0] ^= 1
            else:
                request[field][0] ^= 1
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.answer(request)

    def test_valid_non_discovery_never_grants_or_mutates(self):
        request = copy.deepcopy(self.request)
        request["payload"]["action"] = "PrepareJoin"
        request["tag"] = list(fixture["record_tag"](
            self.secret, 12, self.sender, fixture["REPLICA"], self.binding, request["payload"]))
        self.assertIsNone(self.answer(request))


if __name__ == "__main__":
    unittest.main()
