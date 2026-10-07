# Mutual cluster authentication

Both TCP listeners require the same versioned HMAC-SHA256 handshake before
status, Raft or submit frames. The configured secret is never transmitted.
Protocol version 3 binds exact boot identities and adds runtime admission
to explicitly tagged requests and responses. Earlier versions are rejected, with
no negotiation or fallback. Upgrades require a full-cluster stop,
verification that all VIPs are released, and restart of every member on
the same version. Include offline members before they can rejoin.

## Security boundary

This proves possession of a shared cluster key, not exclusive ownership of
an individual node identity. Node IDs are bound to the transcript and checked
against the configured roster and source address. Submit payload IDs must
also equal the authenticated initiator. Raft retains its existing payload
validation and incarnation checks. Same-IP key holders are not independent
cryptographic identities.

All records remain plaintext. Admission, admission-management and release records
have a role-specific MAC bound to the connection, exact boots and request
challenge. Ordinary Raft RPCs and status replies do not have a per-record
MAC, encryption, authenticated sequencing or RPC replay defense.
An active intermediary can transparently relay a valid handshake and then
read, modify, inject or replay later messages, including configuration
fingerprints and capability assertions in status replies. Authenticated hello
metadata does not authenticate those later JSON records. Use an authenticated
encrypted VPN, IPsec or mTLS tunnel on untrusted paths. Mutual authentication
does not make an untrusted network safe.

Recorded proofs support offline key guessing. Use a unique, high-entropy
random cluster secret; the 32-byte minimum alone does not ensure entropy.
Proof verification uses RustCrypto `hmac` 0.12 with `sha2` 0.10 and its
constant-time `verify_slice` API, not a handmade MAC or comparison.
`getrandom` uses the operating system entropy source and fails closed.

## Fixed encoding

All integers are unsigned big-endian. A hello is exactly 110 bytes.

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 8 | ASCII `KAFDAUTH` |
| 8 | 1 | Protocol version, 3 |
| 9 | 1 | Listener, 1 Raft/status or 2 submit |
| 10 | 1 | Sender role, 1 initiator or 2 responder |
| 11 | 1 | Capabilities: bit 0 failover V2, bit 1 config identity, bit 2 cancellation-safe RPC |
| 12 | 8 | Sender node ID |
| 20 | 8 | Intended recipient node ID |
| 28 | 32 | Fresh OS-generated nonce |
| 60 | 1 | Epoch present, 0 or 1 |
| 61 | 16 | Epoch, all zero when absent |
| 77 | 1 | Boot nonce present, 0 or 1 |
| 78 | 32 | Exact process boot nonce, all zero when absent |

Only capability bytes 6 and 7 are supported. Unknown versions, listeners,
roles, flags, noncanonical epochs and wrong destination IDs fail closed.
Raft and admitted release requests advertise their current epoch when
known. Discovery may legitimately have no epoch. The responder ID must
match the client's configured destination.

For exact hello bytes C and S, proof(role) is the full 32-byte HMAC-SHA256:

```text
HMAC(secret, b"keepafloatd-mutual-auth\0" || [3, role] || C || S)
```

The protocol order is:

1. Initiator sends C with a fresh nonce.
2. Responder sends S with its own fresh nonce, then proof(2).
3. Initiator verifies proof(2), then sends proof(1).
4. Responder verifies proof(1), sends proof(3), and may dispatch.
5. Initiator verifies proof(3) before sending any application frame.

Both nonces, both identities, both epochs, all flags and listener/version
fields are included in every proof. Distinct proof roles prevent reflection;
listener binding prevents cross-protocol proof reuse. Every new connection
requires new nonces, including reconnects and status probes. An old proof
does not match a fresh transcript. There is no nonce cache: replay resistance
depends on fresh independent 256-bit OS nonces and a strong key. This is
handshake replay resistance only.

A single five-second deadline covers each handshake, including all partial
reads and writes. Existing outbound/RPC deadlines may be shorter and still
bound the entire operation. Handshake memory has fixed-size buffers and
pre-authentication connection quotas remain in force. Cancellation drops
the owning connection; no proof or nonce is reused after a failed attempt.
No error or log includes a key, proof or complete transcript.

The public Python helper `tests/e2e/scripts/auth_wire.py` uses the same
encoding with standard-library `hmac.compare_digest` and OS nonces. Its
callers must set a socket timeout; production Rust uses a whole-exchange
deadline. Harness helpers only target explicitly authorized test peers.

## Tagged Raft and status frames

After the version-3 handshake, each frame is a four-byte unsigned
big-endian length followed by JSON. The JSON object has exactly one
operation key. Both the request and its response use the same key:

| Operation key | Request payload | Response payload |
| --- | --- | --- |
| `status` | Cluster status probe | Cluster status and capabilities |
| `pre_vote` | Read-only election probe | Pre-Vote response |
| `append_entries` | Raft log entries and commit position | Append result |
| `install_snapshot` | Vote, snapshot metadata and bytes | Snapshot result |
| `vote` | Raft election request | Vote response |
| `admission` | Signed permission challenge | Signed grant for the same challenge |
| `admission_control` | Signed discovery or management action | Signed result bound to the request |

For example, a minimal status request from node 2 is
`{"status":{"probe_from":2}}`. Normal peers also supply the configuration
fingerprint and stream capabilities in the status payload. The envelope
does not replace those checks or the authenticated sender binding.

Unknown, duplicate or multiple operation keys, unwrapped legacy messages
and malformed payloads are rejected. A reply for a different operation
also fails the exchange, even when its inner payload has a compatible
shape. Failed exchanges discard the stream. No alternate operation is
tried after a decode failure. Integer fields retain their full width,
including 128-bit cluster epochs.

Pre-Vote is mandatory in this version. Missing capability support does
not synthesize a grant or enable a legacy election fallback. The submit
listener uses the same version-3 authentication boundary. Shared-key
possession alone does not grant permission for an admitted cluster write.

Frame-size caps, memory reservations and whole-operation deadlines still
apply. Tags identify the operation; they do not authenticate or encrypt
the record and do not change the security boundary described above.

Admission uses MAC roles 10 and 11; admission management uses roles 12
and 13. Release forwarding uses roles 14 and 15. Each record binds the
exact authenticated hello transcript and sender and recipient boots.
Admission and management also bind their outstanding request challenge.
The lease deadline is measured from the challenge start, not receipt
time. See [runtime admission](runtime-admission.md) for permission and
restart semantics.
