# Operations guide

Running, observing, changing and troubleshooting a keepAfloatD cluster in production.
For the config field reference see [`config.example.yaml`](../config.example.yaml); for the
design see [ARCHITECTURE.md](../ARCHITECTURE.md).

## Running and lifecycle

The packaged `systemd` unit is instanced - one instance per config file:

```bash
sudo install -o root -g root -m 0600 \
  /etc/keepafloatd/config.yaml /etc/keepafloatd/config-node1.yaml
CLUSTER_SECRET="$(openssl rand -hex 32)"
sudo sed -i \
  "s/replace-me-with-a-random-32-byte-string/${CLUSTER_SECRET}/" \
  /etc/keepafloatd/config-node1.yaml
unset CLUSTER_SECRET
sudo systemctl enable --now keepafloatd@node1   # reads /etc/keepafloatd/config-node1.yaml
sudo systemctl status keepafloatd@node1
sudo systemctl restart keepafloatd@node1
sudo systemctl stop keepafloatd@node1
```

`stop`, `Ctrl+C` and `SIGTERM` all drive the same graceful path: the node unbinds every VIP it
holds and hands ownership off before it exits, so a planned stop does **not** black-hole its VIPs.
On the next start the daemon first reclaims any address a previous crashed instance may have left on
the interface, then rejoins Raft - so an ungraceful kill is recovered symmetrically.

`SIGHUP` requests a graceful restart: the daemon releases its VIPs and exits
with status 1. The packaged `Restart=on-failure` policy starts a new process,
which reads the configuration again. This is not an in-place reload.
Without a supervisor, the daemon stays stopped until started manually.
Use `systemctl stop` for a deliberate stop without automatic restart.

The daemon needs `CAP_NET_ADMIN` (for `ip addr add|del`) and, for gratuitous ARP, `CAP_NET_RAW`.
Both are granted by the packaged unit; running as root also works.

## Observing state

- **Logs:** `RUST_LOG=info` for normal operation, `RUST_LOG=keepafloatd=debug` to trace every bind,
  unbind, election and health transition. With `systemd`: `journalctl -u keepafloatd@node1 -f`.
- **Who holds a VIP:** the VIP is a real secondary address, so ask the kernel:
  `ip addr show | grep <vip>` on each node - exactly one node should have it.
- A node logs `bound <vip>` when it takes a VIP and `unbound <vip>` when it releases one.

## Changing the VIP list or config

The effective cluster-wide settings must agree on every node: `peers`, resolved `vips`,
`health.interval_ms`, the missed-probe threshold derived from `health.stale_secs`,
`failover_delay_secs`, `failback`, `cluster_secret`, `max_frame_bytes`, and Raft timing.
`failback_delay_secs` must also agree when `failback: true`; nopreempt ignores it. To add or remove
a VIP:

1. Edit the `vips` list in the config on **every** node.
2. Restart the daemon on each node, one at a time (see rolling upgrade below).

`node_id`, local listen addresses, health command/timeout, submit timeout, dry-run mode, and notify
hook may differ per host.

## Coordinated upgrade for retained-holder fencing

When upgrading from a version that does not retain the previous VIP holder
across an ownerless gap, upgrade all voters together. This changes how
committed entries produce assignments. Mixed-version operation is not
supported, even if the cluster keeps quorum. Plan a maintenance window
with an interruption of VIP service.

1. Stop every old daemon with `systemctl stop keepafloatd@nodeX` on its
   node, or stop its supervisor so it cannot restart automatically.
2. Verify that every old daemon has stopped and every configured VIP is
   absent from all nodes. Check both IPv4 and IPv6 addresses with
   `ip -4 addr show` and `ip -6 addr show`. Do not start a new daemon while
   any old daemon or its VIPs remain.
3. Replace the binary on every voter with the same new version. Check
   `keepafloatd --version` and verify that cluster-wide settings agree
   across all configs before starting any voter.
4. Start the upgraded daemons. Once a majority is running, confirm that
   the cluster forms, health checks pass and each VIP has exactly one
   holder before ending the maintenance window.

Decoding an old snapshot does not make a rolling upgrade safe: an old
snapshot taken during an ownerless gap lacks the previous holder's
identity. See [Upgrading ownerless-gap fencing](../README.md#upgrading-ownerless-gap-fencing).

## Rolling upgrade / maintenance

Use the following procedure only for compatible binary versions. It does
not apply to the retained-holder upgrade described above. For compatible
maintenance, work on one node at a time so the cluster keeps quorum:

1. `systemctl stop keepafloatd@nodeX` - its VIPs fail over to the survivors within seconds.
2. Upgrade the binary / edit the config.
3. `systemctl start keepafloatd@nodeX` - it rejoins via Raft. VIPs rebalance evenly when
   `failback: true`; nopreempt nodes remain available for future orphaned VIPs without taking from
   healthy holders.
4. Confirm health (`journalctl`, `ip addr`) before moving to the next node.

During rolling maintenance, never take down a majority at once, or the cluster loses quorum and
every node unbinds its VIPs until quorum returns. The coordinated upgrade above deliberately
interrupts VIP service instead of running incompatible versions together.

The compatibility safeguards below do not make the retained-holder upgrade safe to roll.

During an upgrade from a binary with legacy failback semantics, the cluster stays on legacy
behavior until every configured voter is online and advertises support for the corrected policy.
Activation is then committed once through Raft. A fresh all-new majority may activate immediately;
an older node that was offline during that reform is fenced and must be upgraded before rejoining.
Pre-existing Legacy recovery and nopreempt entries are retained at activation because the old state
does not record whether each failed node owned a VIP. This preserves real delay/nopreempt guarantees;
ownership-aware tracking applies to failures committed after activation.

Current binaries also exchange a non-secret fingerprint of every cluster-wide setting before the
first Raft frame. Concrete mismatches are rejected immediately. Existing clusters require missing
legacy identities only after every configured voter is online with the same fingerprint and the
leader commits activation. Until that log line appears, an old binary cannot describe its config,
so continue comparing cluster-wide fields manually during the mixed-version window. After
activation, a mismatched node logs `cluster configuration mismatch`, stops reconciliation, unbinds
its locally tracked VIPs, shuts down Raft, exits with status 4, and must have its config repaired
before the service restart can rejoin. Only one coherent foreign fingerprint can trigger this
fence; unrelated mismatches do not add into a false majority.

## Security and networking

- **Shared secret is required.** Every node must set the same non-empty `cluster_secret`; the daemon
  refuses to start without it. It authenticates both the Raft and the submit channel.
- **Sender binding.** A node may only submit state for itself - the leader checks the connection's
  source address against the claimed node's advertised address, so a compromised peer cannot forge
  another node's health.
- **Bind to concrete addresses.** `raft_listen` and `client_submit_listen` must be the
  peer-reachable addresses advertised in `peers` (a wildcard `0.0.0.0` bind is rejected).
- **Firewall the two TCP ports** (`raft_listen`, `client_submit_listen`, e.g. `7000`/`7001`) to the
  cluster peers only.
- Keep `/etc/keepafloatd/config*.yaml` root-owned and mode `0600` - these files hold the secret.
  The DEB and RPM post-install scripts enforce this for regular files without following symlinks.
- The v1 transport is plain TCP/JSON; for hostile networks layer VPN/IPsec/mTLS around it.

## Troubleshooting

**The daemon exits immediately with `cluster_secret is required`.**
Set a non-empty `cluster_secret` (the same on every node). This is the secure-by-default guard.

**The daemon exits with `replace cluster_secret placeholder`.**
Generate one random secret, replace the public example value on every node, and retain mode `0600`.
For example: `openssl rand -hex 32`.

**The daemon exits with `unknown field` or `raft.heartbeat_interval_ms (...) must be <`.**
Every key in the config is checked at load: a misspelled key (for example `stale_sec` instead of
`stale_secs`) is rejected by name instead of silently falling back to its default, and the Raft
timing relations (`heartbeat_interval_ms < election_timeout_min_ms < election_timeout_max_ms`,
all non-zero) are enforced before any host address is touched. Fix the named key and restart.

**The cluster never forms / no leader is elected.**
A majority of nodes must be mutually reachable on their `raft_listen` addresses. Check: the ports
are open between peers, every node lists the **same** `peers` roster, and each node's `raft_listen`
matches its own entry in `peers`. `RUST_LOG=keepafloatd=debug` shows the election attempts.

**The service repeatedly exits with status 4 / `cluster configuration mismatch`.**
This node's canonical cluster-wide settings differ from a roster majority. Compare `peers`, VIP
address/prefix/interface/VLAN data, health interval/staleness, failover/failback policy,
`max_frame_bytes`, and Raft timing. Repair the config and restart. The log prints only fingerprints,
never the shared secret.

**Logs show `connection limit reached`, `handshake timeout` or `frame read timed out`.**
Both listeners admit at most 32 unauthenticated and 64 authenticated connections in total and at
most 8 per configured peer address; every handshake, frame read and response write is bounded to
five seconds, and an idle authenticated Raft stream to the larger of five seconds or twice the
heartbeat interval. These limits are compile-time constants, not configuration. Repeated hits from
a peer address point at a reconnect storm or a stuck client on that host; connections from any
other address are rejected before admission, so seeing them means the listener is reachable from
outside the roster.

**A submit is rejected with "came from … but its advertised address is …".**
The node's source IP doesn't match its `client_submit_address` in `peers`. This happens with NAT or
multi-homed hosts between nodes; put the nodes on a flat, directly-reachable segment (the v1 model),
or advertise the address the node actually egresses from.

**A VIP doesn't move off a failed node.**
For an explicit probe failure, check `failover_delay_secs`; the failure must remain continuous for
that duration. For silent death, the old holder is dropped only when its committed probe-round lag
strictly exceeds `health.stale_secs`. Also check that the survivors still have quorum.

**A recovered node never gets VIPs back.**
With `failback: false` (nopreempt), a recovered node does not take VIPs from healthy holders. It
remains eligible when a holder fails and an orphan needs placement. Use `failback: true` with
`failback_delay_secs` if recovered capacity should also trigger proactive even rebalancing. A node
whose first probe fails during startup may enter the pool after its first successful probe.

**The health check flaps.**
Make sure `health.timeout_ms` is comfortably below `health.interval_ms`, and that the health command
exits `0` only when the service is truly ready. A probe that forks a lingering child is fine - the
drain is time-bounded - but a slow probe near the timeout will flap.
