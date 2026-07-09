# Operations guide

Running, observing, changing and troubleshooting a keepAfloatD cluster in production.
For the config field reference see [`config.example.yaml`](../config.example.yaml); for the
design see [architecture.md](architecture.md).

## Running and lifecycle

The packaged `systemd` unit is instanced — one instance per config file:

```bash
sudo systemctl enable --now keepafloatd@node1   # reads /etc/keepafloatd/config-node1.yaml
sudo systemctl status keepafloatd@node1
sudo systemctl restart keepafloatd@node1
sudo systemctl stop keepafloatd@node1
```

`stop`, `Ctrl+C` and `SIGTERM` all drive the same graceful path: the node unbinds every VIP it
holds and hands ownership off before it exits, so a planned stop does **not** black-hole its VIPs.
On the next start the daemon first reclaims any address a previous crashed instance may have left on
the interface, then rejoins Raft — so an ungraceful kill is recovered symmetrically.

The daemon needs `CAP_NET_ADMIN` (for `ip addr add|del`) and, for gratuitous ARP, `CAP_NET_RAW` —
both are granted by the packaged unit; running as root also works.

## Observing state

- **Logs:** `RUST_LOG=info` for normal operation, `RUST_LOG=keepafloatd=debug` to trace every bind,
  unbind, election and health transition. With `systemd`: `journalctl -u keepafloatd@node1 -f`.
- **Who holds a VIP:** the VIP is a real secondary address, so ask the kernel:
  `ip addr show | grep <vip>` on each node — exactly one node should have it.
- A node logs `bound <vip>` when it takes a VIP and `unbound <vip>` when it releases one.

## Changing the VIP list or config

The cluster-wide fields (`peers`, `vips`, `health.interval_ms`, `health.stale_secs`,
`cluster_secret`, `max_frame_bytes`, `failback`) **must be identical on every node**. To add or
remove a VIP:

1. Edit the `vips` list in the config on **every** node.
2. Restart the daemon on each node, one at a time (see rolling upgrade below).

Only `node_id` and the local listen addresses legitimately differ per host.

## Rolling upgrade / maintenance

Upgrade or reconfigure one node at a time so the cluster keeps quorum throughout:

1. `systemctl stop keepafloatd@nodeX` — its VIPs fail over to the survivors within seconds.
2. Upgrade the binary / edit the config.
3. `systemctl start keepafloatd@nodeX` — it rejoins via Raft and VIPs rebalance evenly.
4. Confirm health (`journalctl`, `ip addr`) before moving to the next node.

Never take down a majority at once, or the cluster loses quorum and every node unbinds its VIPs
until quorum returns.

## Security and networking

- **Shared secret is required.** Every node must set the same non-empty `cluster_secret`; the daemon
  refuses to start without it. It authenticates both the Raft and the submit channel.
- **Sender binding.** A node may only submit state for itself — the leader checks the connection's
  source address against the claimed node's advertised address, so a compromised peer cannot forge
  another node's health.
- **Bind to concrete addresses.** `raft_listen` and `client_submit_listen` must be the
  peer-reachable addresses advertised in `peers` (a wildcard `0.0.0.0` bind is rejected).
- **Firewall the two TCP ports** (`raft_listen`, `client_submit_listen`, e.g. `7000`/`7001`) to the
  cluster peers only.
- Keep `/etc/keepafloatd/config.yaml` readable only by the service account — it holds the secret.
- The v1 transport is plain TCP/JSON; for hostile networks layer VPN/IPsec/mTLS around it.

## Troubleshooting

**The daemon exits immediately with `cluster_secret is required`.**
Set a non-empty `cluster_secret` (the same on every node). This is the secure-by-default guard.

**The cluster never forms / no leader is elected.**
A majority of nodes must be mutually reachable on their `raft_listen` addresses. Check: the ports
are open between peers, every node lists the **same** `peers` roster, and each node's `raft_listen`
matches its own entry in `peers`. `RUST_LOG=keepafloatd=debug` shows the election attempts.

**A submit is rejected with "came from … but its advertised address is …".**
The node's source IP doesn't match its `client_submit_address` in `peers`. This happens with NAT or
multi-homed hosts between nodes; put the nodes on a flat, directly-reachable segment (the v1 model),
or advertise the address the node actually egresses from.

**A VIP doesn't move off a failed node.**
The old holder is only dropped once its committed probe rounds go stale (`health.stale_secs`) or it
loses quorum. Check the survivors still have quorum, and that `health.stale_secs` isn't set so high
that the window is minutes.

**A recovered node never gets VIPs back.**
With `failback: false` (nopreempt) a node that lost its VIPs to a health failure stays ineligible
until the whole cluster is cold-reformed — this is by design. Use `failback: true` (with
`failback_delay_secs`) if you want recovered nodes to re-enter the pool automatically.

**The health check flaps.**
Make sure `health.timeout_ms` is comfortably below `health.interval_ms`, and that the health command
exits `0` only when the service is truly ready. A probe that forks a lingering child is fine — the
drain is time-bounded — but a slow probe near the timeout will flap.
