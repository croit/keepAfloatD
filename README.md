# keepAfloatD

**Standalone Rust daemon** for Keepalived-like virtual IP failover using **OpenRaft** instead of
VRRP. One process reads **one** YAML file: **one** Raft cluster, **one** health-check definition,
**one** shared VIP list.

**Scope (v1):** a single config must not describe multiple independent failover groups. If you
need isolation, run multiple daemon instances with separate configs, ports and Raft clusters.
Multiple VIPs inside one cluster are supported and are distributed round-robin across healthy
members.

**Documentation:** [docs/operations.md](docs/operations.md) for running and troubleshooting a
cluster (operators), [docs/development.md](docs/development.md) for building, testing and the e2e
harness (contributors), [ARCHITECTURE.md](ARCHITECTURE.md) for the design.

## Build

```bash
cargo build --release
```

Binary: `target/release/keepafloatd` (default config path `config.yaml`; override with
`--config` / `-c`).

The container image keeps the binary default unchanged and sets its own default config path with
`-c /etc/keepafloatd/config.yaml`. The packaged `systemd` template uses
`/etc/keepafloatd/config-%i.yaml`; the shipped `/etc/keepafloatd/config.yaml` is a sample config
you can copy to an instance-specific name such as `/etc/keepafloatd/config-node1.yaml` and start
with `systemctl enable --now keepafloatd@node1`.

## Architecture

```text
┌──────────────────────────────────────────────────────────────────────────┐
│ Raft log                                                                │
│ - HealthUpdate { node_id, healthy }                                     │
│ - VipReleased { node_id, vip, generation }                              │
├──────────────────────────────────────────────────────────────────────────┤
│ Committed state machine                                                 │
│ - per-node health + committed probe rounds                              │
│ - vip -> VipAssignment { holder, generation, previous_holder, ... }     │
│ - deterministic multi-VIP rebalancing                                   │
└──────────────────────────────────────────────────────────────────────────┘
           ▲                                                │
           │ submit_request / forward-to-leader             │ apply
           │                                                ▼
┌────────────────────┐                              ┌──────────────────────┐
│ Health script      │                              │ VIP bind / unbind    │
│ local async check  │                              │ ip addr, arping      │
└────────────────────┘                              └──────────────────────┘
```

**Layers:** Raft replicates health observations plus old-holder release acknowledgements. The
state machine records the committed owner of every VIP and fences ownership changes with a per-VIP
generation. Locally, the daemon runs a Keepalived-style command on an interval and binds Linux
addresses only when both the local gates and the committed handoff fence allow it.

### When is a VIP bound on this host?

All of these must be true:

1. The cluster has a **current leader**.
2. The local **health check** succeeds.
3. The node is still **consensus-fresh**: its most recent submit to Raft succeeded.
4. Committed state maps this `node_id` as the VIP's **holder**.
5. The VIP's activation fence has opened:
   the previous holder either committed `VipReleased` for the current generation or is already
   ineligible/stale.

If any one of these becomes false, the daemon must not keep the VIP and will unbind it on the next
reconcile tick.

### Multi-VIP distribution

VIPs are sorted by address. Eligible nodes are voter members whose last-reported health is `true`
and whose most recent committed probe round is within the configured stale window of the cluster's
latest committed probe round. Eligible nodes are sorted by id and VIPs are assigned round-robin in
that order. When health or membership changes, the committed holder map rebalances deterministically
on every node.

## Failover behavior

Three independent mechanisms force this node off a VIP:

- **Local health fails, times out or cannot execute:** `local_healthy` flips to `false`; the next
  reconcile tick unbinds every VIP currently held here. The same `HealthUpdate { healthy: false }`
  is submitted to Raft so ownership can move.
- **Consensus freshness is lost:** if a health or release submit cannot be committed, the local
  `consensus_fresh` gate flips to `false` immediately. This node will unbind all VIPs even if it
  still has a stale local idea of the leader.
- **Ownership changes:** if the previous holder is still eligible, the replacement waits for a
  committed `VipReleased` acknowledgement from that previous holder before binding. If the previous
  holder is already unhealthy/stale, the replacement waits one extra committed probe round before
  activating.

### Silent holder death / partition

If a holder dies or is partitioned before it can publish `healthy: false`, survivors keep
committing probe rounds while that node's committed round stops advancing. Once the lag exceeds the
configured stale window, the old holder is removed from eligibility and the VIP is reassigned.
Because the isolated node also loses `consensus_fresh` as soon as submits fail, it self-fences and
releases the VIP instead of keeping a stale bind alive indefinitely.

### Crash and stop symmetry

- On startup, [`vip::LocalVip::startup_cleanup`] removes every configured VIP from its interface
  (best effort). This reclaims an address left behind by a previous crashed instance before the new
  process rejoins Raft.
- On `SIGINT` or `SIGTERM`, the daemon stops the reconcile loop, unbinds every VIP it still owns,
  then shuts down submit/Raft. The sample `systemd` unit uses the normal `SIGTERM` stop path.

## Requirements

- Linux (`ip`; optional `arping`).
- Privileges for `ip addr add|del`: typically root or `CAP_NET_ADMIN`. The sample
  `deploy/systemd/keepafloatd.service` also grants `CAP_NET_RAW` for some `arping`
  implementations.
- Rust edition 2024 toolchain (1.87+).

## Tests

```bash
cargo test
```

Unit tests cover:

- YAML normalization and validation.
- Eligibility / staleness filtering from committed probe rounds.
- Round-robin multi-VIP assignment.
- Assignment generation / previous-holder fencing.
- Cluster-secret validation on both transports.
- Divergent applied-index scenarios proving the release gate keeps the active binder count at `<= 1`
  under the tested failure cases.

`cargo test` does not spin up multiple real processes on real interfaces, but the core ownership,
handoff and exclusivity rules are exercised directly in unit tests.

For CI smoke coverage, the repository also ships a dry-run multi-node harness:

```bash
cargo build --release
bash scripts/ci/e2e-dry-run.sh
```

It starts three local daemon processes with temporary configs, validates deterministic VIP
distribution, forces a health-driven rebalance, and verifies graceful `SIGTERM` takeover without
touching real host addresses.

## Running E2E tests

The repository also ships a Docker Compose harness for real end-to-end failover on a private
bridge network. It starts three `keepafloatd` containers plus an `e2e-runner` probe container on
`10.50.0.0/24`, with VIPs `10.50.0.100` through `10.50.0.102` claimed inside that bridge only.

Build the image locally, then point Compose at it:

```bash
docker build -t keepafloatd:dev .
KEEPAFLOATD_IMAGE=keepafloatd:dev docker compose up -d
```

Run the scenario suite:

```bash
bash tests/e2e/scripts/run.sh
```

Tear everything down cleanly:

```bash
docker compose down -v
```

The E2E fixtures live under `tests/e2e/`:

- `configs/` contains the 3 static node configs consumed by the Compose harness
- `scripts/health.sh` is the toggleable local probe used to flip one node unhealthy
- `scenarios/` contains the 13 failover scenarios (steady state, holder death, leader death, local
  unhealthy, minority partition, graceful SIGINT, restart/rejoin, full-outage majority recovery,
  concurrent cold start, returning nodes joining a survivor, and survivor rejoin after a leadership
  change, sticky VIP placement, and stale-survivor rejection after cluster reform)

`tests/e2e/scripts/run.sh` resets the Compose stack between scenarios, waits for steady state, and
runs every scenario (continuing past failures). It captures per-scenario logs under
`e2e-artifacts/compose/` (including each run's `scenario.out`) and writes an aggregated
`e2e-artifacts/report.md` summarizing every scenario's PASS/FAIL plus a short failure excerpt, so a
failed run can be triaged from one file. It uses only `KEEPAFLOATD_IMAGE` for local and CI parity.

## CI/CD

GitHub Actions runs formatting, unit, documentation, license, coverage, package, container and E2E
checks. The release workflow builds static amd64 and arm64 binaries, DEB and RPM packages, a
vendored source archive and multi-architecture container images.

Published releases are available from [GitHub Releases](https://github.com/croit/keepAfloatD/releases),
with runtime images at `ghcr.io/croit/keepafloatd`.

## Container image

Build the host-architecture image locally:

```bash
docker build -t keepafloatd:dev .
```

Run it with a mounted config and the capabilities needed for VIP management:

```bash
docker run --rm \
  --cap-add=NET_ADMIN \
  --cap-add=NET_RAW \
  -v "$(pwd)/config.example.yaml:/etc/keepafloatd/config.yaml:ro" \
  keepafloatd:dev
```

Notes:

- The runtime image runs as uid/gid `10001` (`keepafloatd`).
- `CAP_NET_ADMIN` is required for `ip addr add|del`; `CAP_NET_RAW` is needed when `arping` is
  used.
- No ports are exposed in the image; publish the configured Raft/submit ports explicitly.
- A read-only root filesystem is recommended. `/var/lib/keepafloatd` is reserved for future
  writable state.
- The runtime image is intentionally minimal and does not ship `bash`; use `/bin/sh`, simple
  binaries already in the image, or mount an external health-check script if needed.

## Manual multi-node (same host)

Use `examples/node1.yaml`, `examples/node2.yaml`, `examples/node3.yaml`. Only `node_id` and the
listen addresses differ.

```bash
./target/release/keepafloatd -c examples/node1.yaml
./target/release/keepafloatd -c examples/node2.yaml
./target/release/keepafloatd -c examples/node3.yaml
```

The cluster forms automatically regardless of start order and without any special node: each node
probes its peers and, once a majority is reachable and no cluster yet exists, every node calls
`Raft::initialize` with the identical roster (safe per OpenRaft) and Raft elects one leader. Any
majority can form - or, after a full outage, recover - the cluster, even if the lowest-id node is
down.

Examples use `dry_run: true` and `interface: lo` so you can exercise Raft without touching real
addresses.

## Configuration

See `config.example.yaml`.

| Field | Meaning |
|---|---|
| `node_id` | Stable id for this process; must appear in `peers`. No node is special - any majority forms the cluster. |
| `raft_listen` | Address this node listens on for Raft RPC. Must match `peers[node_id].raft_address`. |
| `client_submit_listen` | Address where the leader accepts forwarded `HealthUpdate` and `VipReleased` requests. Must match `peers[node_id].client_submit_address`. |
| `peers` | Peer list (`id`, `raft_address`, `client_submit_address`). Must be identical on every member. |
| `vips` | VIP list (`address`, `interface`). `address` accepts an optional CIDR suffix (`10.0.0.101/24`); without one the VIP is bound as a host route (`/32` for IPv4, `/128` for IPv6). Order is normalized by sorting addresses; duplicates (by IP) are deduped. |
| `health.command` | Executable + args (`execv` style), e.g. `["/bin/sh","-c","curl -sf http://127.0.0.1/"]`. |
| `health.interval_ms` / `timeout_ms` | Probe period and per-run wall timeout (kill on expiry -> unhealthy). |
| `health.stale_secs` | Maximum time a node may stop contributing committed probe rounds before it becomes ineligible. Must be `>= ceil(interval_ms / 1000)`. Defaults to `max(3, ceil(interval_ms / 1000) * 3)`. |
| `cluster_secret` | Optional shared secret for authenticating both Raft handshakes and submit envelopes. |
| `max_frame_bytes` | TCP frame size cap (defaults to 4 MiB; minimum 64 KiB). |
| `submit_timeout_ms` | Wall-clock cap on one submit attempt, including local leader writes and follower->leader forwards (defaults to 2000 ms). |
| `raft` | Optional OpenRaft timing knobs (`election_timeout_{min,max}_ms`, `heartbeat_interval_ms`). |
| `dry_run` | Log intended `ip` operations without mutating the host. |

All nodes must use the **same** `peers`, `vips`, `health.interval_ms`, `health.stale_secs`,
`cluster_secret` and `max_frame_bytes`.

## Health checks

Exit code `0` means healthy. Non-zero exit, timeout or spawn failure means unhealthy.

```yaml
health:
  command: ["/bin/sh", "-c", "curl -sf http://127.0.0.1:8080/health >/dev/null"]
  interval_ms: 2000
  timeout_ms: 3000
  stale_secs: 10
```

```yaml
health:
  command: ["/bin/sh", "-c", "pgrep -x myservice >/dev/null"]
  interval_ms: 1000
  timeout_ms: 2000
  stale_secs: 6
```

## Operations

- **Logs:** `RUST_LOG=info` or `RUST_LOG=debug` (for example `RUST_LOG=keepafloatd=debug`).
- **Stop:** `Ctrl+C`, `kill -TERM`, and `systemctl stop` all drive the same graceful shutdown path.
- **Restart safety:** the next start always runs `startup_cleanup` before rejoining Raft.

## Security

This daemon has two TCP attack surfaces (Raft RPC and the leader submit listener). v1 hardens them
as follows:

- **Bind addresses:** bind only to the concrete peer-reachable address advertised for this node in
  `peers`; wildcard binds like `0.0.0.0` do not pass config validation.
- **Cluster secret:** when `cluster_secret` is set, every Raft handshake and every submit envelope
  must carry the same secret or the request is dropped.
- **Frame-size cap:** `max_frame_bytes` rejects oversized frames before allocating.
- **RPC / submit timeouts:** half-open peers cannot block heartbeats or submit forwarding forever.
- **Per-peer locking:** one slow peer cannot serialize heartbeats to every other peer.

For networks outside one trusted host or one trusted segment, layer VPN/IPsec/mTLS around this v1
transport.

## Limitations (v1)

- **In-memory Raft storage:** cluster state is not persisted to disk.
- **No dynamic membership API:** `peers` are static config.
- **No mTLS:** `cluster_secret` improves baseline safety but does not replace real transport
  security.

## Rustdoc

```bash
cargo doc --no-deps --open
```

Public and non-trivial internal items are documented with `//!` / `///`, including config, Raft
ownership logic, health execution, release fencing, and VIP lifecycle behavior.

## Related

- **Keepalived:** VRRP-based; this tool keeps script-style health checks but elects ownership via
  Raft.

## License

`keepafloatd` uses:

- `GNU AGPL v3` for open-source/community usage
- a commercial license available from `croit.io`

Contributor policy:

- external contributions require either a signed Contributor License Agreement (`CLA`) or a
  copyright assignment accepted by croit GmbH before merge

See `LICENSE`, `LICENSES/AGPL-3.0.txt`, `LICENSES/COMMERCIAL.md`, and `CONTRIBUTING.md`.

## Third-Party Licenses

The current `Cargo.lock` advertises only permissive third-party license families:
`MIT`, `Apache-2.0`, `BSD-2-Clause`, `BSL-1.0`, `Unicode-3.0`, `Unlicense`, and `Zlib`
(including mixed expressions such as `MIT OR Apache-2.0` and `Apache-2.0 WITH LLVM-exception`).

The checked-in inventory lives in `THIRD_PARTY_LICENSES.md`, and CI enforces the dependency
license allowlist with `cargo deny check licenses`. That inventory covers the Rust/Cargo dependency
graph and does not attempt to enumerate Debian base-image packages.
