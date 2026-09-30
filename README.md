# keepAfloatD

**Standalone Rust daemon** for Keepalived-like virtual IP failover using **OpenRaft** instead of
VRRP. One process reads **one** YAML file: **one** Raft cluster, **one** health-check definition,
**one** shared VIP list.

**Scope (v1):** a single config must not describe multiple independent failover groups. If you
need isolation, run multiple daemon instances with separate configs, ports and Raft clusters.
Multiple VIPs inside one cluster are supported. Cold-start placement is round-robin; subsequent
changes preserve healthy holders and move only the VIPs needed to restore an even spread.

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
whose public secret placeholder is intentionally invalid. Copy it to an instance-specific,
root-owned `0600` file, replace the placeholder with one random secret shared by every member,
then start the instance:

```bash
sudo install -o root -g root -m 0600 \
  /etc/keepafloatd/config.yaml /etc/keepafloatd/config-node1.yaml
CLUSTER_SECRET="$(openssl rand -hex 32)"
sudo sed -i \
  "s/replace-me-with-a-random-32-byte-string/${CLUSTER_SECRET}/" \
  /etc/keepafloatd/config-node1.yaml
unset CLUSTER_SECRET
sudo systemctl enable --now keepafloatd@node1
```

## Architecture

```text
┌──────────────────────────────────────────────────────────────────────────┐
│ Raft log                                                                │
│ - HealthUpdate { node_id, healthy }                                     │
│ - VipReleased { node_id, vip, generation }                              │
│ - ClusterFormed / one-way protocol activations                          │
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
5. Before the first local bind for this generation, the previous holder either committed
   `VipReleased` or stopped publishing probes long enough to become stale. A fresh
   `healthy: false` report proves the old process is still participating, so it does not bypass
   the release acknowledgement. Once this process safely activates the assigned VIP for that
   generation, a later recovery of the previous holder does not make the incumbent drop it.

If any one of these becomes false, the daemon must not keep the VIP and will unbind it on the next
reconcile tick.

### Multi-VIP distribution

VIPs are sorted by address. Eligible nodes are voter members whose last-reported health is `true`
and whose most recent committed probe round is within the configured stale window of the cluster's
latest committed probe round. When every eligible node is already known at the first assignment,
sorted VIPs go round-robin over nodes sorted by id. During a staggered start the state machine
recomputes on every committed health report, so the final mapping depends on the order in which
nodes became eligible; it is still balanced (maximum load difference of one) and identical on every
replica, because every node applies the same committed log. After that, healthy holders stay
sticky: orphaned VIPs go to the least-loaded eligible node and only the minimum additional VIPs
move to keep the maximum load difference at one.

## Failover behavior

Three independent mechanisms force this node off a VIP:

- **Local health fails, times out or cannot execute:** after the failure remains continuous for
  `failover_delay_secs`, `local_healthy` flips to `false`; the next reconcile tick unbinds every VIP
  currently held here. The same `HealthUpdate { healthy: false }` is submitted to Raft so ownership
  can move. The default delay is zero. A successful probe resets a pending delay, and startup is
  never made optimistic: a node that has not yet passed a probe is unhealthy immediately.
- **Consensus freshness is lost:** if a health or release submit cannot be committed, the local
  `consensus_fresh` gate flips to `false` immediately. This node will unbind all VIPs even if it
  still has a stale local idea of the leader.
- **Ownership changes:** while the previous holder keeps publishing fresh probes, the replacement
  waits for its committed `VipReleased` acknowledgement before binding, including when those probes
  explicitly report `healthy: false`. A nonzero kernel delete is accepted only when an exact-host
  query verifies that the address is already absent; an unreadable or still-present result keeps the
  handoff fenced. Only a previous holder whose committed probes have become stale may use the crash
  fallback, which waits one extra committed probe round before activating. After that safe local
  activation, a previous holder that recovers before learning the
  handoff cannot make the incumbent process drop the VIP again. A restarted incumbent still begins
  from the conservative first-bind fence, and activation proof from an older generation never
  carries into a new handoff. A temporary interval with no eligible owner does not reset this
  protection: the last assigned holder remains in volatile replicated state and Raft snapshots.
  A later assignment still requires that holder's release or the stale-holder fallback, including
  the extra activation round and local safety delay. If the same holder recovers, it first
  confirms local cleanup and acknowledges its own new assignment generation. Only the first
  assignment in a fresh cluster may activate immediately. A node publishes
  `VipReleased` only after its kernel unbind succeeds. When that release reaches an incumbent that
  already bound the handoff, the incumbent sends one more gratuitous ARP for that generation so
  cleanup of a crashed holder cannot leave neighbor caches pointing at the removed orphan.

### Silent holder death / partition

If a holder dies or is partitioned before it can publish `healthy: false`, survivors keep
committing probe rounds while that node's committed round stops advancing. Once the lag exceeds the
configured stale window, the old holder is removed from eligibility and the VIP is reassigned.
Because the isolated node also loses `consensus_fresh` as soon as submits fail, it self-fences and
releases the VIP instead of keeping a stale bind alive indefinitely. Consensus proof also
expires after the effective probe-staleness window plus half an activation-holdoff
round even if the next health check or release request is blocked. That bounded margin
allows normal probe-renewal jitter when the stale window equals one probe interval;
expiry cancels reconciliation work and withdraws local VIPs while leaving the daemon
running. A new successful health submit permits reconciliation again. Release acknowledgements
do not extend this health proof. The proof is timed from request start, so a delayed
acknowledgement cannot extend it (#26).

Health proof acknowledgements carry the committed log ID; a follower waits until its own
state machine has applied that entry before using the proof. During rolling upgrades, a new
submit-only request format prevents older leaders from accepting an unproven healthy report.
An upgraded follower instead reports unhealthy and remains fenced until a capable leader is
available. Fenced nodes still publish release acknowledgements so older voters can take over.

Committed probe rounds can arrive in a burst after slow probes or delayed writes. Therefore,
a replacement also waits one full proof lifetime plus a bounded cleanup allowance locally after observing an unacknowledged
stale takeover. Any observed advance of the previous holder's health tick resets that wait,
even when queued writes have already made it stale again. Explicit release acknowledgements
and first-ever assignments retain their immediate activation path; replicated eligibility
and activation-round checks still apply.

### Upgrading ownerless-gap fencing

The retained-holder rule changes how committed entries produce assignments. Upgrade all voters
together, not one at a time: stop every old daemon, verify that its VIPs have been removed, replace
all binaries, then start the cluster. This requires an interruption of VIP service. Mixed-version
operation is not supported for this change. Older snapshots can be decoded, but an older snapshot
taken while a VIP has no owner cannot reconstruct the holder identity it never recorded.

### Recovery and nopreempt

With `failback: true`, a recovered node re-enters placement after it has remained continuously
healthy for `failback_delay_secs`. With `failback: false`, it does not take a VIP from a healthy
holder merely to rebalance. It is still eligible for an orphaned VIP if a current holder later
fails; `nopreempt` suppresses proactive failback, not emergency failover capacity. Explicit health
failure and silent staleness build the same recovery history.

At the one-time Legacy-to-V2 activation boundary, existing Legacy recovery and nopreempt entries
are preserved conservatively. Legacy snapshots record node failures but not whether the node owned
a VIP, so reclassifying old entries would either shorten a real owner's delay or allow it to
preempt. Failures committed after activation use ownership-aware V2 tracking.

### Crash and stop symmetry

- Before every bind, keepafloatd records an inert `throw` host route in the dedicated policy table
  `10000 + address_protocol` (default table `10246`) with that route protocol. On startup,
  [`vip::LocalVip::startup_cleanup`] reads that table's numeric IPv4/IPv6 routes plus the address
  inventory and removes exact marked crash orphans together with every currently configured VIP.
  This also reclaims an orphan whose VIP was removed from YAML while the daemon was down. A marker
  matches an address only when that IP occurs on exactly one interface; ambiguous or malformed
  kernel state fails closed. Unmarked and differently marked addresses are never discovered for
  deletion, while configured VIPs are still cleaned for rolling compatibility with older versions.
  Failed address or marker deletion is accepted only after a bounded probe proves absence.
- On `SIGINT` or `SIGTERM`, the daemon stops the reconcile loop, unbinds every VIP it still owns,
  then shuts down submit/Raft. Each delete is bounded to 250 ms and retried up to three times; an
  exhausted cleanup is returned as a daemon error after every tracked VIP has been attempted. The
  sample `systemd` unit uses `KillMode=mixed`, so graceful SIGTERM reaches the daemon but not the
  teardown-time `ip addr del` children; remaining children are still killed at the stop timeout.
- `SIGHUP` runs the same cleanup, then exits with status 1 so the packaged
  `Restart=on-failure` policy restarts the daemon. Configuration is read by the new
  process, not reloaded in place. Signal listeners are registered before startup
  cleanup and cluster startup; signal setup or wait errors fail visibly.
- The submit listener is lifecycle-critical. Failure to bind its configured socket, an unexpected
  listener exit, or a task failure stops the composition root. Accepted submit connections and the
  Raft accept/reconnect/inbound task tree remain owned by their servers. Raft formation, epoch,
  activation, and stale-survivor control loops are also supervised and owned. Unexpected exit,
  error, or panic stops the daemon; shutdown aborts and joins the tasks before releasing sockets.
  Cleanup, network, Raft, and task errors reach the caller instead of leaving a VIP daemon running
  without its write path.
- Every daemon-owned command (`health`, notify hooks, `ip`, and `arping`) runs through one bounded
  Linux process-group runner. Normal completion, timeout, and async cancellation terminate the
  whole group and reap the direct child, so shell grandchildren cannot outlive the probe loop,
  reconciliation, or daemon shutdown.

## Requirements

- Linux with an `iproute2` version that supports numeric JSON address/route output and `throw` route
  protocols; this path is validated on the target EL9 5.14 kernel. Optional `arping`.
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
- Assignment generation / previous-holder fencing, including reassignment after an ownerless gap.
- Cluster-secret validation on both transports.
- Canonical cluster-config identity, legacy rollout, and mismatch fencing before Raft dispatch.
- Mixed-version RPC capability negotiation where V2 semantics do not imply cancellation-safe
  stream ownership.
- Five-node formation with a matching stale minority and a coherent foreign-config majority.
- Health-process cancellation/descendant cleanup and cross-peer endpoint collision validation.
- IPv4-mapped IPv6 endpoint canonicalization, so aliases such as
  `[::ffff:127.0.0.1]:9000` and `127.0.0.1:9000` cannot evade collision checks or config identity.
- Fresh-unhealthy handoff fencing, kernel-verified already-absent deletes, and composition-root
  supervision when the submit listener cannot start.
- Divergent applied-index scenarios proving the release gate keeps the active binder count at `<= 1`
  under the tested failure cases.
- Exhaustive health-transition sequences across immediate failback, delayed failback, and
  nopreempt policies, including deterministic replay and full handoff metadata invariants.

The CI coverage job rejects any production Rust file below 80% line coverage; aggregate coverage
cannot hide an under-tested module. The gate itself has passing, below-threshold, and empty-report
regressions. Public-export verification also compares the complete production Rust source set with
the export manifest, and its self-test proves that removing any source entry fails closed.

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
bash tests/e2e/scripts/run22.sh  # failover-delay and nopreempt regressions
bash tests/e2e/scripts/run26.sh  # mismatched config self-fence and repair
docker build --target runtime -t keepafloatd:runtime-local .
bash tests/haproxy-e2e/run.sh    # real process probe plus HTTP-through-VIP failover
```

Tear everything down cleanly:

```bash
docker compose down -v
```

The E2E fixtures live under `tests/e2e/`:

- `configs/` contains the 3 static node configs consumed by the Compose harness
- `configs22/` contains the isolated one-VIP timing/nopreempt configuration
- `configs26/` contains one deliberately mismatched node for config-identity fencing
- `scripts/health.sh` is the toggleable local probe used to flip one node unhealthy
- `scenarios/` contains the 16 failover and hardening scenarios (steady state, holder death,
  leader death, local
  unhealthy, minority partition, graceful SIGINT, restart/rejoin, full-outage majority recovery,
  concurrent cold start, returning nodes joining a survivor, and survivor rejoin after a leadership
  change, sticky VIP placement, stale-survivor rejection after cluster reform, bounded connection
  pressure, crash-time removal of a marked VIP while preserving an unmarked address, and SIGHUP
  cleanup with supervisor restart)
- `scenarios22/` covers delayed explicit failover and recovered-nopreempt orphan fallback
- `scenarios26/` proves a mismatched node exits VIP-less and rejoins after exact repair

The reusable [real-cluster harness](tests/realcluster/README.md) contains 35 scenarios, including
two legacy compatibility experiments with additional prerequisites. Its scenario code, assertions,
sanitized `env.example.sh`, and local self-test are versioned; `env.sh`, credentials, internal audit
notes, and generated evidence remain ignored. VIP and address-count assertions fail closed if any
node query fails or returns malformed data. Leader assertions accept evidence only from active
daemons, use each active node's latest observed transition, accept only configured ids, and require
majority agreement. Every unchecked scenario command failure terminates through baseline restore.
The campaign runner traps normal exit, interruption, and termination, then restores the exact
captured configs and binaries before returning the original status. CI parses every script, rejects
embedded private topology addresses, and runs the harness self-test with the example environment.
The same checks run against the exported public tree and packaged source archive. The scenario
inventory is checked against its README coverage table. These local checks do not run the live
cluster campaign or certify that every scenario passed on a release. Live runs require a
separately authorized disposable cluster; their logs and topology stay private.

`tests/e2e/scripts/run.sh` resets the Compose stack between scenarios, waits for steady state, and
runs every scenario (continuing past failures). It captures per-scenario logs under
`e2e-artifacts/compose/` (including each run's `scenario.out`) and writes an aggregated
`e2e-artifacts/report.md` summarizing every scenario's PASS/FAIL plus a short failure excerpt, so a
failed run can be triaged from one file. It uses only `KEEPAFLOATD_IMAGE` for local and CI parity.
The separate HAProxy integration compiles the current release binary, copies that exact artifact
into every node image, and rejects the image before startup if its SHA-256 differs.

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
cp config.example.yaml config.container.yaml
CLUSTER_SECRET="$(openssl rand -hex 32)"
sed -i \
  -e "s/replace-me-with-a-random-32-byte-string/${CLUSTER_SECRET}/" \
  -e 's/dry_run: false/dry_run: true/' \
  config.container.yaml
unset CLUSTER_SECRET
chmod 0600 config.container.yaml

docker run --rm \
  --user "$(id -u):$(id -g)" \
  --cap-add=NET_ADMIN \
  --cap-add=NET_RAW \
  -v "$(pwd)/config.container.yaml:/etc/keepafloatd/config.yaml:ro" \
  keepafloatd:dev
```

Notes:

- The runtime image normally runs as uid/gid `10001` (`keepafloatd`). The local example overrides
  that uid/gid with the owner of the mode-`0600` bind mount so the process can read it.
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
down. Status probes carry a versioned SHA-256 identity of the cluster-wide settings, so only nodes
with a matching concrete identity count toward formation.

Examples use `dry_run: true` and `interface: lo` so you can exercise Raft without touching real
addresses.

## Configuration

See `config.example.yaml`.

| Field | Meaning |
|---|---|
| `node_id` | Stable id for this process; must appear in `peers`. No node is special - any majority forms the cluster. |
| `raft_listen` | Address this node listens on for Raft RPC. Must match `peers[node_id].raft_address`. |
| `client_submit_listen` | Address where the leader accepts forwarded `HealthUpdate` and `VipReleased` requests. Must match `peers[node_id].client_submit_address`. |
| `peers` | Peer list (`id`, `raft_address`, `client_submit_address`). Must be identical on every member. Every Raft and submit socket endpoint must be globally unique across the roster after IPv4-mapped IPv6 addresses are canonicalized to IPv4. Each protocol's roster must use one IP family because outbound sockets bind to the advertised source IP. |
| `vips` | VIP list (`address`, `interface`). `address` accepts an optional CIDR suffix (`192.0.2.101/24`); without one the VIP is bound as a host route (`/32` for IPv4, `/128` for IPv6). Order is normalized by sorting addresses; duplicates (by IP) are deduped. `interface: eth0.100` and `interface: eth0` plus `vlan: 100` have the same effective target and configuration identity. |
| `health.command` | Executable + args (`execv` style), e.g. `["/bin/sh","-c","curl -sf http://127.0.0.1/"]`. |
| `health.interval_ms` / `timeout_ms` | Probe period and per-run wall timeout (kill on expiry -> unhealthy). |
| `health.stale_secs` | Maximum time a node may stop contributing committed probe rounds before it becomes ineligible. Must be `>= ceil(interval_ms / 1000)`. Defaults to `max(3, ceil(interval_ms / 1000) * 3)`. Identity uses the resulting whole missed-probe threshold, so second values that produce the same threshold are equivalent. |
| `failover_delay_secs` | Continuous explicit probe-failure duration before an established healthy node publishes unhealthy and releases VIPs. Defaults to `0`; startup failures remain immediately unhealthy. |
| `failback` | `true` (default) allows delayed proactive rebalance after recovery; `false` is nopreempt: no proactive takeover, but the node may still receive orphaned VIPs. |
| `failback_delay_secs` | Continuous healthy duration before a recovered node becomes eligible when `failback: true`. Defaults to 10 seconds. |
| `cluster_secret` | Required non-empty shared secret for authenticating both Raft handshakes and submit envelopes. It must be identical on every member and unique per cluster; the public example placeholder is rejected. |
| `max_frame_bytes` | Raft and snapshot frame cap (defaults to 4 MiB; allowed range 64 KiB-16 MiB). Submit frames use a separate fixed 4 KiB cap. |
| `submit_timeout_ms` | Wall-clock cap on one submit attempt, including local leader writes and follower->leader forwards (defaults to 2000 ms). |
| `raft` | Optional OpenRaft timing knobs (`election_timeout_{min,max}_ms`, `heartbeat_interval_ms`). |
| `address_protocol` | Node-local Linux route protocol and table selector for inert `throw` ownership markers (1-255, default 246; table is `10000 + value`). Every co-located keepafloatd instance must use a distinct value and its derived table must be reserved from other routes and `ip rule`. |
| `dry_run` | Log intended `ip` operations without mutating the host. |
| `notify` | Optional keepalived-style hook run as `<script> INSTANCE <vip> MASTER`, `BACKUP` or `FAULT` after a confirmed first bind or a release. It runs detached from the reconcile tick, inherits the daemon environment, and is killed after 10 seconds. |

All nodes must use the **same effective** `peers`, `vips`, `health.interval_ms`, missed-probe
staleness threshold, `failover_delay_secs`, `failback`, `cluster_secret`, `max_frame_bytes` and Raft
timing. `failback_delay_secs` must also match when `failback: true`; nopreempt ignores and
canonicalizes it when `failback: false`. keepAfloatD hashes every behavior-relevant, non-secret
cluster-wide value into a canonical, versioned fingerprint. Peer ordering, VIP ordering, VLAN
interface spelling, IPv4-mapped peer endpoints, and `stale_secs` values that produce the same whole
missed-probe threshold are normalized; `cluster_secret` remains outside the hash and is compared directly. Node-local values
(`node_id`, listen addresses, health command/timeout, `submit_timeout_ms`, `address_protocol`,
`dry_run`, and `notify`)
are excluded.

A concrete fingerprint mismatch is rejected during a status preflight before any Raft RPC reaches
OpenRaft. A node that repeatedly sees one exact foreign fingerprint on a majority stops
reconciliation, unbinds its locally tracked VIPs, shuts down Raft, and exits with status `4`; a
`Restart=on-failure` supervisor may retry until the config is repaired. Distinct foreign
fingerprints never add into a false majority.
During fresh formation, a reachable peer with a concrete foreign fingerprint is excluded from the
candidate cluster and cannot downgrade the matching majority's advertised capabilities. A coherent
foreign majority also takes precedence over a matching existing minority for the full three-round
confirmation window, independent of YAML peer order.
Existing clusters commit one-way identity enforcement only after every voter is reachable and
advertises the same fingerprint. During a rolling upgrade, an older binary has no fingerprint and
is temporarily accepted until that activation; operators must continue to verify old-node configs
manually in this mixed-version window. If a completed preflight identified a peer as legacy, a
pre-activation reconnect reuses that authenticated legacy identity without waiting on another
status RPC; this prevents an election-time status call from blocking the Raft connection needed to
finish the election. After activation, every such stream is dropped and a missing identity is
fenced on reconnect.

## Health checks

Exit code `0` means healthy. Non-zero exit, timeout or spawn failure means unhealthy. The direct
command and all descendants run in a dedicated process group that is killed and reaped after
completion, timeout, or cancellation.

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
- **SIGHUP:** release VIPs and exit with status 1 to request a supervised restart.
  Without a supervisor, start the daemon again manually.
- **Restart safety:** the next start runs `startup_cleanup` before rejoining Raft. A failed delete is
  verified against kernel state; if the address is still present or cannot be checked, startup
  fails closed instead of later acknowledging an unsafe handoff. After joining, VIP effects stay
  disarmed until the local applied index reaches OpenRaft's leader-reported committed frontier, so
  a diskless replay cannot bind from an obsolete prefix of the log.
- **Address ownership marker:** before binding a VIP, keepafloatd writes a `throw` host route for
  it to dedicated policy table `10000 + address_protocol` (default `10246`) with that protocol.
  If marker creation has an ambiguous process result, keepafloatd retains shutdown tracking and
  treats a later retry as the same first bind; it never assumes the kernel rejected the route.
  Reserve a distinct non-zero value for every co-located keepafloatd instance, and reserve its
  derived table from other routes and `ip rule` entries in the same network namespace. Normal
  configured-address cleanup remains independent of the marker, and this node-local value is
  excluded from the cluster fingerprint. Keep it stable across restarts. To rotate it, gracefully
  stop the old process and verify that no old-protocol route remains in its derived table. After a crash,
  restart once with the old value before rotating, or remove its old address and marker manually.
- **Upgrade before removing VIPs:** binaries predating the marker can leave an unmarked orphan if a
  VIP is deleted from YAML after a crash. Upgrade and restart every node while the VIP remains
  configured, confirm each live VIP has a `throw` route with its configured `address_protocol` and
  derived table in `ip -N -j -4 route show table all` (or `-6`), and only then remove it. The first
  marked version cannot infer ownership of an already removed, unmarked address; remove such a
  legacy orphan manually.

## Security

This daemon has two TCP attack surfaces (Raft RPC and the leader submit listener). v1 hardens them
as follows:

- **Bind addresses:** bind only to the concrete peer-reachable address advertised for this node in
  `peers`; wildcard binds like `0.0.0.0` do not pass config validation.
- **Cluster secret:** every Raft handshake and submit envelope must carry the configured shared
  secret or the request is dropped. Package installs keep secret-bearing configs root-owned and
  mode `0600`; the public example placeholder must be replaced before startup.
- **Config identity:** a bounded status preflight rejects concrete cluster-wide config drift before
  the first Raft frame; replicated activation eventually fences legacy/missing identities too.
- **Frame-size cap:** `max_frame_bytes` rejects oversized frames before allocating.
- **Connection admission:** each Raft and submit listener admits at most 32 unauthenticated and 64
  authenticated tasks globally, with eight connections per configured peer across both phases.
  Peers sharing a canonical source IP share their combined quota. Unknown source IPs and excess
  sockets are dropped before a request-sized allocation; claimed Raft node IDs must match the advertised source address.
  Outbound sockets bind to the local advertised IP, including on multi-homed hosts. Distinct peer
  IPs provide the node-identity boundary; same-host peers sharing one IP rely on `cluster_secret`.
- **Authenticated byte budget:** inbound Raft frame bodies share a 128 MiB reservation budget, so
  concurrent authenticated streams cannot each allocate the configured maximum before reading.
- **Small submit protocol:** submit requests and responses are capped at 4 KiB rather than the
  Raft snapshot limit.
- **RPC / submit timeouts:** half-open peers cannot block heartbeats or submit forwarding forever.
  Handshakes, whole frames (including partial prefixes), request processing, and response writes
  have five-second bounds. Idle authenticated Raft links expire after the greater of five seconds
  and twice the configured heartbeat interval. Legacy response timing includes waiting for state.
- **Bounded diskless replay:** unary AppendEntries catch-up batches are capped at 32 log entries so
  a wiped follower can apply each batch within the heartbeat-derived RPC deadline.
- **Cancellation-safe RPC streams:** a cancelled Raft request drops its stream before reconnecting,
  so an unread response cannot be mistaken for the next election term's response.
- **Rolling-upgrade framing:** the status preflight advertises cancellation-safe stream ownership.
  This dedicated capability is independent of the V2 failover-semantics handshake bit. Current
  peers may reuse a connection only after advertising it. For a legacy reader, the receiver reuses
  only responses dispatched and accepted by one nonblocking TCP write inside 80% of one heartbeat
  interval. A short write or later response closes the connection without continuing the frame,
  before the old reader's competing timeout can retain a late response for the next RPC.
- **Per-peer locking:** one slow peer cannot serialize heartbeats to every other peer.

For networks outside one trusted host or one trusted segment, layer VPN/IPsec/mTLS around this v1
transport.

The transport implementation is split by concern: `src/raft/network.rs` manages peer connections
and RPC exchange, `src/raft/network/client.rs` adapts that exchange to OpenRaft's client traits,
`src/raft/network/server.rs` owns listener admission, authentication, and stream supervision,
`src/raft/network/inbound.rs` dispatches authenticated RPCs,
`src/raft/network/wire.rs` implements bounded framing, versioned handshakes, and cancellation-safe
stream ownership, and `src/raft/network/status.rs` implements legacy-compatible status and
reader-capability probes.

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
