# Development guide

Building, testing and working on keepAfloatD. For the runtime design see
[ARCHITECTURE.md](../ARCHITECTURE.md); for running it in production see
[operations.md](operations.md).

## Build and test

Requires a Rust 2024 toolchain (1.88+, the `rust-version` in `Cargo.toml`).

```bash
cargo build --release      # target/release/keepafloatd
cargo test                 # unit + in-process cluster tests
cargo clippy --all-targets
cargo fmt --check
```

`cargo test` covers the pure logic (config validation, eligibility/staleness filtering, VIP
assignment and generation fencing, cluster-secret and sender-binding checks) plus in-process
multi-node cluster tests that exercise ownership and handoff without touching real interfaces.

In-process cluster fixtures keep their loopback listeners open from
allocation until the real Raft and submit servers take ownership. Do not
discover a free port by closing a reservation and binding it again later.
Tests that intentionally check configured bind failures keep that path;
normal daemon startup still binds its configured addresses.

`THIRD_PARTY_LICENSES.md` must match `Cargo.lock`; CI checks it with
`scripts/ci/test/third-party-licenses.sh`. After a dependency update run
`scripts/ci/test/third-party-licenses.sh --write` (needs `jq`) and commit the result.

### CI and release checks

Both CI systems check all targets with the locked dependency graph on
Rust 1.88.0, in addition to the normal Rust 1.91.1 build and tests.
`cargo deny --locked check` covers licenses, advisories, duplicate versions
and dependency sources. Unknown registries and Git sources are rejected;
the explicit duplicate-version exceptions in `deny.toml` document the
current transitive constraints.

Unsafe Rust is denied for every project target. The process-group cleanup
and FIFO fixtures have narrowly scoped, documented Unix FFI exceptions.
Release builds use thin LTO and one codegen unit, stripping debug
information while retaining the default unwinding behavior.

Coverage uses Tarpaulin's LLVM engine rather than ptrace and requires the
`llvm-tools-preview` Rust component. Every reported production Rust file
must retain at least 80 percent line coverage.

A release runs the normal checks, including Docker E2E, public source
verification and realcluster harness self-tests, before publishing.
In GitLab, select the pipeline ref that matches `PUBLISH_REF`; in GitHub,
select the workflow ref that matches `publish_ref`. Both must identify
the same commit as the pipeline or workflow. A ref that advances during
the checks is rejected; start a new branch pipeline to verify that new
commit. GitHub tag-triggered releases verify that tag's commit directly.
Dry runs retain the same checks but skip release publication. GitLab's
ordinary build jobs still publish temporary CI images for their tests.
These checks do not run the 35 scenarios against a live Ceph cluster.

GitLab runtime images use the static amd64 and arm64 binary artifacts
from the same pipeline through Docker's `runtime-dist` target. The image
job checks their architecture and static linkage before pushing. Rust
is not recompiled under ARM64 emulation; the dev image and all test gates
remain separate checks. Local source-based Docker builds are unchanged.

## Code layout

| Path | Responsibility |
|---|---|
| `src/main.rs` | Process startup, wiring, signal handling. |
| `src/config.rs` | YAML config, validation (secure-by-default guards live here). |
| `src/health.rs` | The local script/command health probe. |
| `src/vip.rs` | `ip addr` bind/unbind, ARP/NA announcements, and the reconcile loop. |
| `src/bind_policy.rs` | Pure decision "should this node hold this VIP right now?". |
| `src/submit.rs` | The TCP/JSON submit channel (auth + sender binding). |
| `src/raft/` | OpenRaft integration: `network` (transport), `probe` (peer discovery), `store/` (log + state machine + `vip_logic` placement). |
| `src/cluster_test.rs` | In-process multi-node integration tests. |

The placement and eligibility logic in `src/raft/store/vip_logic.rs` is deliberately pure and
deterministic (no clocks, no RNG, iteration over sorted structures) so every node and every replay
computes the same holder map - this is what keeps ownership consistent across the cluster.

`src/raft/admission/runtime/` drives cold formation, learner joining,
promotion and renewal under exact per-boot admission. The genesis is
immutable; applying it enables V2 semantics and configuration identity
enforcement together. `src/raft/control.rs` owns the stale-survivor
guard. See [runtime admission](runtime-admission.md) for the separate
permission, restart and VIP activation clocks.

## End-to-end scenario harness

Real failover is tested with a Docker Compose harness under [`tests/e2e/`](../tests/e2e/): three (or
five) `keepafloatd` containers plus a probe container on a private bridge, with VIPs claimed inside
that bridge only.

```bash
docker build -t keepafloatd:dev .
KEEPAFLOATD_IMAGE=keepafloatd:dev bash tests/e2e/scripts/run.sh    # 3-node suite
KEEPAFLOATD_IMAGE=keepafloatd:dev bash tests/e2e/scripts/run22.sh  # timing/nopreempt suite
KEEPAFLOATD_IMAGE=keepafloatd:dev bash tests/e2e/scripts/run26.sh  # config drift/repair suite
KEEPAFLOATD_IMAGE=keepafloatd:dev bash tests/e2e/scripts/run5.sh   # 5-node minimal-movement suite
```

`run.sh` resets the stack between scenarios, waits for steady state, runs every scenario (continuing
past failures), and writes an aggregated `e2e-artifacts/report.md` plus per-scenario logs under
`e2e-artifacts/compose/<scenario>/`.

### Layout of `tests/e2e/`

- `configs/` - the static node configs the harness feeds each container.
- `configs22/` - one-VIP configs with explicit failover delay and nopreempt enabled.
- `configs26/` - matching majority plus one deliberate cluster-config mismatch.
- `scenarios/` - one `NN_name.sh` per failover scenario (steady state, holder death, leader death,
  network partition, graceful SIGINT, restart/rejoin, full-outage recovery, cold start, stale-survivor
  fencing, …).
- `scenarios22/` - delayed explicit failover and recovered-nopreempt fallback regressions.
- `scenarios26/` - pre-dispatch config mismatch self-fence and exact-repair rejoin.
- `scripts/lib.sh` - the shared helpers (`kill_service`, `wait_for_service_exit`,
  `wait_for_even_over_nodes`, VIP-holder assertions) every scenario builds on.

### Adding a scenario

Copy an existing `scenarios/NN_*.sh`, source `lib.sh`, drive the cluster (kill/partition/restart a
container), then assert the observable outcome with the `wait_for_*` helpers. Scenarios are numbered
so they run in order; each starts from a fresh stack. Keep them deterministic - assert on VIP
placement and leadership, not on wall-clock timing.

For a post-event log assertion, capture `log_checkpoint` before the
event and pass its opaque value to `wait_for_log_any_after`. Checkpoints
track each container's identity and log prefix, not a line count in
Compose's reorderable aggregate output. A restarted container may append
new evidence; a replacement container, lost prefix or failed log read
invalidates the observation. Keep the functional VIP assertions too.
`bash tests/e2e/scripts/log-checkpoint-test.sh` checks this boundary with
local fixtures and no Docker access.

The Compose scenario `17_gratuitous_arp` passively captures two unsolicited
ARP requests from a new holder after health-driven handoff. It never
probes the VIP during capture, so solicited replies cannot mask a missing
announcement. Packet output is retained with the scenario artifacts.

The Compose scenario `18_missing_interface` uses a disposable container
network namespace to check startup with a missing VIP device, shutdown
after device deletion, and fail-closed shutdown after device renaming.
It covers IPv4 and IPv6, orphan marker cleanup and subnet siblings.
Daemon exit status and kernel state are checked before container teardown.
The rename case also rejects shutdown handoff messages after failed
cleanup.

`25_promote_secondaries` checks the real daemon's startup warning in
isolated network namespaces. It covers disabled promotion, per-interface
and global enablement, a missing device, IPv6-only VIPs and dry-run mode.
The warning must precede cleanup, startup must continue, and sysctls
must remain unchanged.

`27_follower_rejoin_election` isolates a follower, observes repeated
Pre-Vote attempts, and heals the partition without a majority leader
change. It repeats the check after SIGSTOP/SIGCONT and checks unique
ownership after the resumed follower has fenced itself and rejoined.
It does not claim that a stopped process can clean its kernel addresses.
Controlled loopback tests cover saved terms, majority election after
leader loss, authentication, capability negotiation and rejected RPCs.

`23_graceful_handoff` stops a follower and a leader in separate fresh
stacks. Its fixtures use a sixty-second stale window and a thirty-second
probe failure delay. It requires committed shutdown releases and unique,
ARP-reachable VIPs on the survivors before either window can expire.

`19_bind_failure` renames a holder's interface inside its container while
retaining its peer IP. It checks a real bind failure, unhealthy handoff,
absence of VIPs across every local interface, and recovery after repairing
the device name and restarting the daemon. The service health probe stays
successful throughout. No host interfaces or real Ceph services change.
Preparation waits for live address commands, not exited children that
the paused daemon cannot reap. Its process-state regression uses fixtures:
`bash tests/e2e/scripts/address-commands-test.sh`.

`20_formation_retry` replaces one stopped test node with a discovery-only
TCP peer. Two blank daemons observe its existing-cluster response without
receiving any Raft state. After that peer disappears, the same daemons
must form a majority and restore unique VIP ownership without restarting.
Only disposable container endpoints change; the peer helper never sends
Raft votes or log entries.

`21_source_admission` borrows a stopped test peer's container address for
status-only connections. It fills pre-authentication and authenticated
source quotas independently, checks rejection at each bound, and verifies
that existing authenticated sockets still work under handshake pressure.
No Raft votes or log entries are sent by the helper.

`22_ipv6_announcement` recreates only the test stack with a dual-stack
bridge and IPv6 VIP fixtures derived from the normal node configs. It
checks `nodad` and source-address deprecation, then seeds the observer's
neighbor cache with the old holder's MAC. A health-driven handoff must
produce an unsolicited NA for the VIP with the override flag and replace
that cached MAC. No active VIP probe may repair the cache during capture.
Kernel state, packet capture and before/after cache entries are retained.

### Real-cluster scenarios

The [real-cluster guide](../tests/realcluster/README.md) lists the 35 SSH-driven scenarios,
their prerequisites, and the distinction between live campaign results and local self-tests.
Validate the complete harness without accessing a cluster:

```bash
./scripts/ci/test/realcluster-harness.sh
```

This checks the documented inventory, shell syntax, and failure-handling self-tests using
only the sanitized example environment. It never runs `run-all.sh` against a live cluster.

### Signal and supervisor regressions

`cargo test --test signals` sends real signals to a dry-run daemon child and
checks VIP cleanup and exit status. It never changes host interfaces.
The same fixture's `network_logs` regression reconnects rejected Raft and
submit clients, checks bounded warnings and a successful status request,
then verifies graceful shutdown. Limiter unit tests use paused Tokio time
to check interval boundaries and suppressed counts without a wall-clock
observation window.
The Compose suite also covers SIGHUP and SIGQUIT release, survivor takeover
and rejoin in `16_signal_restart`.

On an authorized disposable systemd host, run the packaged-unit regression:

```bash
mkdir -p /tmp/keepafloatd-signal-evidence
sudo bash tests/systemd/sighup.sh /absolute/path/to/keepafloatd \
  deploy/systemd/keepafloatd@.service /tmp/keepafloatd-signal-evidence
```

It creates a temporary unit and private network namespace, sends SIGHUP,
SIGQUIT and SIGKILL twice each, and asserts the VIP is absent before each
automatic restart. For SIGKILL, it also checks the VIP exists before
ExecStopPost runs, proving that the fallback performs the cleanup.
A final SIGTERM must stop successfully. Unrelated addresses and another
instance's marker must remain. The namespace survives each daemon exit,
so kernel address leakage cannot be hidden by container teardown.
Cleanup removes only the temporary unit and namespace; the journal and
exit evidence are kept in the requested directory. No Ceph services,
existing keepafloatd instances or host interfaces are modified.

## Conventions

- `cargo fmt` (edition 2024 style) and `cargo clippy --all-targets` must be clean.
- Keep `src/raft/store/vip_logic.rs` pure - no clocks, RNG or hash-map iteration order, so replays
  stay deterministic.
- Every behaviour change needs a test: a unit test for logic, or an e2e scenario for cluster-level
  behaviour.
- Contributions require a signed CLA or copyright assignment - see [CONTRIBUTING.md](../CONTRIBUTING.md).
