# Development guide

Building, testing and working on keepAfloatD. For the runtime design see
[architecture.md](architecture.md); for running it in production see
[operations.md](operations.md).

## Build and test

Requires a Rust 2024 toolchain (1.87+).

```bash
cargo build --release      # target/release/keepafloatd
cargo test                 # unit + in-process cluster tests
cargo clippy --all-targets
cargo fmt --check
```

`cargo test` covers the pure logic (config validation, eligibility/staleness filtering, VIP
assignment and generation fencing, cluster-secret and sender-binding checks) plus in-process
multi-node cluster tests that exercise ownership and handoff without touching real interfaces.

## Code layout

| Path | Responsibility |
|---|---|
| `src/main.rs` | Process startup, wiring, signal handling. |
| `src/config.rs` | YAML config, validation (secure-by-default guards live here). |
| `src/health.rs` | The local script/command health probe. |
| `src/vip.rs` | `ip addr` bind/unbind, gratuitous ARP, and the reconcile loop. |
| `src/bind_policy.rs` | Pure decision "should this node hold this VIP right now?". |
| `src/submit.rs` | The TCP/JSON submit channel (auth + sender binding). |
| `src/raft/` | OpenRaft integration: `network` (transport), `probe` (peer discovery), `store/` (log + state machine + `vip_logic` placement). |
| `src/cluster_test.rs` | In-process multi-node integration tests. |

The placement and eligibility logic in `src/raft/store/vip_logic.rs` is deliberately pure and
deterministic (no clocks, no RNG, iteration over sorted structures) so every node and every replay
computes the same holder map — this is what keeps ownership consistent across the cluster.

## End-to-end scenario harness

Real failover is tested with a Docker Compose harness under [`tests/e2e/`](../tests/e2e/): three (or
five) `keepafloatd` containers plus a probe container on a private bridge, with VIPs claimed inside
that bridge only.

```bash
docker build -t keepafloatd:dev .
KEEPAFLOATD_IMAGE=keepafloatd:dev bash tests/e2e/scripts/run.sh    # 3-node suite
KEEPAFLOATD_IMAGE=keepafloatd:dev bash tests/e2e/scripts/run5.sh   # 5-node minimal-movement suite
```

`run.sh` resets the stack between scenarios, waits for steady state, runs every scenario (continuing
past failures), and writes an aggregated `e2e-artifacts/report.md` plus per-scenario logs under
`e2e-artifacts/compose/<scenario>/`.

### Layout of `tests/e2e/`

- `configs/` — the static node configs the harness feeds each container.
- `scenarios/` — one `NN_name.sh` per failover scenario (steady state, holder death, leader death,
  network partition, graceful SIGINT, restart/rejoin, full-outage recovery, cold start, stale-survivor
  fencing, …).
- `scripts/lib.sh` — the shared helpers (`kill_service`, `wait_for_service_exit`,
  `wait_for_even_over_nodes`, VIP-holder assertions) every scenario builds on.

### Adding a scenario

Copy an existing `scenarios/NN_*.sh`, source `lib.sh`, drive the cluster (kill/partition/restart a
container), then assert the observable outcome with the `wait_for_*` helpers. Scenarios are numbered
so they run in order; each starts from a fresh stack. Keep them deterministic — assert on VIP
placement and leadership, not on wall-clock timing.

## Conventions

- `cargo fmt` (edition 2024 style) and `cargo clippy --all-targets` must be clean.
- Keep `src/raft/store/vip_logic.rs` pure — no clocks, RNG or hash-map iteration order, so replays
  stay deterministic.
- Every behaviour change needs a test: a unit test for logic, or an e2e scenario for cluster-level
  behaviour.
- Contributions require a signed CLA or copyright assignment — see [CONTRIBUTING.md](../CONTRIBUTING.md).
