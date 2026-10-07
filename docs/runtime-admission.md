# Diskless runtime admission

Every process starts with an empty Raft store and a fresh, random
256-bit boot nonce. Its Raft identity is the pair of configured physical
node ID and boot nonce. A restarted process cannot reuse the previous
process's votes, health reports or permission to participate in Raft.
No admission, vote, log or clock state is stored on disk.

## Permission and replicated history

Admission selects an immutable genesis: the configuration fingerprint,
cluster epoch and exact consenting boot identities. Status discovery
provides candidates, not permission. A configured physical majority
must reserve the same genesis before a consumer can use it.

The runtime drives three separate operations:

- Cold formation requires consent from every proposed genesis member
  as well as a majority of distinct configured physical IDs.
- A replacement boot first joins as a learner. A committed preparation
  names the exact operation and old and new voter sets. Catch-up and
  a committed acknowledgement precede the committed membership change.
  A fresh challenge can continue the same live learner operation, but
  cannot revive an expired process or authorize an unrelated operation.
- Renewal requires the consumer's own freshly challenged progress to
  be committed and applied locally, followed by fresh majority grants.
  An issuer can wait for its local application within the existing RPC
  budget. It checks the exact evidence and live authority before granting.

Admission-only progress does not report service health or advance probe
ticks. Only an actual health probe changes those replicated inputs.
Historical identities in logs or snapshots are data, not current voting
permission. Installing a snapshot does not restore local admission.

Grant requests and replies bind the exact sender and consumer boots,
genesis, operation, challenge and authenticated connection. A reply is
usable only before the deadline measured from its request's start.
Repeated, delayed or cross-connection replies cannot start a new lease.
Two verified renewals may share one clock tick; accepting the same
deadline does not extend permission or revive an expired process.
Permission expiry is terminal for that process. The supervisor must
perform cleanup and restart with a new boot identity.

## Separate restart and VIP timers

The timing argument assumes a maximum ratio of two between participating
monotonic clock rates. `H` is the existing health-proof freshness
lifetime; increasing admission lifetime does not extend service health
freshness. Let `N` be the configured physical member count, `Emin` and
`Emax` the configured Raft election timeouts, and `heartbeat` its interval.
Admission uses these separate budgets:

```text
R = min(H / 6, 500ms)                  RPC and local-grant budget
P = H / 4                             renewal pause
E = 3 * Emax + 1.5 * heartbeat + 2 * Emin
A = R                                 local progress-apply budget
L = (N + 4) * R                       renewal-round body budget
U = max(H, 2 * E + 4 * L + 2 * P)      consumer permission lifetime
```

`E` includes the greater-log election wait. `L` includes per-member work
and bounded local apply (`A`). Waiting for the round lock and executing
its body each have an `L` bound; a local grant has an `R` bound. Voter
renewal takes priority over membership maintenance.

The `2 * E` term accounts for the worst supported clock-rate ratio
during one ordinary election. The four `L` allowances cover the last
proof's age, a failed attempt, queued work and successful renewal;
the two `P` allowances cover renewal pauses. This is a timely-case
budget, not a guarantee under repeated split votes, unbounded apply
delays or scheduler stalls. Missing the bounds does not permit extending
an expired lease or weakening the unchanged health gate.

The issuer reserves its physical member for `G = 2 * U`. A restarted
issuer waits `B = 2 * G = 4 * U` before issuing or using permission,
covering reservations that disappeared with its volatile memory.

Kernel cleanup does not increase `G` or `B`. It has a separate effect
fence. Let `C` be the reconciliation interval plus the bounded cleanup
budget for every configured VIP. A process waits `2 * (U + C)` from its
first verified admission before an applied health proof can authorize
local VIP activation. Renewal does not move that starting point.
This fence also applies when an assignment has no previous holder;
first-generation placement is not an exception.

For the shipped three-node examples, `H = 11s`, `R = A = 0.5s`,
`P = 2.75s`, `Emin = 0.4s`, `Emax = 0.8s` and `heartbeat = 0.25s`.
Thus `E = 3.575s`, `L = 3.5s` and `U = 26.65s`. Two VIPs give
`C = 0.25s + 2 * 12s = 24.25s`: the issuer reservation is 53.3 seconds,
restart quarantine is 106.6 seconds and VIP activation waits another
101.8 seconds after admission. Quarantine plus activation delay totals
208.4 seconds. Election, catch-up, health checks and network delays add
to startup time; this is not a promise of a fixed recovery deadline.

At startup, the `runtime admission timing` log reports quarantine,
activation delay and their sum (`startup_safety_wait_ms`). This sum is
the earliest permitted VIP activation after a cold start, not a
readiness signal. Monitoring must still observe successful formation,
health and VIP activation.

## Operational assumptions

Run only one live process for each configured physical identity. Every
participant must use the same configuration and timing policy. Do not
shorten the policy while grants from the previous policy may remain
valid. A configuration or protocol change requires a coordinated stop.

Clock-rate bounds, scheduling and completion of cleanup within its
budget are assumptions, not properties provided by a timer. Suspended
clocks, indefinitely stalled processes or kernels, or failed address
removal can leave OS-held VIPs behind. The protocol does not fence such
a machine physically. Environments requiring safety despite those
failures need independent fencing and verified address cleanup.

Automatic recovery requires an eventually reachable, timely physical
majority. No particular node is a bootstrap master. A minority cannot
grant itself authority, and unavailable quorum must never be replaced
with an optimistic local decision.

An established follower cannot install a snapshot across membership
changes it cannot verify from its local history. It fails closed. If
ordinary replication cannot catch up, its permission expires and the
supervisor restarts that node. The fresh boot obtains a verified learner
admission before accepting the current snapshot. This requires the
surviving quorum, not a coordinated restart of the whole cluster. Run
with a supervisor that restarts failed processes, as the packaged
systemd unit does; running the binary alone does not restart it.

## Upgrade

Protocol version 3 is incompatible with earlier releases. Stop all
instances and their automatic restart mechanisms, verify that every
VIP and ownership marker has been removed, replace all binaries and
then restart on the same version and configuration. Include offline
members before allowing them to rejoin. This interrupts VIP service;
it is not a rolling upgrade. See the [operations guide](operations.md).
