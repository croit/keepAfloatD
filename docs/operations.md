# Operations guide

Running, observing, changing and troubleshooting a keepAfloatD cluster in production.
For the config field reference see [`config.example.yaml`](../config.example.yaml); for the
design see [ARCHITECTURE.md](../ARCHITECTURE.md).

## Running and lifecycle

The packaged `systemd` unit is instanced, one instance per config file.
Before enabling it, configure this node's id/listen addresses, the shared
peer roster, VIPs and health command. Generate the secret once and securely
provision that same value on every member, not a new value per node:

```bash
sudo install -o root -g root -m 0600 \
  /etc/keepafloatd/config.yaml /etc/keepafloatd/config-node1.yaml
CLUSTER_SECRET="$(openssl rand -hex 32)"
sudo sed -i \
  "s/replace-me-with-a-random-32-byte-string/${CLUSTER_SECRET}/" \
  /etc/keepafloatd/config-node1.yaml
unset CLUSTER_SECRET
sudoedit /etc/keepafloatd/config-node1.yaml
sudo systemctl enable --now keepafloatd@node1   # reads /etc/keepafloatd/config-node1.yaml
sudo systemctl status keepafloatd@node1
sudo systemctl restart keepafloatd@node1
sudo systemctl stop keepafloatd@node1
```

The unit uses `Type=simple`: an `active` service means the process has
started, not that formation or VIP activation has completed. Each new
process observes restart quarantine and a separate activation delay
after admission. The `runtime admission timing` log reports their sum
as `startup_safety_wait_ms`, a lower bound rather than a readiness signal.
Wait for formation, healthy service probes and unique VIP ownership.
See [runtime admission timing](runtime-admission.md#separate-restart-and-vip-timers) for
the calculation and its clock, cleanup and scheduling assumptions.

`stop`, `Ctrl+C` and `SIGTERM` use the same graceful path. The node stops
health publishing and reconciliation, then unbinds every VIP it holds.
After successful cleanup it reports unhealthy and acknowledges the
resulting ownership releases. With a working quorum, healthy survivors
can take over without waiting for the stale-holder window or the probe
failure delay. Reconciliation and leader election can still cause a brief
interruption; this is not a zero-downtime guarantee.

Health publication uses bounded admission-management requests. A
leadership change does not itself cancel an in-flight request. Failed
or cancelled publication never renews the local health proof.

Followers use Pre-Vote before starting an election. An isolated follower
cannot advance its term without contacting a willing majority, reducing
disruption when it returns. Each attempt authenticates and sends an
explicitly tagged, read-only request. Protocol version 3 requires
Pre-Vote support; it never grants a vote just because a peer reports
missing support. Old protocol versions and failed exchanges never count
as grants. Genuine leader loss still causes election and VIP withdrawal
under the existing freshness gates.
Pre-Vote does not repair history or votes forgotten by a diskless restart.

The notification phase shares one `submit_timeout_ms` budget. If the
leader cannot be reached, quorum is unavailable, or the leader cannot
provide an applied-log proof, shutdown logs a warning and continues.
Survivors then use the ordinary stale-holder fallback. A cleanup failure
does not acknowledge release and makes shutdown fail. On the next start,
the daemon first reclaims any marked crash orphans, then rejoins Raft.

If the configured interface disappears, cleanup can still prove that its
VIP is absent by querying the exact address across all interfaces. A failed
query or an address still present on another interface (including after a
rename) blocks cleanup and retains ownership evidence. A missing device
does not make the node able to serve the VIP; restore the device before
expecting it to bind again.

Any failed bind or ownership-marker write puts the node into a local
FAULT state immediately, even if the service health command succeeds.
Its health reports remain unhealthy and healthy peers can take over once
cleanup is proven. Successful service probes do not reset this fault.
Fix the cause shown in the bind error (such as a missing interface or
`CAP_NET_ADMIN`), then restart the affected daemon. Merely restoring the
interface does not make that process eligible again. A failed cleanup
still blocks release acknowledgement; do not bypass its ownership fence.

`SIGHUP` and `SIGQUIT` request a graceful restart: the daemon releases its VIPs and exits
with status 1. The packaged `Restart=on-failure` policy starts a new process,
which reads the configuration again. This is not an in-place reload.
`SIGQUIT` follows this cleanup path without producing a core dump.
Without a supervisor, the daemon stays stopped until started manually.
Use `systemctl stop` for a deliberate stop without automatic restart.

The daemon needs `CAP_NET_ADMIN` (for `ip addr add|del`) and `CAP_NET_RAW`
for neighbor announcements. Both are granted by the packaged unit;
running as root also works. Packaged installs include `arping` for IPv4
and `ndptool` (libndp 1.8+) for IPv6. For a standalone binary, install
`libndp-tools` on Debian or `libndp` on EL9 to provide `ndptool`.
IPv6 VIPs skip DAD through per-address `nodad`, relying on the ownership
fence, and remain deprecated for outbound source-address selection.
Announcement failures are logged without changing VIP ownership.

### Protective failure and repair policies

A panic or unexpected exit in a supervised critical task stops the
daemon and attempts VIP cleanup. Continuing with an incomplete health,
consensus or reconciliation loop could leave stale ownership in use.
The process exits unsuccessfully; the packaged supervisor can restart
it after post-stop cleanup. Failed cleanup is never proof of release.
Ordinary failed health probes and failed notification commands are not
task panics and keep their existing handling.

While all health, ownership and consensus-freshness gates permit a bind,
reconciliation periodically reasserts the VIP in the kernel. This
intentionally repairs external address deletion. Manually removing an
address does not withdraw the node's assignment or disable the daemon.
Stop the instance through its supervisor for maintenance instead.
Repair does not bypass a failed gate or reset a latched bind fault.

### Transition notification hooks

The optional `notify` script receives these arguments:
`<script> INSTANCE <vip> MASTER|BACKUP|FAULT`. Accepted hooks run one at
a time, in submission order across all VIPs in this daemon instance.
Each invocation retains its ten-second budget. A nonzero exit, spawn
failure or timeout is logged and does not stop later hooks. Dry-run logs
the intended invocation without starting a worker or a subprocess.

Hooks never delay address cleanup or reconciliation. One worker runs at
most one hook, with room for 64 waiting invocations. If the queue is full,
the newest invocation is dropped with a warning identifying the VIP,
state and rejection reason. Accepted hooks keep their original order;
there are no retries or persistent delivery guarantees. Keep hooks short
and monitor `notify dropped` warnings. A hook is advisory, not an
ownership fence or proof that a VIP is still present.

After cleanup and ownership handoff, daemon shutdown closes the queue
and allows one second for accepted hooks to drain. It then cancels the
active hook, kills its process group and discards any remaining queued
hooks, logging the cancellation. Joining the worker has a separate
one-second bound. Shutdown draining does not extend the per-hook budget.
An unexpected worker panic is reported to daemon supervision. Hook
failures and overload do not change health or fencing decisions.

### Cleanup-only and post-stop recovery

**Stop the instance and prevent automatic restart before running
`--cleanup-only` manually. Never run it alongside a live daemon.**
The command removes addresses without coordinating with Raft and does
not check whether another process for this instance is still running.

After confirming the instance has stopped, use its existing config:

```bash
sudo /usr/bin/keepafloatd --config /etc/keepafloatd/config-node1.yaml --cleanup-only
```

The command loads and validates the full config before cleanup. It uses
the normal startup cleanup to remove configured VIPs and this instance's
marked crash orphans, including marked VIPs no longer listed in YAML.
Unrelated unmarked addresses and another instance's ownership markers
are not cleanup targets. Keep `address_protocol` unchanged so the
command can find the stopped instance's markers. IPv4 removal still
requires the secondary-address promotion precautions below.

No Raft runtime, listeners, health checks or notify hooks start.
`dry_run: true` remains a simulation: it logs the configured cleanup
intent without issuing address or marker commands. Configuration,
discovery and cleanup errors return a nonzero status; failed cleanup
is not proof that the VIPs have been released.

The packaged unit runs the same command through `ExecStopPost` after
the daemon exits, including failed startup and forced termination.
The hook reads the instance's config again, so keep that file and its
secret source valid and accessible until stopping has finished.
Its failure is not ignored: systemd records a failed service result.
`Restart=on-failure` can retry after a daemon or post-stop failure,
subject to systemd's start-rate limits. An explicit `systemctl stop`
does not trigger an automatic restart. The next start still performs
startup cleanup; the hook is not a permanent restart inhibitor.

### Whole-stop budget

The packaged unit retains `TimeoutStopSec=15` as its initial hang guard.
`NotifyAccess=exec` lets the daemon and the cleanup-only `ExecStopPost`
command extend that deadline with `EXTEND_TIMEOUT_USEC` notifications.
Each checkpoint grants the next finite work item's allowance plus the
existing fifteen-second guard. Total stop time can therefore exceed
fifteen seconds when cleanup is progressing through many VIPs or orphans.
No background timer renews the lease: a stalled work item cannot renew
its own deadline. Systemd may retain an earlier, later deadline rather
than shorten it when a subsequent item needs less time.

The allowances include process cleanup, not just command execution:

| Work item | Conservative allowance before the guard |
|---|---|
| One IPv4 promotion sysctl read | 250 ms |
| Concurrent address and marker discovery | 1.25 seconds |
| One post-stop address or marker deletion, including verification | 2 seconds |
| One graceful VIP release, including all three attempts | 12 seconds |
| Ownership handoff for all VIPs together | `submit_timeout_ms` |
| One supervised Raft control or network task join | 1 second |

An `ip` command has a 250 ms execution limit. A timed-out child may need
another 500 ms to reap; captured output may need another 500 ms to drain.
An address or marker deletion can require a separate presence query.
The graceful allowance covers both deletions and both queries on every
attempt. These are conservative sums, not expected latencies: some error
paths stop before consuming every component. Failed cleanup still fails
the stop and never authorizes a release acknowledgement.

For `V` configured VIPs, the graceful command allowance is at most
`12 * V` seconds, followed by the single handoff budget and up to two
seconds for notification drain/cancellation. Control and network task
joins get checkpoints individually, so their allowance follows the
actual task count. Cancellation and final Raft shutdown also have
finite supervisor guards; no claim of an intrinsic timeout is made for
their underlying joins.

Post-stop cleanup discovers its work again. With `A` merged configured
and discovered address targets and `M` discovered markers, its command
allowance is `1.25 + 2 * (A + M)` seconds. Promotion checks add at most
`0.25 * (I + 1)` seconds for `I` configured IPv4 interfaces. These loops
renew per item, including markers with no remaining address. Startup
cleanup uses the same checkpoints so a stop requested during startup
can finish that finite inventory before normal teardown.

If `NOTIFY_SOCKET` is absent, the standalone binary sends no supervisor
notifications. If a notification fails, cleanup continues and logs a
warning; the previous supervisor deadline remains in force. A custom
supervisor or unit must support and admit these notifications to obtain
the work-scaled guard. Never treat a timed-out stop as verified cleanup:
inspect the journal and confirm that all owned VIPs and markers are absent.

Validate supervisor behavior on an authorized disposable systemd guest,
not a production host. Exercise graceful cleanup lasting longer than
fifteen seconds and SIGKILL followed by equally long `ExecStopPost`
cleanup, including IPv4, IPv6 and marked orphans removed from YAML.
Check the actual kernel state before restart. Separately suspend the
daemon and the cleanup helper after a checkpoint and verify finite
timeout/failure, with no renewal from a background worker. Socket-capture
unit tests alone do not prove systemd accepted or enforced the leases.

The checked-in guest harness runs those cases with a private network
namespace and a temporary unit derived from the packaged template:

```bash
mkdir -p /tmp/keepafloatd-stop-evidence
sudo env KEEPAFLOATD_DISPOSABLE_SYSTEMD_TEST=1 \
  bash tests/systemd/stop-budget.sh /absolute/path/to/keepafloatd \
  deploy/systemd/keepafloatd@.service /tmp/keepafloatd-stop-evidence
```

It keeps journals, unit results and kernel inventories. After a deliberately
stalled post-stop helper, it records residual VIPs, confirms both processes
have exited, runs manual cleanup, and checks absence before teardown.
Unexpected cleanup failures retain the namespace for diagnosis.
The harness also seeds marked orphans before startup and requests stop
while their cleanup is active, verifying progress beyond fifteen seconds
and kernel absence before teardown.

### Unacknowledged takeover allowance

Without a committed release acknowledgement, a replacement waits for the
previous holder's probes to become stale and for the activation round.
It also starts a local timer when it observes that stale assignment.
That timer reserves one full health-proof lifetime, a 250 ms reconcile
tick, and 12 seconds for every configured VIP. It uses the configured
inventory, not just the addresses assigned to the replacement.

The 12-second component is the same conservative address-and-marker
cleanup allowance described above, including verification, child cleanup
and all permitted retries. Compared with counting only two raw command
timeouts, this adds 11.5 seconds per configured VIP to the local timer.
For three configured VIPs, the cleanup component is 36 seconds.

A matching release acknowledgement bypasses this additional timer.
An already activated generation and a first assignment in a fresh
cluster retain their existing shortcuts. A new observed probe from the
previous holder resets an unacknowledged takeover's timer.

This is a reservation for one cleanup pass, not a guarantee that failed
kernel operations eventually succeed or that suspended processes run.
Persistent cleanup errors remain errors and never authorize a release
acknowledgement. Do not treat this local timer as an end-to-end failover
deadline: probe progress, elections and scheduling also affect recovery.

### IPv4 secondary-address promotion

Before startup cleanup, the daemon checks `promote_secondaries` once
for each configured IPv4 VIP interface. If both the interface setting
and `net.ipv4.conf.all.promote_secondaries` are disabled, it warns that
removing a primary address can also remove other addresses in the same
subnet. Enabling either setting prevents that cascading removal.

Review and enable the interface-specific setting on the relevant hosts
before relying on independent IPv4 VIP removal. With `sysctl`, use the
slash form `net/ipv4/conf/<interface>/promote_secondaries` for interface
names containing dots, such as VLAN devices. Configure persistence with
the host's sysctl management. See the
[Linux IP sysctl documentation](https://kernel.org/doc/html/latest/networking/ip-sysctl.html).

The check is diagnostic only: it never changes sysctls, fails startup
or changes VIP ownership. An unreadable or invalid value produces a
separate warning unless the other setting already proves promotion is
enabled. Each read has a 250 ms budget. Dry-run and IPv6-only configs
skip the check. Later sysctl changes are not monitored.

## Observing state

- **Logs:** `RUST_LOG=info` for normal operation, `RUST_LOG=keepafloatd=debug` to trace every bind,
  unbind, election and health transition. With `systemd`: `journalctl -u keepafloatd@node1 -f`.
- **Who holds a VIP:** the VIP is a real secondary address, so ask the kernel:
  `ip addr show | grep <vip>` on each node - exactly one node should have it.
- A node logs `bound <vip>` when it takes a VIP and `unbound <vip>` when it releases one.

Repeated inbound Raft and submit warnings are limited to one message per
warning site per listener every 30 seconds, including `accept` errors.
The first occurrence is immediate. The next occurrence after the interval
includes `suppressed`, the number of intervening warnings omitted at that
site. The current connection's details are a sample, not a complete audit
of rejected clients. Different remote addresses, ports or error text at
the same site share the limit, so reconnecting cannot reset it.
Different warning sites and listeners have independent limits. No summary
timer runs when traffic stops; counters are volatile and reset on restart.
Rejection, retry and shutdown behavior is unchanged. Lifecycle, health and
VIP transition messages are not subject to this network-warning limit.

## Changing the VIP list or config

The effective cluster-wide settings must agree on every node: `peers`,
resolved `vips`, `health.interval_ms`, the missed-probe threshold
derived from `health.stale_secs`, `failover_delay_secs`, `failback`,
`cluster_secret`, `max_frame_bytes`, and Raft timing.
`failback_delay_secs` must also agree when `failback: true`; nopreempt
ignores it.

Changing the VIP list, a VIP's prefix or effective interface, or another
effective cluster-wide setting requires a coordinated stop and restart.
Plan a maintenance window with an interruption of VIP service. Do not
apply these changes through rolling restarts, `SIGHUP` or `SIGQUIT`.

Each process reads its config only at startup. Editing every YAML file
does not update running processes. VIPs are part of the configuration
fingerprint, so a restarted node with a changed VIP list cannot join
peers still running the old config. In a three-node cluster, the first
restarted node can confirm the two old-config peers as a mismatched
majority and exit with status 4.

1. Save each node's current config securely, including the old VIP list
   and interfaces. Include offline nodes in the plan so none can return
   with the old config.
2. Stop every daemon using its current config with
   `systemctl stop keepafloatd@nodeX` on its node, or stop its
   supervisor so it cannot restart automatically. Do not start an
   updated daemon while an old-config daemon can still run.
3. Verify that all old daemons have stopped and all old VIPs are absent
   from every node, including VIPs being removed or moved to another
   interface. Check IPv4 and IPv6 with `ip -4 addr show` and
   `ip -6 addr show`. If a node or VIP cannot be checked, stop here
   until it can be verified or the node is confirmed powered off.
   Keep offline nodes stopped until their configs are updated.
4. Apply the new cluster-wide settings on every node, preserving the
   node-local values and restricted config-file permissions. Verify that
   the effective settings agree before starting any daemon. Do not rely
   on startup cleanup to remove VIPs omitted from the new config.
5. Start the daemons with the new config. Once a majority is running,
   confirm that the cluster forms and health checks pass. Before ending
   maintenance, check that every new VIP has exactly one holder, removed
   VIPs remain absent, and clients can reach the intended services.

To roll back, repeat the coordinated stop. Verify that VIPs from both
the old and new lists are absent before restoring each node's saved
config and starting the daemons. Do not roll back one node at a time.

`node_id`, local listen addresses, health command/timeout, submit
timeout, `address_protocol`, secret-file path, dry-run mode, and notify
hook may differ per host. Node IDs and listen addresses must still match
the shared peer roster, and secret files must resolve to the same value.
The effective VIP interface name must match on every node; it is part of
the fingerprint, not a per-host override. `eth0.100` and `eth0` plus
`vlan: 100` resolve identically, but `ens3.100` is a different identity.
Keep each instance's address protocol stable across restarts and follow
the README's marker-rotation procedure before changing it. Changes
confined to node-local settings may use rolling maintenance below if
the effective cluster-wide settings remain unchanged.

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

Use the following procedure only for compatible binary versions with
unchanged effective cluster-wide settings. It does not apply to the
retained-holder upgrade, protocol version 3 upgrade or the VIP/config
changes described above. For compatible maintenance, work on one node
at a time so the cluster keeps quorum:

1. `systemctl stop keepafloatd@nodeX`: wait for cleanup and check takeover
   on healthy survivors. Confirmed shutdown releases avoid the stale
   wait; failed handoff uses the ordinary fallback. There is no fixed
   failover-time guarantee.
2. Upgrade to a compatible binary or edit only node-local settings.
3. `systemctl start keepafloatd@nodeX` - it rejoins via Raft. VIPs rebalance evenly when
   `failback: true`; nopreempt nodes remain available for future orphaned VIPs without taking from
   healthy holders.
4. Confirm health (`journalctl`, `ip addr`) before moving to the next node.

During rolling maintenance, never take down a majority at once, or the cluster loses quorum and
every node unbinds its VIPs until quorum returns. The coordinated upgrade above deliberately
interrupts VIP service instead of running incompatible versions together.

The compatibility safeguards below do not make the retained-holder upgrade safe to roll.

## Protocol version 3 upgrade

Protocol version 3 adds exact boot identities and runtime admission to
tagged Raft requests and responses. Both listeners reject earlier
binaries. Stop every member, verify every managed VIP is released, install the same
new version on every member (including offline members), then start and verify the
cluster. This requires a service interruption. Do not roll this upgrade or run a
legacy process alongside the new cluster. Authentication never falls
back to earlier versions or the plaintext format. Existing internal
snapshot/semantic compatibility code does not change this upgrade
contract.

A new process waits through a restart quarantine before participating
in Raft. Local VIP activation has an additional, separate safety delay
after admission. The delay depends on the probe freshness lifetime and
configured VIP cleanup budget. See [runtime admission](runtime-admission.md)
for the timing assumptions, example durations and fencing limitations.

Current binaries exchange a non-secret fingerprint of the effective
cluster-wide consensus settings before the first Raft frame. Concrete
mismatches are rejected immediately, even before identity enforcement is
activated. A node that repeatedly observes a roster majority sharing one
different fingerprint logs `cluster configuration mismatch`, stops
reconciliation, unbinds its locally tracked VIPs, shuts down Raft and
exits with status 4. Repair its config before restarting it. Unrelated
mismatches do not add into a false majority.

A committed admitted genesis enables configuration identity enforcement
and V2 failover semantics together. Missing identity is not a substitute
for runtime admission. Every connection also requires mutual HMAC
authentication, separately from the non-secret fingerprint.

## Security and networking

- **Shared secret is required.** Configure exactly one of `cluster_secret`
  and `cluster_secret_file`. Every node must resolve the same random
  secret, 32 to 256 UTF-8 bytes, with no whitespace or controls. It
  authenticates both the Raft and submit channels. Length is not an
  entropy check; generate a unique value, for example `openssl rand -hex 32`.
- **Secret files** must be regular UTF-8 files, optionally ending in one
  LF or CRLF. Relative paths resolve against the YAML directory, not the
  process working directory. Reads are bounded and happen once during
  startup. Config Debug output redacts the resolved value.
- **Sender binding.** A node may only submit state for itself - the leader checks the connection's
  source address against the claimed node's advertised address and checks the payload
  identity against the authenticated initiator. Shared-IP peers share the same identity boundary;
  the shared key does not distinguish individual key holders.
- **Bind to concrete addresses.** `raft_listen` and `client_submit_listen` must be the
  peer-reachable addresses advertised in `peers` (a wildcard `0.0.0.0` bind is rejected).
- **Firewall the two TCP ports** (`raft_listen`, `client_submit_listen`, e.g. `7000`/`7001`) to the
  cluster peers only.
- Keep `/etc/keepafloatd/config*.yaml` root-owned and mode `0600` - these files hold the secret.
  The DEB and RPM post-install scripts enforce this for regular files without following symlinks.
- When using `cluster_secret_file`, provision that file with the same
  ownership and mode yourself. The daemon warns about group/other access
  to secret-bearing files but does not change their permissions.
- Both listeners use mutual HMAC-SHA256 challenge-response. The secret never
  travels on the wire. Nonces, endpoint IDs, listener, roles, version, epoch and
  capability flags are authenticated. Captured proofs still permit offline
  guessing of weak secrets.
- All RPCs remain plaintext. Admission, management and release records
  have connection-bound MACs; admission and management also bind their
  outstanding request challenge. Ordinary Raft and status records lack
  record integrity and sequence-number replay defense. An active
  intermediary can relay the handshake and alter those ordinary records.
  Shared-key possession is not an exclusive per-node credential. Protect
  untrusted paths with an authenticated encrypted VPN/IPsec/mTLS tunnel.
  See [authentication.md](authentication.md).

Before upgrading, check existing secrets and VIP bindings against the
current validation rules. Older binaries accepted short secrets and
silently discarded conflicting duplicate VIPs. Fix those configurations
before deploying. To rotate a shared secret, use a coordinated maintenance
window: stop all members, confirm all managed VIPs are released, install
the same new secret on every member (including offline nodes), then start
the cluster and verify ownership. Do not rotate by sequential restarts:
members with different secrets cannot communicate. All binaries using
the mutual-authentication protocol support `cluster_secret_file`.
An upgrade from a binary without that option must use the coordinated
maintenance window above; the file option does not enable a rolling
wire-protocol upgrade.

## Troubleshooting

**The daemon exits immediately with `cluster_secret is required`.**
Set exactly one secret source, with the same valid secret on every node.
The secret must contain 32 to 256 UTF-8 bytes without whitespace or controls.

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

**Logs show `accept failed; retrying`.**
The Raft or submit listener could not accept a new TCP connection, for
example because the process ran out of file descriptors. It logs the OS
error and retries after one second without restarting the daemon.
Existing connection handlers and shutdown remain active during the wait.
Check the reported resource or network error if it persists. This does
not bypass health or consensus fencing, and a failure to bind a listening
socket at startup still stops the daemon.

**A submit is rejected with "came from … but its advertised address is …".**
The node's source IP doesn't match its `client_submit_address` in `peers`. This happens with NAT or
multi-homed hosts between nodes; put the nodes on a flat, directly-reachable segment (the v1 model),
or advertise the address the node actually egresses from.

**A VIP doesn't move off a failed node.**
For an explicit probe failure, check `failover_delay_secs`; the failure must remain continuous for
that duration. For silent death, the old holder becomes ineligible when
its committed probe-round lag exceeds
`floor(effective_stale_secs * 1000 / health.interval_ms)`, not a raw
wall-clock count of `health.stale_secs`. Slow probes and commits affect
how quickly the rounds advance. Without a confirmed release, takeover
also waits for the activation round and a local proof-expiry/cleanup
safety delay; a new old-holder probe resets that local wait. See the
[unacknowledged takeover allowance](#unacknowledged-takeover-allowance)
for its per-VIP cleanup component.
Check quorum, local health and the previous holder's release/cleanup
logs before changing timeouts. A fresh unhealthy report alone cannot
prove that the previous holder removed its VIP. See
[Silent holder death / partition](../README.md#silent-holder-death--partition).

**A recovered node never gets VIPs back.**
With `failback: false` (nopreempt), a recovered node does not take VIPs from healthy holders. It
remains eligible when a holder fails and an orphan needs placement. Use `failback: true` with
`failback_delay_secs` if recovered capacity should also trigger proactive even rebalancing. A node
whose first probe fails during startup may enter the pool after its first successful probe.

**The health check flaps.**
Make sure the health command normally finishes well before `health.timeout_ms`
and exits `0` only when the service is truly ready. The timeout must be strictly
below the effective stale window, rounded down to whole probe intervals as
described in the README's Health checks section. Leave room for command cleanup,
scheduling and publishing the result as well. Startup rejects a timeout at or
above this window. A probe that forks a lingering child is fine because the
drain is time-bounded, but a slow probe near the timeout will flap.
