# keepafloatd real-cluster test campaign

Reusable SSH-driven scenarios that exercise keepafloatd on a **real croit cluster** -
covering the things the docker-compose e2e suite and unit tests cannot: actual kernel
`ip addr` VIP binding, gratuitous ARP, real RGW service failover with end-to-end S3,
the notify hook, cluster-incarnation fencing, and endurance across openraft snapshot/purge
cycles (the path the `install_snapshot` crash lived in).

## Why this exists (vs. the docker e2e suite)

`tests/e2e/` already proves failover *logic* in containers (kill-holder, partition,
sticky-vip, stale-survivor, …). This harness proves the same behaviours **on real hardware
with real OS effects and a real service (Ceph RGW)** - so a pass means the data path
actually moved, not just that a log line appeared. It runs against a live cluster over SSH
rather than in CI.

## Layout

```
tests/realcluster/
  env.example.sh    # sanitized configuration template
  env.sh            # private local configuration, ignored and never published
  lib.sh            # SSH plumbing (jump host) + assertions ported from tests/e2e/scripts/lib.sh
  scenario.sh       # per-scenario lifecycle: check/check_eq, evidence capture, restore-to-baseline
  run-all.sh        # runs scenarios in order, tallies, writes results/report.md
  run-all-guard.sh  # exact config/binary restoration on exit and interruption
  final-audit.sh    # verifies cleanup and the configured candidate BuildID
  self-test.sh      # local helper regressions, no live-cluster access
  safety-self-test.sh # authentication, evidence, and scoped cleanup regressions
  evidence.sh       # process-scoped journal checks for A5, D7, D19 and C2
  evidence-self-test.sh # scenario regressions with local fake boundaries
  protocol-self-test.sh # exact boot identity and startup/activation regressions
  journal-evidence.py # strict journal evidence parser (Python standard library)
  journal-evidence-test.py # parser regressions without cluster access
  raft-deadline-probe.py # authenticated network-deadline helper for D25
  soak.sh           # optional long disruption soak; exact-config cleanup is automatic
  scenarios/*.sh    # one self-checking scenario per behaviour
  results/          # per-scenario .log evidence + report.md (generated)
```

## How it reaches the cluster

The cluster nodes are reachable **only from the configured management node**. Every node command
is proxied: `workstation --ssh--> mgmt --ssh(mgmt key)--> node`. The outer hop
uses SSH ControlMaster multiplexing (fast); per-node queries are batched and run in parallel
so a full VIP/holder snapshot of the cluster takes <1s.

Prerequisite: the mgmt node's `~/.ssh/id_ed25519` must be authorised on the nodes. On croit
this is set durably via the API: `PUT /api/server-access/keys` (croitd distributes it and
re-applies it across restarts).

## Running

These scenarios stop services, change configs and binaries, partition nodes, and write S3 test
objects. Use only an explicitly authorized disposable cluster, never a production cluster.
The runner restores captured configs and binaries, but test users, buckets, objects, and
evidence can remain. Review the selected scenarios and prepare recovery access first.

The controller requires Linux, Bash 4+, GNU coreutils (including `timeout` and `base64 -w0`),
OpenSSH, `jq`, `openssl`, Python 3, and `setsid`. The management node needs Bash, `s3cmd`, `curl`,
Python 3, and the Ceph administration tools. The nodes need systemd, iproute2, iptables,
`curl`, `jq`, Python 3, and the configured Ceph/RGW/HAProxy services. Startup
checks use node-local monotonic timestamps and process identity in every scenario;
D25 additionally needs PyYAML on the nodes.
Configure the management hop and node access in the local `env.sh`. Its example SSH options
disable host-key verification for disposable lab nodes; do not reuse them for production access.

From the repository root, validate the exported harness without contacting any cluster:

```bash
./scripts/ci/test/realcluster-harness.sh
```

For a live campaign, copy `env.example.sh` to the ignored `env.sh` and replace its example values
before running any scenario. Configure three nodes, three VIPs, service instance names, mutation
addresses, the candidate BuildID, and access credentials. D15/D16 now cover full restart and legacy-wire rejection;
the full run still includes all 35 scenarios.

```bash
cd tests/realcluster
cp env.example.sh env.sh  # first setup only; edit this private file before proceeding
export REALCLUSTER_AUDIT_SINCE="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
./run-all.sh              # all scenarios, in order
./run-all.sh 'A*'         # just the A-series (glob over scenarios/<glob>.sh)
./run-all.sh 00_baseline  # one scenario
./self-test.sh            # local fail-closed harness regressions; no cluster access
cat results/report.md     # summary table; results/<scenario>.log for per-step evidence
./soak.sh 3               # optional three-hour disruption soak
./final-audit.sh          # read-only checks, using the campaign start exported above
```

Set `REALCLUSTER_AUDIT_SINCE` before starting the campaign and retain that timestamp if
running the final audit in another shell. The audit refuses to run without it; setting it
only after the tests would hide earlier failures. Credentials may be supplied through
`CROIT_PASS`, `S3_ACCESS`, and `S3_SECRET` in the process environment; the example preserves them.

Each scenario **restores the cluster to baseline** (3 nodes active, fixed binary, 3 VIPs
uniquely + evenly held, RGW up, partitions healed) before the next one, and `run-all.sh`
re-asserts steady state up front. The campaign guard freezes each node's exact config and binary
hash, refuses to overwrite a campaign backup, verifies exact restoration, and rejects
every campaign-owned runtime artifact before capturing state. Cleanup removes only the known
campaign backup names and tagged firewall rules; an unknown operator backup is preserved.
`final-audit.sh` can therefore reject leaked timing, secret, notify, VIP, marker, firewall, or
binary changes without deleting unrelated state. A scenario that leaves the cluster dirty fails,
including when the campaign is interrupted or reaches its external timeout.

Remote mutations, parallel restarts, and journal/file counters fail if any node or evidence source
is unreadable. A readable source with no matching events reports zero; transport or command errors
never do. Leader evidence comes only from each active daemon's current systemd invocation, and all
active nodes must report the same configured leader. Every SSH execution has a separate 60-second
command deadline in addition to its connect timeout. The local `self-test.sh` suite covers these
failure paths, inline and block YAML health commands, exact campaign restoration, real data-path
recovery, and sustained convergence predicates.

Leader agreement compares the complete physical-node and boot identity. Physical IDs
are used separately to select a host for an operation. Startup waits read
`startup_safety_wait_ms` from the current systemd invocation, not from an older boot
or a duplicated configuration formula. Recovery observations start only after that
invocation records runtime admission and its `vip_activation_ms` has elapsed on the
node's monotonic clock. Kernel uniqueness is checked throughout that wait. Missing
admission is pending; malformed evidence, transport failures, or a changed invocation
fail the check. Normal failover deadlines and post-activation observation windows
are unchanged.

Config restoration attempts every node and reports every failure. A retry can restore
remaining copies even if an earlier node's copy was already consumed. Missing copies
still cause failure: inspect the reported nodes and confirm their configs before resuming
the campaign. Missing backup files alone are not proof of successful restoration.

The campaign guard also attempts every reachable node after a failure and does not
restart the cluster after an incomplete restore. A stopped node whose backup was
already consumed can pass a retry only if its current file matches the captured
SHA-256 hash. This applies independently to the config and binary, including an
interrupted restore between the two moves. Corrupt backups or mismatched live
files keep the guard active and abort the campaign.

`soak.sh` is separate from `run-all.sh` because its default duration is three hours. It cycles
through all four disruption types deterministically and restores the exact configs it found, even
when interrupted. `SOAK_SECONDS` and `SOAK_CYCLE_SECONDS` provide a bounded smoke mode; such a run
passes only after exercising all four disruption types, but is not evidence of multi-hour
endurance. A clean full reform must recover the deterministic even spread. Restart, SIGKILL, and
partition recovery use the nopreempt invariant instead: every daemon is active, every node agrees
on the current leader, and every VIP is uniquely reachable; a safe uneven spread is accepted.

Config capture refuses existing backups and rolls back confirmed copies if a later node fails.
No soak disruption or recovery command runs until every config is captured. If SSH fails after
a remote copy, its result may be unknown: inspect the named backup on that node before retrying.
Rollback errors are reported, not ignored, and the final audit rejects leftover soak backups.

Copy `env.example.sh` to the ignored `env.sh`, then configure node IPs, instances, VIPs, disposable
mutation/administrator addresses, expected BuildID, and SSH settings. The harness discovers the `kafdtest` S3 credentials on the management
node when they are not exported; it never writes them to evidence logs.

## Scenario coverage

| ID | Behaviour validated | Ground truth |
|----|---------------------|--------------|
| 00_baseline | steady state: even unique VIPs, one leader, fixed binary, Ceph healthy | `ip addr`, ping, ceph |
| **A - real OS effects** | | |
| A1_ip_bind_unbind | VIP actually leaves a stopped holder's kernel and appears on a new one | `ip addr` + ping |
| A2_arp | gratuitous ARP repoints the neighbor MAC to the new holder after failover | `ip neigh` MAC |
| A3_vlan_binding | a VLAN-tagged VIP binds uniquely on the configured sub-interface and nowhere on the base interface | `ip -4 addr` |
| A4_cidr_ipv6 | explicit IPv4 CIDR and default IPv6 prefix bind with the right family/prefix and one holder | `ip -4/-6 addr` |
| A5_startup_cleanup | after SIGKILL, the new process reports successful reclamation of the exact orphan VIP; ownership converges without permanent double-bind | invocation-scoped journal + `ip addr` |
| **B - real service failover** | | |
| B1_rgw_failover_s3 | local RGW/front-end death → health fails → VIPs move; **S3 put/get through the VIP stays available** | s3cmd put/get |
| B3_notify_hook | notify scripts fire `MASTER`, local-failure `FAULT`, and cluster-driven `BACKUP`; recovery evidence is isolated from startup events | notify log file |
| **C - scale + endurance** | | |
| C2_endurance_snapshots | every node completes two distinct snapshots followed by advancing purge commands, without restart or Raft errors; repeated samples retain unique VIPs | invocation-scoped journal + sampled `ip addr` |
| C4_cascading_failures | rotating SIGKILL/restart gives each VIP exactly one live holder and startup cleanup removes every orphan | `ip addr` |
| C5_config_mutation | adding/removing a fourth VIP gives it exactly one holder and cleans it from every kernel afterward | config + `ip addr` |
| **D - failover semantics on real HW** | | |
| D2_consensus_self_fence | a holder isolated from the leader self-unbinds (consensus_fresh=false) while still running | `ip addr` + log |
| D3_silent_death_staleness | the current leader/VIP holder is SIGKILLed; no early survivor bind is allowed, then survivors must re-elect before stale reassignment | leader journals + sustained `ip addr` timing |
| D5_failback_timing | a recovered node observes the failback delay before reclaiming VIPs | node-local journal timing + `ip addr` |
| D6_leader_kill | killing the Raft leader elects a new one; VIPs stay correctly held | leader log + `ip addr` |
| D7_stale_survivor | an isolated owner's original boot expires and releases its VIPs before healing; a fresh boot completes a committed learner promotion into the reformed majority | invocation-scoped expiry, cleanup and exact-boot promotion journal + systemd + `ip addr` |
| D8_full_outage_recovery | stop all, restart a majority → cluster reforms and redistributes VIPs | `ip addr` + ping |
| D9_cluster_secret | a node with the wrong `cluster_secret` is refused; matching it lets it rejoin | `ip addr` + log |
| D10_asymmetric_partition | twenty near-atomic snapshots show no simultaneous double-bind during a one-way partition | parallel `ip addr` snapshots |
| D11_failover_delay_nopreempt | a six-second failure delay has a measured lower bound; recovered nopreempt node accepts a later orphan | monotonic timing + `ip addr` |
| D12_startup_recovery | `unknown -> unhealthy -> healthy` startup with `failback:false` joins normal 1/1/1 balancing | sentinel probe + `ip addr` |
| D13_failure_delay_reset | 500 ms probe rounds enforce a three-second delay and a healthy interval resets an interrupted failure streak | monotonic timing + sentinel probe |
| D14_silent_nopreempt_fallback | a silently stale owner returns without preempting but remains eligible for an orphan | SIGKILL + sentinel probe + `ip addr` |
| D15_mixed_version_activation | coordinated full stop releases all VIPs before every member restarts on the candidate | BuildID + `ip addr` + service availability |
| D16_chained_legacy_activation | plaintext legacy framing is rejected on both listeners, then authenticated status and service still work | wire rejection + authenticated status + VIP availability |
| D17_nopreempt_restart_leader_churn | recovered-node and leader restarts preserve nopreempt ownership; only orphaned VIPs may move | journal + sustained `ip addr` + ping |
| D18_ownerless_gap_reassignment | all nodes become unhealthy and remove every VIP; one recovered node safely reacquires the ownerless set | sentinel probe + sustained `ip addr` + ping |
| D19_config_identity | the original old-config owner confirms fatal mismatch and releases every held VIP without rebinding; exact repair restores service | invocation-scoped journal + sustained `ip addr` |
| D20_config_identity_equivalence | nopreempt nodes with unequal ignored failback delays and equivalent missed-probe thresholds form one cluster without a false config fence | config fingerprint + daemon liveness + unique `ip addr` ownership |
| D21_fresh_unhealthy_delete_fence | a fresh unhealthy holder whose kernel delete fails keeps the replacement fenced; a non-zero delete with exact-host absence safely releases | injected `ip` status + sustained `ip addr` snapshots + journal |
| D22_startup_endpoint_failures | an occupied submit socket stops the daemon, and an IPv4-mapped alias of its Raft endpoint is rejected before startup | real TCP bind + systemd lifecycle + config diagnostics |
| D23_crash_removed_vip_cleanup | a SIGKILL orphan removed from every config is reclaimed from its instance marker before rejoin, while unmarked and differently marked addresses survive | protocol-derived-table `throw` route + config + systemd lifecycle |
| D24_ipv6_crash_removed_vip_cleanup | the same crash-time config-removal cleanup reclaims an IPv6 `/128` and its IPv6 marker before a quorum can form | `ip -6 addr` + IPv6 `throw` route + systemd lifecycle |
| D25_authenticated_raft_deadlines | authenticated idle, partial-prefix, partial-body, and trickled frames expire; renewed status requests succeed and every voter/VIP remains available | source-bound TCP + monotonic close times + agreed leader + unique reachable VIPs |
| D26_isolated_health_proof | a delayed health probe cannot keep an isolated leader bound; its expired process stops and a fresh boot rejoins after healing | exact candidate daemons with dry-run VIPs in a private namespace on the real kernel |

A5/D7/D19/C2 pin evidence to the node's boot and systemd invocation, using
node-local monotonic timestamps. A5 requires successful reclamation of the exact
orphan on the expected interface by a different invocation. D7 requires the
original owner's exact terminal runtime-permission expiry and release of every
VIP it held before isolation, without a later rebind. It checks a changed service
invocation on the same OS boot before healing, then requires a different admitted
runtime boot for the same physical node and its exact committed learner promotion
in the reformed peers' pinned journals. An arbitrary restart, generic failure or
epoch message cannot satisfy this evidence. Before healing, only the specified
surviving majority must agree on a current leader; an isolated fresh process has
no leader transition yet. Read errors on either surviving peer fail the check.
Restart activation waits use the current invocation's reported timing policy,
with continuous duplicate-VIP checks, not a larger fixed timeout. This tests
terminal admission expiry and fresh-boot recovery, not execution of the separate
incarnation guard. D7 requires the explicit `committed learner promotion` record
from application of the committed stable membership, independent of whether the
normal acknowledgement or interrupted-operation recovery finishes the join.
A healthy process alone is not accepted as proof of a committed join.
D19 checks every VIP held before mutation,
accepts safe release on quorum loss before the fatal config fence or during
shutdown, and rejects missing release or a later rebind. It does not claim that
an early release proves the shutdown cleanup path executed.

C2 counts distinct `BuildSnapshotDone` records followed by advancing purge
commands on each original invocation, not queued builds or aggregate node
totals. Each node's observation window starts after the accelerated baseline
is established. A restart, malformed/unreadable evidence, or a Raft failure
within that window aborts the scenario. Kernel ownership is sampled between
journal checks with a two-second pause; this is not continuous observation and
cannot exclude a shorter transient between samples. Purge log messages record
issued commands, not an independent
inspection of the in-memory storage contents.

D5/D11/D13 timestamp the health-changing action on the victim and use the first
matching VIP event in that node's journal. D5 checks the first returning configured
VIP after sentinel recovery. D11 still stops the real RGW and HAProxy services;
its clock starts before submitting the node-local stop request, not when polling
later observes the missing VIP. D13 times the second sentinel fault and release.
The evidence is restricted to the captured boot and daemon invocation. SSH and
polling latency do not count toward the configured delay; missing, malformed or
unreadable evidence fails the check. Daemon-side ARP or marker cleanup and journal
delivery can delay the logged event after the kernel change, so these checks do
not bound that remaining latency.

D26 runs three copies of the exact candidate on the real cluster kernel inside a disposable
network namespace. Its VIP effects are dry-run: it checks that a blocked health probe cannot
keep an isolated leader logically bound after a survivor takes over. The isolated process
expires terminally; a fresh process rejoins after healing. The ordinary D2/A1 scenarios
separately verify real kernel withdrawal. The whole probe includes two startup fences
reported by the candidate; individual SSH commands retain their existing deadline.
D26 requires Python pidfd APIs and kernel support for `pidfd_open` and
`pidfd_send_signal`; a syscall preflight runs before namespace creation or
daemon startup. Cleanup pins process identities and requires a matching namespace
creation receipt. D26 removes its namespace, firewall rules, helper files, and
probe fixtures. Cleanup failures preserve diagnostics and fail the scenario.

D25 uses `raft-deadline-probe.py` with Python 3 and PyYAML on each source node. The helper reads
the source node's configured identity and secret locally, negotiates the target's configuration
fingerprint, and confirms authentication before applying pressure. It opens four connections,
leaving capacity for ordinary peer traffic, and closes every fixture socket even on failure.
It does not write configs, submit Raft mutations, or print authentication material. Both premature
rejection and missing deadlines fail the scenario; a new authenticated status exchange must work
after cleanup. The scenario checks sustained cluster availability after each source/target pair.

## What this campaign found

The startup-recovery scenario covers the direct `unknown -> unhealthy -> healthy` path.
The expanded state-transition matrix also exercises
explicit failover timing, silent-recovery history, recovered-nopreempt orphan eligibility, exact
fractional probe timing, coordinated restart, legacy-wire rejection, nopreempt
restart persistence, and ownerless-gap activation. D11-D18
cover those paths on real nodes.

D19-D20 cover both sides of configuration identity: a behavior-changing mismatch must fence the
minority, while syntactic differences that have the same effective behavior must not split the
cluster.

The full-outage D8 scenario also exposed a graceful-shutdown race: SIGTERM could interrupt a
periodic `ip addr replace`, whose failure path removed an already-bound address from in-memory
tracking before `unbind_all` ran. The daemon now clears tracking only after a failed first bind and
retains it after a failed reassert. D8 proves all VIPs leave every kernel during a full graceful
outage before the majority reforms.

The EL9 cleanup audit exposed another blind spot: the original orphan test assumed Linux address
protocol metadata was available on the target kernel. The real 5.14/iproute2 surface disproved that
assumption. D23 and D24 inspect the actual IPv4/IPv6 address and protocol-derived `throw` route
state, crash the holder, remove the VIP from configuration, and prove cleanup occurs before quorum
rejoin while unrelated addresses and another instance's marker survive.

## Results and release scope

The coverage table describes what the scripts assert, not a record of a successful run on any
particular release. CI checks inventory, syntax, and local self-tests; it does not contact a live
cluster. A live campaign produces `results/report.md` and per-scenario logs. Keep these private
because they contain topology and operational evidence. A public summary must name the tested
commit, environment, executed scenarios, failures, and omissions, with sensitive data removed.

D15/D16 retain their scenario identifiers for inventory continuity, but their old
mixed-version activation contracts are obsolete. D15 exercises coordinated full
restart with release-before-start checks. D16 sends deliberately secret-free old
wire headers to both listeners and requires rejection plus a successful new
authenticated status probe. No legacy executable is required or started.
The same shared helper, `../e2e/scripts/auth_wire.py`, is bundled in memory with
the probe for D16 and D25. These scenarios do not test transport encryption or
post-authentication integrity. Follow [operations.md](../../docs/operations.md)
for production upgrades; plaintext-secret and mutual-authentication binaries
must never be rolled together.

## Notes / caveats

- Kill/partition/restart effects are confined to the explicitly configured lab. The harness
  re-asserts that `ceph health` recovers to `HEALTH_OK` in `restore_baseline`.
- All 35 scenarios keep the fixed candidate binary on every node. D15/D16
  explicitly test the new full-restart and legacy-rejection contracts.
- Health failure is induced by stopping both the node's HAProxy front end and RGW backend. Current
  generated configs probe HAProxy on `127.0.0.1:9400/healthz`; stopping RGW alone leaves that endpoint
  healthy, so both services must stop to exercise the configured probe and real S3 data path together.
  Recovery and final audit require the concrete templated RGW unit, HAProxy, that health endpoint,
  and a successful HTTP request to the actual RGW backend on `127.0.0.1:80`; the aggregate
  `ceph-radosgw.target` is not accepted as proof.
- B1 idempotently provisions its dedicated `kafdtest` RGW user if the lab does not already have it,
  verifies or creates the bucket through a VIP, and stops immediately if fixture setup fails.
- Config-mutating cases write every node's config first and then perform one coordinated full
  reform. Do not use sequential restarts for a wire-protocol upgrade: they can create
  diskless-incarnation splits and false negatives.
