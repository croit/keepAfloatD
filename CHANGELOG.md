# Changelog

All notable changes to keepafloatd. Versions follow the calendar scheme
`vYYMM.N` used by the release pipelines. Issue numbers refer to the
project tracker. Release preparation may precede publication; the
published release pages confirm which artifacts are available.

## v2610.2 (2026-10-30)

Changes since `v2610.1`.

### Upgrade notes

- Protocol version 3 and the configuration format are unchanged, and
  `v2610.2` members exchange the same messages as `v2610.1` members.
  A cluster mixing the two versions was not tested on a live cluster.
- The packaged systemd unit gains `StartLimitIntervalSec=2h` and
  `StartLimitBurst=3`: it allows three starts per two-hour window,
  including the first one, and refuses the next with
  `start-limit-hit`. Before a deliberate restart, run
  `systemctl reset-failed keepafloatd@<instance>` so maintenance does
  not use up that budget; see
  [protective failure and repair policies](docs/operations.md#protective-failure-and-repair-policies).
  The rpm `postinst` now reloads systemd, so an upgraded unit applies
  before the next start.
- The new systemd status text relies on the packaged unit's existing
  `NotifyAccess=exec`. A custom unit must allow notifications from the
  daemon's main process, or it reports nothing.

### VIP command timeouts

- A VIP bind or reassert command that misses its deadline no longer
  keeps the node out of placement until an operator restarts it (#44).
  The daemon stops as on `SIGHUP` and exits with a failure status. A
  supervisor that restarts failed processes, such as the packaged
  unit, starts it again, and the new process re-enters placement after
  startup cleanup and runtime admission. Without such a supervisor the
  daemon stays stopped. A bind that fails outright, such as one naming
  a missing interface, still latches local FAULT, and cleanup failures
  are still retried.
- A host whose commands keep timing out ends in a failed unit once the
  start limit is reached, instead of moving VIPs back and forth.
- A VIP release is acknowledged only once every cancelled address
  command has been killed and reaped (#46). A release could previously
  be acknowledged while a killed `ip` child could still re-add the
  address, so two nodes could hold the VIP. While such a child may
  still run, no release is acknowledged and peers take the VIP over
  only after the stale-holder fences, which grow with the health
  timing and the configured VIP count; see
  [unacknowledged takeover allowance](docs/operations.md#unacknowledged-takeover-allowance).
  A cancelled command that cannot be reaped at all also stops the
  daemon for a supervised restart.
- A timeout restart can still bypass the stale-holder wait when every
  cancelled command was reaped before cleanup began, the cleanup
  succeeded and the release acknowledgement commits within the handoff
  budget.

### Observability

- Under systemd, after startup cleanup, the status text reads
  `awaiting admission` while the member waits to vote in its cluster,
  and `admitted` from then on (#47). Read it with
  `systemctl show -p StatusText keepafloatd@<instance>`. `admitted`
  means the member counts toward quorum, not that VIP activation has
  finished. Rolling maintenance waits for `admitted` before moving to
  the next node, as the rolling upgrade steps in
  [the operations guide](docs/operations.md) describe.

### Validation and known limits

- The release candidate passed all 37 GitLab jobs on main, including
  Docker E2E and source-export checks, with 94.89 percent line
  coverage. Release publication runs its own checks against the final
  commit and version-stamped archive.
- The full 36-scenario live Ceph campaign passed on a three-node stand
  with the release candidate, including the new bind-timeout restart
  scenario. A second run with the packaged unit confirmed both status
  text values and clean stops in seven restart and failure scenarios.
- The causes of the earlier VIP command timeouts in CI and on the Ceph
  test stand remain unexplained. A bind or reassert timeout now leads
  to a supervised restart instead of a latched fault.
- Runtime admission assumptions and the physical fencing limits listed
  for `v2610.0` still apply.

## v2610.1 (2026-10-28)

Changes since `v2610.0`.

### Upgrade notes

- Protocol version 3, the configuration format and the packaged
  systemd unit are unchanged. Upgrading from a release before
  `v2610.0` still requires the
  [protocol upgrade procedure](docs/operations.md#protocol-version-3-upgrade).
- OpenRaft moves from `0.10.0-alpha.32` to `0.10.0-alpha.37`. Snapshot
  metadata stays readable in both directions. A cluster mixing
  `v2610.0` and `v2610.1` members was not tested. The tested path is
  to stop every member, verify that its VIPs are released and start
  all members on `v2610.1`, as in the
  [coordinated upgrade](docs/operations.md#coordinated-upgrade-for-retained-holder-fencing)
  steps. This interrupts VIP service.

### VIP continuity

- Keep a healthy, completed VIP binding through a short leader or
  transport outage. In `v2610.0` any failed health or release submit
  withdrew every local VIP. The binding is kept only until the earlier
  of its health-proof deadline and the admission deadline accepted
  with that proof, and it is never reasserted or newly acquired without
  fresh authority. Health loss, an explicit rejection, reassignment and
  expiry still withdraw the VIP. Elections longer than the remaining
  proof lifetime can still interrupt service.
- The binding is also kept while a peer restarts, while a periodic
  reassert cannot reach the management service, and while another
  configured member has a wrong cluster secret or a different
  configuration. Such members never count toward quorum. `MASTER`
  notifications and neighbor announcements of a VIP that stays
  authorized still run to completion; notification delivery remains
  best effort.
- A rejoining node does not attract a VIP before its activation fence
  lets it bind that address, so the current holder keeps serving it
  until the replacement is ready.
- A graceful stop no longer skips the VIP handoff when admission state
  is briefly busy. Handoff still needs a working quorum and must finish
  within `submit_timeout_ms`; otherwise the stale-holder fallback
  applies.
- A request addressed to a peer's previous process no longer closes
  the restarted peer's current Raft connection, which could delay its
  recovery.

### Dependencies

- Upgrade tokio to 1.54.1 for RUSTSEC-2026-0386.
- Replace the unmaintained `serde_yaml` with `yaml_serde` 0.10.7 for
  RUSTSEC-2024-0449. Configuration files are parsed as before.

### Validation and known limits

- The release candidate passed all 37 GitLab jobs on main, including
  Docker E2E, source-export checks and offline archive builds. Its Rust
  run passed 990 unit and 28 integration tests with 94.84 percent line
  coverage. Release publication runs its own checks against the final
  commit and version-stamped archive.
- Real-cluster scenarios gained stricter snapshot, leader and
  ownership evidence checks and isolated crash injection. The full
  35-scenario live Ceph campaign was not rerun for this version.
- Earlier VIP address commands that missed their deadline in CI and on
  the Ceph test stand remain unexplained. A bind timeout latches local
  FAULT; repair the cause and restart the affected daemon.
- Runtime admission assumptions and the physical fencing limits listed
  for `v2610.0` still apply. Native ARM execution and a live campaign
  of the published artifacts are not established by cross-compilation
  or Docker CI.

## v2610.0 (2026-10-07)

Changes since `v2609.1`.

### Breaking upgrade requirements

- Protocol version 3 is incompatible with earlier releases. Stop every
  instance and prevent automatic restart, verify that all managed VIPs
  and ownership markers are absent, then replace every binary and start
  all members on the same version and configuration. Include offline
  members before allowing them to rejoin. This interrupts VIP service;
  a rolling or mixed-version upgrade is not supported. Follow the
  [protocol upgrade procedure](docs/operations.md#protocol-version-3-upgrade).
- Configure exactly one of `cluster_secret` and `cluster_secret_file`.
  Every member must resolve the same random, cluster-unique secret of
  32 to 256 UTF-8 bytes without whitespace or control characters.
  Public placeholders are rejected. Keep secret-bearing files
  root-owned and mode `0600`; resolve invalid secrets, conflicting VIP
  bindings and invalid timing settings before starting the new version.
- Each process now has a fresh boot identity and requires runtime
  permission from a physical majority before participating in Raft.
  Restart quarantine and a separate VIP activation fence add startup
  delay. The shipped three-node, two-VIP examples have a combined
  minimum safety wait of 208.4 seconds, before election, health and
  network delays. An active systemd unit is not a readiness signal.
  See [runtime admission](docs/runtime-admission.md) for calculations
  and assumptions. No Raft or admission state is persisted to disk.

### Security and recovery

- Replace plaintext-secret exchanges with mutual HMAC-SHA256
  challenge-response on both listeners. Tagged requests bind peer and
  boot identities; admission, management and release records have
  connection-bound MACs. There is no legacy authentication fallback.
  This is not encryption: ordinary Raft and status records still lack
  record integrity and sequence-number replay protection. Use an
  authenticated encrypted tunnel on untrusted networks. See the
  [authentication boundary](docs/authentication.md).
- Require physical-majority admission, immutable genesis and committed
  learner transitions for diskless restarts and rejoining. Expired
  permission cannot be revived within the same process; run under a
  supervisor that performs cleanup and restarts failed instances.
- Add authenticated Pre-Vote to reduce disruption from an isolated
  follower returning to the cluster. Reconnects, listener errors and
  repeated rejected connections have bounded retry or warning behavior.
- Account for complete command, reap and cleanup work before takeover
  without a release acknowledgement. Failed cleanup never proves that
  a previous holder has released its VIPs.

### VIP lifecycle and operations

- Latch bind failures as local FAULT, stop reporting healthy eligibility
  and permit handoff only after verified cleanup. Repair the cause and
  restart the affected daemon; successful service probes alone do not
  clear the bind fault.
- Verify VIP absence independently of interface names, handle graceful
  shutdown handoff, and clean up before supervised SIGHUP/SIGQUIT
  restarts. Add `--cleanup-only` and packaged `ExecStopPost` recovery.
  Never run cleanup-only alongside a live instance.
- Extend the packaged systemd stop deadline at bounded work checkpoints
  for large cleanup inventories. A stalled task cannot renew its own
  deadline. Custom supervisors must support the notification mechanism.
- Send bounded IPv4 and IPv6 neighbor announcements in the background.
  Warn when IPv4 secondary-address promotion settings could remove
  neighboring addresses, without changing those settings automatically.
- Serialize notification hooks through a bounded queue. Overflow and
  shutdown cancellation are reported; hooks remain advisory and do not
  delay address cleanup or provide guaranteed delivery.

### Validation and known limits

- The preparation baseline passed all 37 GitLab jobs, including Docker
  E2E, source-export checks and offline archive builds. Its Rust run
  passed 895 unit and 28 integration tests, with 94.82 percent line
  coverage and the per-file coverage gate. Release publication runs
  its own checks against the final commit and version-stamped archive.
- All 35 documented real-cluster scenarios and their required helpers
  are included in the public export. Inventory, syntax and local
  harness self-tests passed. The full 35-scenario live Ceph campaign
  was not rerun for this version. The September campaign below is
  historical evidence for `v2609.1`, not validation of `v2610.0`.
- The legacy local helper `scripts/ci/e2e-dry-run.sh` is incompatible
  with current configuration validation and is not supported smoke
  coverage for this release. Use the documented Docker Compose suite
  instead. This limitation does not concern the separate 35-scenario
  real-cluster harness; the legacy helper's repair is deferred.
- Genuine leader loss can still withdraw healthy incumbents' VIPs
  under the existing consensus-freshness gates. The unfinished change
  to preserve those incumbents is not included. This release does not
  promise uninterrupted VIP service during leader changes.
- Runtime admission assumes bounded clock-rate differences, scheduling
  and address cleanup. It cannot physically fence a suspended machine
  or a kernel that retains addresses. Deployments requiring protection
  against those failures need independent fencing and verified cleanup.
- OpenRaft remains at `0.10.0-alpha.32`. Native ARM execution and a
  full live-cluster campaign of the final published artifacts are not
  established by cross-compilation or Docker CI.

## v2609.1 (2026-09-30)

Changes since `v2609.0`. The campaign and upgrade notes in this section
describe that historical release, not later versions.

### Fixed

- Keep unaffected VIPs bound when a renewed consensus proof revokes a
  different VIP. This avoids unnecessary failover and duplicate notify
  events. Loss of consensus proof or local health, or absence of a known
  leader, still withdraws every locally held VIP.
- Keep an already activated VIP bound when its predecessor's release
  acknowledgement arrives. An interrupted first ARP announcement remains
  pending so reconciliation can complete the announcement and `MASTER`
  notification.
- Export all 35 documented real-cluster scenarios with their required
  runners, helpers, sanitized configuration example and local
  self-tests.
  The `v2609.0` public export included only the D26 scenario and omitted
  required harness files. Private configurations, credentials and
  generated campaign evidence remain excluded.
- Reject false passes in real-cluster tests caused by failed SSH
  commands, unreadable evidence, stale process logs or incomplete
  restoration. Check failure-delay lower bounds and snapshot/purge
  progress explicitly.
  Restoration attempts all nodes and permits a safe retry after partial
  completion without treating a missing backup as proof of success.
- Wait for complete five-node convergence in the Docker rebalance test
  and use OS-assigned ports for single-node Raft fixtures to avoid port
  races.

### Release checks and regression coverage

- Check the documented scenario inventory and harness dependencies in
  the exported tree and packaged source archive. Local harness
  self-tests do not contact a live cluster.
- Build the exact version-stamped vendored source archive offline and
  run its harness checks before uploading it. Container publication
  waits for source validation; GitHub also waits for both binary
  architectures and DEB/RPM builds.
  Publication is not atomic across registries and release APIs: a later
  upload failure can still leave partial artifacts.
- Exercise health-proof rejection through running daemons and real
  notify hooks. The regression checks `BACKUP` on proof loss, recovery
  to `MASTER` and `FAULT` on an actual local health-check failure, with
  dry-run VIP operations.
- Check health-probe termination without treating zombie process
  entries as still-running descendants. The regressions still detect
  a live descendant left behind after cancellation or a successful
  probe; delayed reaping no longer causes a false failure.

### Upgrade notes

- Upgrades from versions older than `v2609.0` still require a
  coordinated stop of all voters before replacing binaries. Plan an
  interruption of VIP service; mixed-version tests do not establish
  rolling compatibility for that upgrade. Follow the
  [coordinated upgrade procedure](docs/operations.md#coordinated-upgrade-for-retained-holder-fencing).
- The campaign below does not establish rolling-upgrade compatibility
  from `v2609.0` to this release. Verify the exact version pair before
  scheduling a rolling upgrade.
- OpenRaft remains at `0.10.0-alpha.32`; this update does not change the
  dependency lockfile or the configuration format.

### Pre-release validation and limits

On 2026-09-29, the candidate completed all 35 documented scenarios on a
dedicated Ceph VM cluster: 35 passed, none failed or were skipped, and
none were retried. The final cleanup audit passed. This is evidence for
the tested candidate, not certification of later version-stamped
release artifacts or every deployment environment.

The archive-publication gates and additional health regression tests
described above were added after this campaign and checked separately.
The live campaign was not rerun for those changes.

- C2 checks two snapshot/purge cycles per node with sampled VIP
  ownership; it is not a three-hour soak or continuous observation of
  ownership.
- D26 uses real daemon processes and the kernel network stack, but its
  VIP operations are dry-run. Other scenarios include real kernel VIP
  failover.
- D15/D16 use a legacy fixture identified by hash and BuildID, not a
  verified historical release tag.
- D19 observed VIP withdrawal before fatal configuration fencing; this
  does not by itself prove that shutdown cleanup caused the withdrawal.
- Native ARM execution and final published-artifact checks are not
  covered by this campaign.

## v2609.0 (2026-09-28)

### Upgrade warning

This release introduced retained-holder fencing across ownerless gaps.
Upgrading from older versions requires stopping all old daemons and
verifying that their VIPs are absent before starting any new daemon.
Do not use a rolling upgrade. See the
[coordinated upgrade procedure](docs/operations.md#coordinated-upgrade-for-retained-holder-fencing).

### Fixed

- Preserve the previous VIP holder across ownerless gaps and require
  current, locally applied consensus proof before takeover. Expire that
  proof even while a health probe or release request is blocked.
- Release VIPs before exiting on `SIGHUP` for a supervised restart.
- #34: an IPv6 VIP is bound with `preferred_lft 0` so a holder's own outbound connections keep
  using the node's primary address (RFC 6724 source selection). IPv4 binds are unchanged.
- #19: a node whose first health report is unhealthy no longer enters the permanent no-failback
  block, so initial placement spreads VIPs across all recovering nodes.
- #22: `failover_delay_secs`, `failback_delay_secs` and `health.stale_secs` are honored with exact
  millisecond-to-probe-round conversion; nopreempt nodes stay eligible for orphaned VIPs; the new
  semantics activate only once every voter supports them.
- #24: every health probe, notify hook and `ip` call runs in its own process group with bounded
  output capture and reaping, so a forked grandchild can no longer wedge the health loop.
- #25: transport accept and reconnect tasks are supervised and joined on shutdown; outbound
  connect and handshake have deadlines; a failed submit listener terminates the daemon.
- #26: a versioned fingerprint of all cluster-wide settings is checked in every handshake and by
  the cluster guard; a node with different consensus inputs self-fences with exit status 4.
- #27: both listeners cap unauthenticated and authenticated connections, per-peer sources and
  in-flight frame bytes, and bound every handshake, read and write phase.
- #28: the shipped `cluster_secret` placeholder is rejected at load; Debian and RPM packages install
  the configuration root-owned with mode 0600 and repair permissive modes on upgrade.
- #29: `anyhow` updated past RUSTSEC-2026-0190; CI now runs `cargo deny check advisories`.
- #30: VIPs carry an inert route marker so a crash followed by a configuration change still
  reclaims the removed address on the next start.
- #31: the stale-survivor guard requires one identical foreign incarnation on a roster majority;
  distinct foreign incarnations no longer add up to a false quorum.
- #32: real-cluster and in-process lifecycle oracles fail closed on unreadable evidence.
- #33: the public source export verifies closure over tracked Rust sources, `deploy/` files and
  every path referenced from `Cargo.toml`; the RPM post-install script is now exported.
- Configuration: Raft timing relations are validated at load, before any host address is touched;
  unknown YAML keys are rejected by name instead of silently defaulting.
- Security: cluster secrets are compared in constant time; the per-source admission budget refuses
  addresses outside the roster instead of sharing a bucket for them.
- Network: the Raft transport can be started only once per process.
- `THIRD_PARTY_LICENSES.md` is regenerated from the lockfile and checked in CI.

### Changed

- OpenRaft 0.10.0-alpha.32 with `allow_log_reversion`, so a diskless follower that restarts blank
  is replayed instead of stalling replication.
- Initial VIP placement during a staggered start converges on a balanced mapping that depends on
  join order; the documentation now says so.
- Internal structure: the VIP module is split (effects, ownership, startup, reconcile, notify),
  child output capture lives in one place, the cluster guard decision is a pure type, and the bind
  policy no longer carries unused eligibility inputs.
- Release images carry `org.opencontainers.image.version` and `revision` labels.

### Known limitations

- Identified after release: the public export contained only the D26
  real-cluster scenario and omitted required helpers, including
  `scenario.sh`. Passing build and Rust source-closure checks did not
  establish harness completeness.
- The stale-survivor self-reset needs a roster majority of foreign reports and can therefore not
  fire on 1- or 2-node rosters; such a survivor stays transport-fenced until restarted.
- The GitLab publish stage ships static binaries, the source tarball and the container image. The
  Debian and RPM packages, `SHA256SUMS` and the build-provenance attestation come from the GitHub
  release workflow.
- Connection limits and phase deadlines are compile-time constants.

## Earlier releases

See the release notes attached to each tag.
