# Changelog

All notable changes to keepafloatd. Versions follow the calendar scheme
`vYYMM.N` used by the release pipelines. Issue numbers refer to the
project tracker. `Unreleased` contains draft release notes, not an
announcement that new artifacts are available.

## Unreleased

Changes since `v2609.0`; version and release date are not yet assigned.

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
