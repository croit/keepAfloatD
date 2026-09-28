# Changelog

All notable changes to keepafloatd. Versions follow the calendar scheme `vYYMM.N` used by the
release pipelines. Issue numbers refer to the project tracker.

## Unreleased

### Fixed

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

- The stale-survivor self-reset needs a roster majority of foreign reports and can therefore not
  fire on 1- or 2-node rosters; such a survivor stays transport-fenced until restarted.
- The GitLab publish stage ships static binaries, the source tarball and the container image. The
  Debian and RPM packages, `SHA256SUMS` and the build-provenance attestation come from the GitHub
  release workflow.
- Connection limits and phase deadlines are compile-time constants.

## v2608.0 and earlier

See the release notes attached to each tag.
