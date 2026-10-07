//! YAML configuration: peers, Raft listen address, VIP list, health-check command, security and timeouts.
//!
//! All cluster-wide invariants must be identical on every node:
//! - `peers` list (same ids, same `raft_address` and `client_submit_address` per id),
//! - `vips` list (same set, order normalized internally by sorting addresses),
//! - `health.interval_ms`, `health.stale_secs` and `cluster_secret` (so that staleness filtering,
//!   activation delays and authentication are derived deterministically and pass between peers).
//!
//! Per-host fields that legitimately differ:
//! - `node_id`, `raft_listen`, `client_submit_listen`, `address_protocol`.
//!
//! Cluster formation is automatic (see [`crate::raft::start_raft`]).

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

mod fingerprint;
mod secret;
mod validation;
mod vip_validation;

#[cfg(test)]
mod hardening_tests;

pub use fingerprint::ClusterConfigFingerprint;

/// Default upper bound on accepted TCP frame size for both Raft RPC and submit channel.
///
/// Rationale: AppendEntries with a few hundred entries fits well below this; snapshots up to
/// a few MB are allowed; anything larger is treated as protocol abuse and refused.
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 4 * 1024 * 1024;

/// Hard upper bound for configurable Raft and snapshot frames.
pub const MAX_RAFT_FRAME_BYTES: u32 = 16 * 1024 * 1024;

/// Default cap on time spent forwarding a single submit request to the leader.
pub const DEFAULT_SUBMIT_TIMEOUT_MS: u64 = 2_000;

/// Default Linux route protocol used in the dedicated `10000 + protocol` ownership-marker table.
pub const DEFAULT_VIP_ADDRESS_PROTOCOL: u8 = 246;

/// Public marker in the shipped sample. It is intentionally invalid so copying the sample without
/// replacing the secret cannot start a cluster with a credential known to every installation.
const INSECURE_CLUSTER_SECRET_PLACEHOLDER: &str = "replace-me-with-a-random-32-byte-string";

/// Top-level daemon configuration (one file per process).
///
/// All members of a cluster must share the same `peers`, `vips`, `health.interval_ms`,
/// `health.stale_secs`, `cluster_secret`, `max_frame_bytes` and timing-relevant tuning. Node-local
/// process/effect fields such as [`Config::node_id`], listen addresses, health command,
/// `address_protocol`, `dry_run`, and notify may differ per host.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// This process identity; must appear in [`Config::peers`].
    pub node_id: u64,
    /// Local OpenRaft listen address (must match `raft_address` for this `node_id` in `peers`).
    pub raft_listen: String,
    /// TCP listen for follower-submitted health updates when this node is Raft leader.
    pub client_submit_listen: String,
    /// Full voter list; replicated state uses only ids from this map + Raft membership.
    pub peers: Vec<PeerConfig>,
    /// Virtual IPs to distribute; normalized by sorting [`VipConfig::address`] at load time.
    pub vips: Vec<VipConfig>,
    /// Single script/command health probe for this node.
    pub health: HealthConfig,
    /// Optional OpenRaft timing.
    #[serde(default)]
    pub raft: RaftTuneConfig,
    /// Pre-shared secret for Raft and submit peers on a trusted network. Configure exactly one
    /// of this field or `cluster_secret_file`, with the same secret on every member.
    /// Secrets require 32-256 UTF-8 bytes with no whitespace or control characters.
    ///
    /// This is **not** a substitute for mTLS: it only raises the bar above zero on a network
    /// where the peer addresses are otherwise unauthenticated.
    #[serde(default)]
    pub cluster_secret: Option<String>,
    /// Read the secret from a regular UTF-8 file at startup, relative to the YAML directory.
    /// One terminal LF or CRLF is allowed. The resolved value is stored in `cluster_secret`;
    /// this node-local path is cleared after loading and never affects the fingerprint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_secret_file: Option<PathBuf>,
    /// Per-RPC TCP frame size cap (bytes). Receivers refuse to allocate buffers larger than this.
    /// Defaults to [`DEFAULT_MAX_FRAME_BYTES`].
    #[serde(default = "default_max_frame_bytes")]
    pub max_frame_bytes: u32,
    /// Wall-clock budget for one follower to leader submit forward. Defaults to
    /// [`DEFAULT_SUBMIT_TIMEOUT_MS`].
    #[serde(default = "default_submit_timeout_ms")]
    pub submit_timeout_ms: u64,
    /// Linux route protocol and derived table used as this process's crash-safe VIP namespace.
    /// Co-located keepafloatd instances must use distinct non-zero values. Keep this stable across
    /// restarts; rotate it only after a graceful stop or explicit cleanup with the old value.
    #[serde(default = "default_vip_address_protocol")]
    pub address_protocol: u8,
    /// When true, log intended `ip` operations but do not run `ip` (tests / lab).
    #[serde(default)]
    pub dry_run: bool,
    /// Optional notify script called on VIP ownership transitions (keepalived-compatible).
    ///
    /// When set, the script is invoked as:
    ///   `<script> INSTANCE <vip_address> MASTER` - when this node gains the VIP,
    ///   `<script> INSTANCE <vip_address> BACKUP` - when a healthy node releases the VIP, and
    ///   `<script> INSTANCE <vip_address> FAULT` - when this node releases the VIP because its
    ///                                               own health check failed.
    ///
    /// The script runs fire-and-forget in a separate task; failures are logged but do not affect
    /// the reconciliation loop. This is a per-node field and may differ between cluster members.
    #[serde(default)]
    pub notify: Option<String>,
    /// Minimum continuous unhealthy duration before an established healthy node is reported
    /// unhealthy and releases its VIPs. Default: zero, preserving immediate failover.
    ///
    /// The duration is converted to local probe rounds. A node that has never passed its health
    /// check remains unhealthy immediately; the delay never makes startup health optimistic.
    #[serde(default)]
    pub failover_delay_secs: u32,
    /// Whether a recovered node may reclaim its VIPs (keepalived `preempt` / `nopreempt`).
    ///
    /// `true` (default, keepalived `preempt`): after being unhealthy, a node regains eligibility
    /// once it has been continuously healthy for at least `failback_delay_secs`.
    /// `false` (keepalived `nopreempt`): a recovered node does not receive proactive rebalance
    /// moves from healthy holders, but remains eligible for orphaned VIPs when a holder fails.
    ///
    /// Cluster-wide: must be the same on every node.
    #[serde(default = "default_failback")]
    pub failback: bool,
    /// Minimum continuous healthy duration (seconds) before a recovered node becomes eligible for
    /// VIP assignment again. Only meaningful when `failback: true`; ignored when `failback: false`.
    /// Default: 10. Zero means immediate re-eligibility upon recovery.
    ///
    /// Converted at startup to a committed probe-round count so the state machine stays
    /// deterministic (no wall-clock reads inside the SM).
    ///
    /// Cluster-wide when `failback` is true. It is ignored and normalized out of the cluster
    /// configuration identity when `failback` is false.
    #[serde(default = "default_failback_delay_secs")]
    pub failback_delay_secs: u32,
}

fn default_max_frame_bytes() -> u32 {
    DEFAULT_MAX_FRAME_BYTES
}

fn default_failback() -> bool {
    true
}

fn default_failback_delay_secs() -> u32 {
    10
}

fn default_submit_timeout_ms() -> u64 {
    DEFAULT_SUBMIT_TIMEOUT_MS
}

fn default_vip_address_protocol() -> u8 {
    DEFAULT_VIP_ADDRESS_PROTOCOL
}

fn parse_socket_addr(field: &str, value: &str) -> anyhow::Result<SocketAddr> {
    value
        .parse()
        .map(canonical_socket_addr)
        .with_context(|| format!("{field} must be a valid IP:port socket address (got {value})"))
}

/// Canonicalize Linux listener aliases before equality checks and configuration fingerprinting.
/// An IPv4-mapped IPv6 socket and its IPv4 form compete for the same kernel endpoint (#26).
pub(crate) fn canonical_socket_addr(address: SocketAddr) -> SocketAddr {
    match address {
        SocketAddr::V6(address) => address
            .ip()
            .to_ipv4_mapped()
            .map_or(SocketAddr::V6(address), |ip| {
                SocketAddr::new(IpAddr::V4(ip), address.port())
            }),
        SocketAddr::V4(_) => address,
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    /// Raft node identifier; must match [`Config::node_id`] on that host.
    pub id: u64,
    /// Peer-reachable `IP:port` socket address for OpenRaft RPC (length-prefixed JSON).
    pub raft_address: String,
    /// Peer-reachable `IP:port` where this peer's [`Config::client_submit_listen`] is reachable
    /// when it is leader (followers forward [`crate::raft::KafRequest::HealthUpdate`] here).
    pub client_submit_address: String,
}

/// A VIP plus the prefix length it is bound with, parsed from the `address`
/// field: `"10.0.0.101"` (host route) or `"10.0.0.101/24"` (explicit prefix).
///
/// When no `/<prefix>` suffix is given the family host prefix is used (`/32` for IPv4, `/128` for
/// IPv6), matching the legacy behavior. The prefix is a local-effect concern: the Raft state
/// machine keys VIP ownership on [`VipAddr::addr`] alone and never inspects [`VipAddr::prefix`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct VipAddr {
    /// The virtual IP itself (this is what the consensus layer keys on).
    pub addr: IpAddr,
    /// Prefix length passed to `ip addr add|del` as `<addr>/<prefix>`.
    pub prefix: u8,
}

impl VipAddr {
    /// Largest valid prefix length for the address family (a host route).
    const fn host_prefix(addr: IpAddr) -> u8 {
        match addr {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        }
    }

    /// A host-route VIP (`/32` or `/128`) - used by tests and as the no-suffix default.
    #[must_use]
    pub fn host(addr: IpAddr) -> Self {
        Self {
            addr,
            prefix: Self::host_prefix(addr),
        }
    }
}

impl FromStr for VipAddr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (addr_str, prefix_str) = match s.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (s, None),
        };
        let addr: IpAddr = addr_str
            .parse()
            .with_context(|| format!("vip address {addr_str:?} is not a valid IP"))?;
        let max = Self::host_prefix(addr);
        let prefix = match prefix_str {
            None => max,
            Some(p) => {
                let prefix: u8 = p
                    .parse()
                    .with_context(|| format!("vip prefix {p:?} is not a valid prefix length"))?;
                anyhow::ensure!(
                    prefix <= max,
                    "vip prefix /{prefix} out of range for {addr} (max /{max})"
                );
                prefix
            }
        };
        Ok(Self { addr, prefix })
    }
}

impl TryFrom<String> for VipAddr {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl fmt::Display for VipAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix)
    }
}

impl From<VipAddr> for String {
    fn from(v: VipAddr) -> Self {
        v.to_string()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VipConfig {
    /// Secondary address managed by keepAfloatD (must be identical on every cluster member).
    /// Accepts an optional CIDR suffix (`10.0.0.101/24`); without one the VIP is
    /// bound as a host route (`/32` for IPv4, `/128` for IPv6).
    pub address: VipAddr,
    /// Linux interface name (e.g. `eth0`) for `ip addr add|del`.
    pub interface: String,
    /// Optional IEEE 802.1Q VLAN tag (1-4094). When set, all `ip addr` operations target
    /// `{interface}.{vlan}` (e.g. `eth0.100`). The sub-interface must pre-exist; keepafloatd
    /// does not create or destroy VLAN sub-interfaces. `interface` must not itself contain a dot
    /// when `vlan` is set. Absent means no VLAN (current behaviour).
    ///
    /// Duplicate IPs must have the same prefix and effective interface. Conflicts are rejected;
    /// identical bindings collapse to one at load time.
    #[serde(default)]
    pub vlan: Option<u16>,
}

/// Local script/command health probe and the cluster-wide staleness window.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HealthConfig {
    /// Executable and arguments (like execv): e.g. `["/bin/bash","-c","curl -sf http://127.0.0.1/"]`.
    pub command: Vec<String>,
    /// Period between probe runs (milliseconds).
    pub interval_ms: u64,
    /// Wall-clock limit for one run; on expiry the process is killed and health is **false**.
    /// Must be strictly below the stale window after rounding to whole probe intervals.
    pub timeout_ms: u64,
    /// Staleness window (seconds). The state machine converts this to a maximum number of missed
    /// committed health probe rounds using `interval_ms`; a peer whose most recent committed probe
    /// falls behind the cluster's latest committed round by more than that window is considered
    /// ineligible regardless of its last-reported `healthy` flag. This is what causes failover
    /// when a node dies silently (without managing to publish `healthy: false`).
    ///
    /// Must be at least `ceil(interval_ms / 1000)`. The whole-interval window must exceed
    /// `timeout_ms`. Defaults to `max(3, ceil(interval_ms / 1000) * 3)` if not set.
    #[serde(default)]
    pub stale_secs: Option<u64>,
}

impl HealthConfig {
    /// Effective staleness window in seconds (uses default heuristic if not configured).
    #[must_use]
    pub fn effective_stale_secs(&self) -> u64 {
        if let Some(s) = self.stale_secs {
            return s;
        }
        let interval_secs = self.interval_ms.div_ceil(1_000).max(1);
        (interval_secs * 3).max(3)
    }

    /// Effective staleness window expressed as "how many committed probe rounds may this node
    /// miss before the cluster fences it off".
    #[must_use]
    pub fn effective_stale_missed_probes(&self) -> u64 {
        let stale_ms = u128::from(self.effective_stale_secs()).saturating_mul(1_000);
        let rounds = stale_ms / u128::from(self.interval_ms.max(1));
        (rounds.min(u128::from(u64::MAX)) as u64).max(1)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RaftTuneConfig {
    /// Lower bound of leader election random timeout (OpenRaft).
    pub election_timeout_min_ms: u64,
    /// Upper bound of leader election random timeout (OpenRaft).
    pub election_timeout_max_ms: u64,
    /// Leader heartbeat / append-entries interval (OpenRaft).
    ///
    /// Also influences replication RPC budget; on slow links / WSL, very low values cause
    /// `timeout when AppendEntries`. Must be strictly less than `election_timeout_min_ms`.
    pub heartbeat_interval_ms: u64,
}

impl Default for RaftTuneConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 400,
            election_timeout_max_ms: 800,
            heartbeat_interval_ms: 250,
        }
    }
}

impl Config {
    /// Load + validate + normalize a YAML config.
    pub fn load_path(path: impl AsRef<Path>) -> anyhow::Result<Arc<Self>> {
        let raw = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("read {}", path.as_ref().display()))?;
        let mut c: Config = serde_yaml::from_str(&raw).context("parse yaml")?;
        c.resolve_secret(path.as_ref())?;
        c.normalize()?;
        Ok(Arc::new(c))
    }

    /// Failback delay expressed as committed probe-round count.
    ///
    /// Converts `failback_delay_secs` to the number of consecutive probe rounds a recovered node
    /// must accumulate before it is eligible again. Zero means immediate re-eligibility.
    /// Ignored when `failback` is `false`; nopreempt placement is tracked separately.
    #[must_use]
    pub fn effective_failback_delay_ticks(&self) -> u64 {
        duration_to_probe_rounds_ceil(u64::from(self.failback_delay_secs), self.health.interval_ms)
    }

    /// Explicit health-failure delay expressed as local probe rounds.
    #[must_use]
    pub fn effective_failover_delay_ticks(&self) -> u64 {
        duration_to_probe_rounds_ceil(u64::from(self.failover_delay_secs), self.health.interval_ms)
    }

    pub fn get_peer(&self, id: u64) -> Option<&PeerConfig> {
        self.peers.iter().find(|p| p.id == id)
    }

    pub fn other_peers(&self) -> Vec<&PeerConfig> {
        self.peers.iter().filter(|p| p.id != self.node_id).collect()
    }

    /// Sorted VIPs for deterministic assignment (same order on every node).
    ///
    /// When a VIP has a `vlan` tag, the returned interface is `{interface}.{vlan}` (e.g.
    /// `eth0.100`). Callers receive only the effective interface and need no VLAN awareness.
    pub fn sorted_vips(&self) -> Vec<(VipAddr, String)> {
        self.vips
            .iter()
            .map(|v| (v.address, v.effective_interface()))
            .collect()
    }
}

fn duration_to_probe_rounds_ceil(duration_secs: u64, interval_ms: u64) -> u64 {
    if duration_secs == 0 {
        return 0;
    }
    let duration_ms = u128::from(duration_secs).saturating_mul(1_000);
    let rounds = duration_ms.div_ceil(u128::from(interval_ms.max(1)));
    rounds.min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests;
