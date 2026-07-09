//! YAML configuration: peers, Raft listen address, VIP list, health-check command, security and timeouts.
//!
//! All cluster-wide invariants must be identical on every node:
//! - `peers` list (same ids, same `raft_address` and `client_submit_address` per id),
//! - `vips` list (same set, order normalized internally by sorting addresses),
//! - `health.interval_ms`, `health.stale_secs` and `cluster_secret` (so that staleness filtering,
//!   activation delays and authentication are derived deterministically and pass between peers).
//!
//! Per-host fields that legitimately differ:
//! - `node_id`, `raft_listen`, `client_submit_listen`.
//!
//! Cluster formation is automatic (see [`crate::raft::auto_form_cluster`]).

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

/// Default upper bound on accepted TCP frame size for both Raft RPC and submit channel.
///
/// Rationale: AppendEntries with a few hundred entries fits well below this; snapshots up to
/// a few MB are allowed; anything larger is treated as protocol abuse and refused.
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 4 * 1024 * 1024;

/// Default cap on time spent forwarding a single submit request to the leader.
pub const DEFAULT_SUBMIT_TIMEOUT_MS: u64 = 2_000;

/// Top-level daemon configuration (one file per process).
///
/// All members of a cluster must share the same `peers`, `vips`, `health.interval_ms`,
/// `health.stale_secs`, `cluster_secret`, `max_frame_bytes` and timing-relevant tuning. Only
/// [`Config::node_id`] and the listen addresses differ per host.
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    /// Optional pre-shared secret used to authenticate Raft and submit peers on a v1 trusted
    /// network. If set, every member must use the **same** value; mismatched or missing token
    /// causes the receiver to drop the connection.
    ///
    /// This is **not** a substitute for mTLS: it only raises the bar above zero on a network
    /// where the peer addresses are otherwise unauthenticated.
    #[serde(default)]
    pub cluster_secret: Option<String>,
    /// Per-RPC TCP frame size cap (bytes). Receivers refuse to allocate buffers larger than this.
    /// Defaults to [`DEFAULT_MAX_FRAME_BYTES`].
    #[serde(default = "default_max_frame_bytes")]
    pub max_frame_bytes: u32,
    /// Wall-clock budget for one follower→leader submit forward. Defaults to
    /// [`DEFAULT_SUBMIT_TIMEOUT_MS`].
    #[serde(default = "default_submit_timeout_ms")]
    pub submit_timeout_ms: u64,
    /// When true, log intended `ip` operations but do not run `ip` (tests / lab).
    #[serde(default)]
    pub dry_run: bool,
    /// Optional notify script called on VIP ownership transitions (keepalived-compatible).
    ///
    /// When set, the script is invoked as:
    ///   `<script> INSTANCE <vip_address> MASTER`  — when this node gains the VIP,
    ///   `<script> INSTANCE <vip_address> BACKUP`  — when a healthy node releases the VIP, and
    ///   `<script> INSTANCE <vip_address> FAULT`   — when this node releases the VIP because its
    ///                                               own health check failed.
    ///
    /// The script runs fire-and-forget in a separate task; failures are logged but do not affect
    /// the reconciliation loop. This is a per-node field and may differ between cluster members.
    #[serde(default)]
    pub notify: Option<String>,
    /// Whether a recovered node may reclaim its VIPs (keepalived `preempt` / `nopreempt`).
    ///
    /// `true` (default, keepalived `preempt`): after being unhealthy, a node regains eligibility
    /// once it has been continuously healthy for at least `failback_delay_secs`.
    /// `false` (keepalived `nopreempt`): a node that lost its VIPs due to a health failure never
    /// has VIPs reassigned to it until the entire cluster is fully reformed (all nodes restart
    /// from scratch without an existing snapshot). The block is replicated via snapshot, so
    /// restarting a single daemon is not sufficient to clear it.
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
    /// Cluster-wide: must be the same on every node.
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

fn parse_socket_addr(field: &str, value: &str) -> anyhow::Result<SocketAddr> {
    value
        .parse()
        .with_context(|| format!("{field} must be a valid IP:port socket address (got {value})"))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

    /// A host-route VIP (`/32` or `/128`) — used by tests and as the no-suffix default.
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
pub struct VipConfig {
    /// Secondary address managed by keepAfloatD (must be identical on every cluster member).
    /// Accepts an optional CIDR suffix (`10.0.0.101/24`); without one the VIP is
    /// bound as a host route (`/32` for IPv4, `/128` for IPv6).
    pub address: VipAddr,
    /// Linux interface name (e.g. `eth0`) for `ip addr add|del`.
    pub interface: String,
    /// Optional IEEE 802.1Q VLAN tag (1–4094). When set, all `ip addr` operations target
    /// `{interface}.{vlan}` (e.g. `eth0.100`). The sub-interface must pre-exist; keepafloatd
    /// does not create or destroy VLAN sub-interfaces. `interface` must not itself contain a dot
    /// when `vlan` is set. Absent means no VLAN (current behaviour).
    ///
    /// VIPs are deduplicated by IP address (same as the CIDR prefix field); two entries with the
    /// same `address` but different `vlan` values collapse to one at load time. Use distinct
    /// `address` values for distinct VLAN bindings.
    #[serde(default)]
    pub vlan: Option<u16>,
}

/// Local script/command health probe and the cluster-wide staleness window.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthConfig {
    /// Executable and arguments (like execv): e.g. `["/bin/bash","-c","curl -sf http://127.0.0.1/"]`.
    pub command: Vec<String>,
    /// Period between probe runs (milliseconds).
    pub interval_ms: u64,
    /// Wall-clock limit for one run; on expiry the process is killed and health is **false**.
    pub timeout_ms: u64,
    /// Staleness window (seconds). The state machine converts this to a maximum number of missed
    /// committed health probe rounds using `interval_ms`; a peer whose most recent committed probe
    /// falls behind the cluster's latest committed round by more than that window is considered
    /// ineligible regardless of its last-reported `healthy` flag. This is what causes failover
    /// when a node dies silently (without managing to publish `healthy: false`).
    ///
    /// Must be greater than `interval_ms / 1000` so that a single missed probe does not flap
    /// eligibility. Defaults to `max(3, ceil(interval_ms / 1000) * 3)` if not set.
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
        let interval_secs = self.interval_ms.div_ceil(1_000).max(1);
        self.effective_stale_secs().div_ceil(interval_secs).max(1)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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
        c.normalize()?;
        Ok(Arc::new(c))
    }

    fn normalize(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.peers.is_empty(), "peers must be non-empty");
        anyhow::ensure!(!self.vips.is_empty(), "vips must be non-empty");
        // Secure by default: a loaded config must carry a shared secret. Without it the Raft and
        // submit listeners would accept unauthenticated requests from any reachable host, so we
        // fail closed at load time rather than run an open control plane.
        anyhow::ensure!(
            self.cluster_secret
                .as_deref()
                .is_some_and(|s| !s.is_empty()),
            "cluster_secret is required and must be non-empty"
        );
        anyhow::ensure!(
            !self.health.command.is_empty(),
            "health.command must be non-empty"
        );
        anyhow::ensure!(
            self.health.interval_ms > 0,
            "health.interval_ms must be > 0"
        );
        anyhow::ensure!(self.health.timeout_ms > 0, "health.timeout_ms must be > 0");
        anyhow::ensure!(
            self.health.timeout_ms <= self.health.interval_ms.saturating_mul(10),
            "health.timeout_ms suspiciously large vs interval_ms"
        );

        let stale = self.health.effective_stale_secs();
        let interval_secs = self.health.interval_ms.div_ceil(1_000).max(1);
        anyhow::ensure!(
            stale >= interval_secs,
            "health.stale_secs ({stale}) must be >= ceil(interval_ms/1000) ({interval_secs})"
        );

        let ids: BTreeSet<u64> = self.peers.iter().map(|p| p.id).collect();
        anyhow::ensure!(ids.len() == self.peers.len(), "duplicate peer id");
        anyhow::ensure!(
            self.peers.iter().any(|p| p.id == self.node_id),
            "node_id must appear in peers"
        );
        let raft_listen = parse_socket_addr("raft_listen", &self.raft_listen)?;
        let client_submit_listen =
            parse_socket_addr("client_submit_listen", &self.client_submit_listen)?;
        for peer in &self.peers {
            let raft_address = parse_socket_addr(
                &format!("peers[{}].raft_address", peer.id),
                &peer.raft_address,
            )?;
            let client_submit_address = parse_socket_addr(
                &format!("peers[{}].client_submit_address", peer.id),
                &peer.client_submit_address,
            )?;
            anyhow::ensure!(
                !raft_address.ip().is_unspecified(),
                "peers[{}].raft_address must not use an unspecified IP ({})",
                peer.id,
                peer.raft_address
            );
            anyhow::ensure!(
                !client_submit_address.ip().is_unspecified(),
                "peers[{}].client_submit_address must not use an unspecified IP ({})",
                peer.id,
                peer.client_submit_address
            );
            anyhow::ensure!(
                raft_address != client_submit_address,
                "peer {} raft_address and client_submit_address must differ",
                peer.id
            );
        }

        let local_peer = self
            .get_peer(self.node_id)
            .context("node_id must appear in peers after validation")?;
        let local_raft_address = parse_socket_addr(
            &format!("peers[{}].raft_address", local_peer.id),
            &local_peer.raft_address,
        )?;
        let local_client_submit_address = parse_socket_addr(
            &format!("peers[{}].client_submit_address", local_peer.id),
            &local_peer.client_submit_address,
        )?;
        anyhow::ensure!(
            raft_listen == local_raft_address,
            "raft_listen {} must match peers[{}].raft_address {}",
            self.raft_listen,
            self.node_id,
            local_peer.raft_address
        );
        anyhow::ensure!(
            client_submit_listen == local_client_submit_address,
            "client_submit_listen {} must match peers[{}].client_submit_address {}",
            self.client_submit_listen,
            self.node_id,
            local_peer.client_submit_address
        );

        if let Some(secret) = &self.cluster_secret {
            anyhow::ensure!(
                !secret.is_empty(),
                "cluster_secret must be non-empty if set (omit the field to disable)"
            );
            anyhow::ensure!(
                secret.len() <= 256,
                "cluster_secret must be at most 256 bytes"
            );
        }

        anyhow::ensure!(
            self.max_frame_bytes >= 64 * 1024,
            "max_frame_bytes must be >= 64 KiB to fit Raft heartbeats"
        );
        anyhow::ensure!(self.submit_timeout_ms > 0, "submit_timeout_ms must be > 0");

        self.vips.sort_by_key(|v| v.address.addr);
        self.vips.dedup_by_key(|v| v.address.addr);
        anyhow::ensure!(!self.vips.is_empty(), "vips must be non-empty after dedup");

        for (i, vip) in self.vips.iter().enumerate() {
            if let Some(vlan) = vip.vlan {
                anyhow::ensure!(
                    (1..=4094).contains(&vlan),
                    "vips[{i}] ({}): vlan {vlan} is out of range: IEEE 802.1Q allows 1–4094",
                    vip.address
                );
                anyhow::ensure!(
                    !vip.interface.contains('.'),
                    "vips[{i}] ({}): interface {:?} must not contain a dot when vlan is set; \
                     use the base interface name (e.g. \"eth0\", not \"eth0.10\")",
                    vip.address,
                    vip.interface
                );
            }
        }
        Ok(())
    }

    /// Failback delay expressed as committed probe-round count.
    ///
    /// Converts `failback_delay_secs` to the number of consecutive probe rounds a recovered node
    /// must accumulate before it is eligible again. Zero means immediate re-eligibility.
    /// When `failback` is `false` this value is irrelevant (the node is permanently blocked).
    #[must_use]
    pub fn effective_failback_delay_ticks(&self) -> u64 {
        if self.failback_delay_secs == 0 {
            return 0;
        }
        let interval_secs = self.health.interval_ms.div_ceil(1_000).max(1);
        (self.failback_delay_secs as u64).div_ceil(interval_secs)
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
            .map(|v| {
                let iface = match v.vlan {
                    Some(vlan) => format!("{}.{}", v.interface, vlan),
                    None => v.interface.clone(),
                };
                (v.address, iface)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    use std::net::IpAddr;
    use std::str::FromStr;

    fn parse_normalize(yaml: &str) -> anyhow::Result<Config> {
        let mut c: Config = serde_yaml::from_str(yaml)?;
        c.normalize()?;
        Ok(c)
    }

    const MINIMAL_YAML: &str = r#"
node_id: 1
raft_listen: "127.0.0.1:17101"
client_submit_listen: "127.0.0.1:17102"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:17101"
    client_submit_address: "127.0.0.1:17102"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;

    #[test]
    fn minimal_config_loads() {
        let c = parse_normalize(MINIMAL_YAML).unwrap();
        assert_eq!(c.node_id, 1);
        assert_eq!(c.max_frame_bytes, super::DEFAULT_MAX_FRAME_BYTES);
        assert_eq!(c.submit_timeout_ms, super::DEFAULT_SUBMIT_TIMEOUT_MS);
        assert_eq!(c.cluster_secret.as_deref(), Some("test-secret"));
        assert_eq!(c.health.effective_stale_secs(), 3);
        assert_eq!(c.health.effective_stale_missed_probes(), 3);
    }

    #[test]
    fn config_without_cluster_secret_is_rejected() {
        // Secure by default: loading a config with no shared secret fails closed.
        let yaml = MINIMAL_YAML.replace("cluster_secret: \"test-secret\"\n", "");
        let err = parse_normalize(&yaml).unwrap_err().to_string();
        assert!(err.contains("cluster_secret is required"), "{err}");
    }

    #[test]
    fn vips_sorted_by_address_after_normalize() {
        let yaml = r#"
node_id: 2
raft_listen: "127.0.0.1:3"
client_submit_listen: "127.0.0.1:4"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
  - id: 2
    raft_address: "127.0.0.1:3"
    client_submit_address: "127.0.0.1:4"
vips:
  - address: 10.0.0.10
    interface: eth0
  - address: 10.0.0.2
    interface: eth0
  - address: 2001:db8::1
    interface: eth0
health:
  command: ["/bin/sh", "-c", "true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let c = parse_normalize(yaml).unwrap();
        let addrs: Vec<IpAddr> = c.vips.iter().map(|v| v.address.addr).collect();
        assert_eq!(addrs.len(), 3);
        assert!(addrs[0] < addrs[1]);
        assert!(addrs[1] < addrs[2]);
        let sorted = c.sorted_vips();
        assert_eq!(sorted[0].0.addr, IpAddr::from_str("10.0.0.2").unwrap());
        assert_eq!(sorted[1].0.addr, IpAddr::from_str("10.0.0.10").unwrap());
        assert_eq!(sorted[2].0.addr, IpAddr::from_str("2001:db8::1").unwrap());
    }

    #[test]
    fn vip_address_parses_cidr_suffix() {
        let v: super::VipAddr = "10.0.0.101/24".parse().unwrap();
        assert_eq!(v.addr, IpAddr::from_str("10.0.0.101").unwrap());
        assert_eq!(v.prefix, 24);
        let v6: super::VipAddr = "2001:db8::1/64".parse().unwrap();
        assert_eq!(v6.prefix, 64);
    }

    #[test]
    fn vip_address_defaults_to_host_prefix() {
        let v4: super::VipAddr = "10.0.0.101".parse().unwrap();
        assert_eq!(v4.prefix, 32);
        let v6: super::VipAddr = "2001:db8::1".parse().unwrap();
        assert_eq!(v6.prefix, 128);
    }

    #[test]
    fn vip_address_rejects_out_of_range_prefix() {
        assert!("10.0.0.101/33".parse::<super::VipAddr>().is_err());
        assert!("2001:db8::1/129".parse::<super::VipAddr>().is_err());
        assert!("10.0.0.101/abc".parse::<super::VipAddr>().is_err());
        assert!("not-an-ip/24".parse::<super::VipAddr>().is_err());
    }

    #[test]
    fn vip_cidr_suffix_flows_through_config() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: "10.0.0.101/24"
    interface: eth0
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let c = parse_normalize(yaml).unwrap();
        let sorted = c.sorted_vips();
        assert_eq!(sorted.len(), 1);
        assert_eq!(sorted[0].0.addr, IpAddr::from_str("10.0.0.101").unwrap());
        assert_eq!(sorted[0].0.prefix, 24);
    }

    #[test]
    fn vip_out_of_range_prefix_rejected_by_config() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: "10.0.0.101/33"
    interface: eth0
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        assert!(parse_normalize(yaml).is_err());
    }

    #[test]
    fn dedup_removes_duplicate_vip_addresses_keeping_one() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: eth0
  - address: 10.0.0.1
    interface: eth0
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let c = parse_normalize(yaml).unwrap();
        assert_eq!(c.vips.len(), 1);
    }

    #[test]
    fn empty_peers_rejected() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers: []
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        assert!(parse_normalize(yaml).is_err());
    }

    #[test]
    fn duplicate_peer_ids_rejected() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
  - id: 1
    raft_address: "127.0.0.1:3"
    client_submit_address: "127.0.0.1:4"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        assert!(parse_normalize(yaml).is_err());
    }

    #[test]
    fn node_id_must_be_listed_as_peer() {
        let yaml = r#"
node_id: 9
raft_listen: "127.0.0.1:9"
client_submit_listen: "127.0.0.1:10"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        assert!(parse_normalize(yaml).is_err());
    }

    #[test]
    fn empty_health_command_rejected() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: []
  interval_ms: 1000
  timeout_ms: 500
"#;
        assert!(parse_normalize(yaml).is_err());
    }

    #[test]
    fn empty_vips_rejected() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips: []
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        assert!(parse_normalize(yaml).is_err());
    }

    #[test]
    fn empty_cluster_secret_rejected() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
cluster_secret: ""
"#;
        assert!(parse_normalize(yaml).is_err());
    }

    #[test]
    fn small_max_frame_rejected() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
max_frame_bytes: 1024
"#;
        assert!(parse_normalize(yaml).is_err());
    }

    #[test]
    fn stale_secs_smaller_than_interval_rejected() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 5000
  timeout_ms: 500
  stale_secs: 1
"#;
        assert!(parse_normalize(yaml).is_err());
    }

    #[test]
    fn raft_listen_must_match_local_peer_raft_address() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:17099"
client_submit_listen: "127.0.0.1:17102"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:17101"
    client_submit_address: "127.0.0.1:17102"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let err = parse_normalize(yaml).unwrap_err();
        assert!(err.to_string().contains("raft_listen"));
    }

    #[test]
    fn client_submit_listen_must_match_local_peer_submit_address() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:17101"
client_submit_listen: "127.0.0.1:17099"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:17101"
    client_submit_address: "127.0.0.1:17102"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let err = parse_normalize(yaml).unwrap_err();
        assert!(err.to_string().contains("client_submit_listen"));
    }

    #[test]
    fn peer_addresses_must_be_valid_socket_addresses() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:17101"
client_submit_listen: "127.0.0.1:17102"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "not-an-addr"
    client_submit_address: "127.0.0.1:17102"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let err = parse_normalize(yaml).unwrap_err();
        assert!(err.to_string().contains("peers[1].raft_address"));
    }

    #[test]
    fn peer_addresses_must_not_use_unspecified_ip() {
        let yaml = r#"
node_id: 1
raft_listen: "0.0.0.0:17101"
client_submit_listen: "0.0.0.0:17102"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "0.0.0.0:17101"
    client_submit_address: "0.0.0.0:17102"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let err = parse_normalize(yaml).unwrap_err();
        assert!(err.to_string().contains("must not use an unspecified IP"));
    }

    #[test]
    fn get_peer_other_peers() {
        let c = parse_normalize(MINIMAL_YAML).unwrap();
        assert_eq!(c.get_peer(1).map(|p| p.id), Some(1));
        assert!(c.get_peer(2).is_none());
        let others: Vec<u64> = c.other_peers().into_iter().map(|p| p.id).collect();
        assert!(others.is_empty());
    }

    fn hc(interval_ms: u64, stale_secs: Option<u64>) -> super::HealthConfig {
        super::HealthConfig {
            command: vec!["/bin/true".into()],
            interval_ms,
            timeout_ms: 1,
            stale_secs,
        }
    }

    #[test]
    fn effective_stale_window_heuristics() {
        // Default heuristic: max(3, ceil(interval_ms/1000) * 3) seconds, at least one probe round.
        assert_eq!(hc(1, None).effective_stale_secs(), 3);
        assert_eq!(hc(1, None).effective_stale_missed_probes(), 3);
        assert_eq!(hc(1000, None).effective_stale_secs(), 3);
        assert_eq!(hc(1000, None).effective_stale_missed_probes(), 3);
        assert_eq!(hc(2000, None).effective_stale_secs(), 6);
        assert_eq!(hc(2000, None).effective_stale_missed_probes(), 3);
        assert_eq!(hc(5000, None).effective_stale_secs(), 15);
        assert_eq!(hc(5000, None).effective_stale_missed_probes(), 3);
        // Explicit override is honored and converted to whole missed rounds (ceil).
        assert_eq!(hc(1000, Some(10)).effective_stale_secs(), 10);
        assert_eq!(hc(1000, Some(10)).effective_stale_missed_probes(), 10);
        assert_eq!(hc(3000, Some(10)).effective_stale_missed_probes(), 4);
    }

    #[test]
    fn vip_address_accepts_prefix_zero_and_host_max() {
        assert_eq!("10.0.0.0/0".parse::<super::VipAddr>().unwrap().prefix, 0);
        assert_eq!("10.0.0.1/32".parse::<super::VipAddr>().unwrap().prefix, 32);
        assert_eq!("::/0".parse::<super::VipAddr>().unwrap().prefix, 0);
        assert_eq!(
            "2001:db8::1/128".parse::<super::VipAddr>().unwrap().prefix,
            128
        );
    }

    #[test]
    fn dedup_by_address_ignores_prefix_and_keeps_first_after_sort() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: "10.0.0.1/24"
    interface: eth0
  - address: "10.0.0.1/32"
    interface: eth0
  - address: "2001:db8::1"
    interface: eth0
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let c = parse_normalize(yaml).unwrap();
        let sorted = c.sorted_vips();
        // Same address with different prefixes collapses to a single VIP (consensus keys on addr).
        assert_eq!(sorted.len(), 2);
        assert_eq!(sorted[0].0.addr, IpAddr::from_str("10.0.0.1").unwrap());
        // dedup keeps the first entry encountered after the stable sort (the /24).
        assert_eq!(sorted[0].0.prefix, 24);
        assert_eq!(sorted[1].0.addr, IpAddr::from_str("2001:db8::1").unwrap());
    }

    fn yaml_with_secret(secret: &str) -> String {
        format!(
            r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
cluster_secret: "{secret}"
"#
        )
    }

    #[test]
    fn cluster_secret_length_boundary() {
        // Exactly 256 bytes is accepted; 257 is rejected.
        let ok = "a".repeat(256);
        let c = parse_normalize(&yaml_with_secret(&ok)).unwrap();
        assert_eq!(c.cluster_secret.as_deref(), Some(ok.as_str()));
        let too_long = "a".repeat(257);
        assert!(parse_normalize(&yaml_with_secret(&too_long)).is_err());
    }

    #[test]
    fn timeout_far_larger_than_interval_rejected() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 10001
"#;
        assert!(parse_normalize(yaml).is_err());
    }

    #[test]
    fn submit_timeout_zero_rejected() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
submit_timeout_ms: 0
"#;
        assert!(parse_normalize(yaml).is_err());
    }

    #[test]
    fn vip_addr_display_and_string_conversion() {
        let v: super::VipAddr = "10.0.0.5/24".parse().unwrap();
        assert_eq!(v.to_string(), "10.0.0.5/24");
        let host: super::VipAddr = "10.0.0.5".parse().unwrap();
        assert_eq!(host.to_string(), "10.0.0.5/32");
        let s: String = host.into(); // serde `into = "String"` path
        assert_eq!(s, "10.0.0.5/32");
    }

    #[test]
    fn load_path_reads_validates_and_normalizes_a_file() {
        let path = std::env::temp_dir().join(format!("kaf-cfg-{}.yaml", std::process::id()));
        std::fs::write(&path, MINIMAL_YAML).unwrap();
        let c = Config::load_path(&path).unwrap();
        assert_eq!(c.node_id, 1);
        assert_eq!(c.sorted_vips().len(), 1);
        let _ = std::fs::remove_file(&path);

        // A missing file surfaces an error rather than panicking.
        assert!(Config::load_path("/nonexistent/keepafloatd-config.yaml").is_err());
    }

    #[test]
    fn peer_raft_and_submit_address_must_differ() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:1"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:1"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let err = parse_normalize(yaml).unwrap_err();
        assert!(err.to_string().contains("must differ"));
    }

    fn vlan_yaml(vlan_line: &str) -> String {
        format!(
            r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: "10.0.0.101/24"
    interface: eth0
    {vlan_line}
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#
        )
    }

    #[test]
    fn vlan_field_absent_preserves_interface() {
        let c = parse_normalize(&vlan_yaml("")).unwrap();
        assert_eq!(c.sorted_vips()[0].1, "eth0");
    }

    #[test]
    fn vlan_field_produces_effective_subinterface() {
        let c = parse_normalize(&vlan_yaml("vlan: 100")).unwrap();
        assert_eq!(c.sorted_vips()[0].1, "eth0.100");
    }

    #[test]
    fn vlan_boundary_1_accepted() {
        let c = parse_normalize(&vlan_yaml("vlan: 1")).unwrap();
        assert_eq!(c.sorted_vips()[0].1, "eth0.1");
    }

    #[test]
    fn vlan_boundary_4094_accepted() {
        let c = parse_normalize(&vlan_yaml("vlan: 4094")).unwrap();
        assert_eq!(c.sorted_vips()[0].1, "eth0.4094");
    }

    #[test]
    fn vlan_zero_rejected() {
        let err = parse_normalize(&vlan_yaml("vlan: 0")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("out of range"), "{err}");
        assert!(msg.contains("10.0.0.101"), "{err}");
    }

    #[test]
    fn vlan_4095_rejected() {
        let err = parse_normalize(&vlan_yaml("vlan: 4095")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("out of range"), "{err}");
        assert!(msg.contains("10.0.0.101"), "{err}");
    }

    #[test]
    fn vlan_interface_with_dot_rejected() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: "10.0.0.101/24"
    interface: eth0.10
    vlan: 100
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let err = parse_normalize(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("must not contain a dot"), "{err}");
        assert!(msg.contains("eth0.10"), "{err}");
    }

    #[test]
    fn vlan_deduped_invalid_entry_does_not_block_startup() {
        // Two VIPs with the same IP: the first (vlan:100, valid) wins the stable sort and
        // survives dedup; the second (vlan:0, invalid) is deduplicated away before validation.
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: "10.0.0.1"
    interface: eth0
    vlan: 100
  - address: "10.0.0.1"
    interface: eth0
    vlan: 0
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let c = parse_normalize(yaml).unwrap();
        assert_eq!(c.vips.len(), 1);
        assert_eq!(c.vips[0].vlan, Some(100));
    }

    #[test]
    fn vlan_flows_through_sorted_vips_without_regression_on_no_vlan() {
        // Mix: one VLAN VIP, one plain VIP.
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: "10.0.0.10"
    interface: eth0
    vlan: 200
  - address: "10.0.0.1"
    interface: eth1
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
        let c = parse_normalize(yaml).unwrap();
        let sorted = c.sorted_vips();
        // After sort: 10.0.0.1 first, 10.0.0.10 second.
        assert_eq!(sorted[0].1, "eth1");
        assert_eq!(sorted[1].1, "eth0.200");
    }

    #[test]
    fn failback_defaults_to_true_and_delay_to_10() {
        let c = parse_normalize(MINIMAL_YAML).unwrap();
        assert!(c.failback, "failback must default to true (preempt)");
        assert_eq!(c.failback_delay_secs, 10);
    }

    #[test]
    fn failback_false_parses_and_loads() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
failback: false
"#;
        let c = parse_normalize(yaml).unwrap();
        assert!(!c.failback);
        assert_eq!(c.failback_delay_secs, 10); // default still applied
    }

    #[test]
    fn failback_delay_secs_zero_parses() {
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
failback_delay_secs: 0
"#;
        let c = parse_normalize(yaml).unwrap();
        assert_eq!(c.failback_delay_secs, 0);
        assert_eq!(c.effective_failback_delay_ticks(), 0);
    }

    #[test]
    fn effective_failback_delay_ticks_converts_seconds_to_probe_rounds() {
        // interval_ms=1000 → interval_secs=1 → delay_ticks = ceil(10/1) = 10
        let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
failback_delay_secs: 10
"#;
        let c = parse_normalize(yaml).unwrap();
        assert_eq!(c.effective_failback_delay_ticks(), 10);

        // interval_ms=3000 → interval_secs=3 → delay_ticks = ceil(10/3) = 4
        let yaml2 = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 3000
  timeout_ms: 500
failback_delay_secs: 10
"#;
        let c2 = parse_normalize(yaml2).unwrap();
        assert_eq!(c2.effective_failback_delay_ticks(), 4);

        // interval_ms=500 (<1s) → interval_secs=1 → delay_ticks = ceil(5/1) = 5
        let yaml3 = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 500
  timeout_ms: 500
failback_delay_secs: 5
"#;
        let c3 = parse_normalize(yaml3).unwrap();
        assert_eq!(c3.effective_failback_delay_ticks(), 5);
    }
}
