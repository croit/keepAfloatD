//! Canonical identity of every configuration input that can affect cluster behavior.

use super::{Config, PeerConfig, canonical_socket_addr};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::net::{IpAddr, SocketAddr};

const FINGERPRINT_VERSION: u8 = 1;
const DOMAIN_SEPARATOR: &[u8] = b"keepafloatd-cluster-config";

/// Versioned digest of the canonical cluster-wide configuration.
///
/// The digest is safe to log: it contains no secret material or node-local configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClusterConfigFingerprint {
    pub version: u8,
    pub digest: [u8; 32],
}

impl fmt::Display for ClusterConfigFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}:", self.version)?;
        for byte in self.digest {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

struct CanonicalBytes(Vec<u8>);

impl CanonicalBytes {
    fn new() -> Self {
        let mut bytes = Self(Vec::new());
        bytes.field(DOMAIN_SEPARATOR);
        bytes.u8(FINGERPRINT_VERSION);
        bytes
    }

    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }

    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn u16(&mut self, value: u16) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }

    fn field(&mut self, value: &[u8]) {
        self.u64(value.len() as u64);
        self.0.extend_from_slice(value);
    }

    fn text(&mut self, value: &str) {
        self.field(value.as_bytes());
    }

    fn ip(&mut self, value: IpAddr) {
        match value {
            IpAddr::V4(address) => {
                self.u8(4);
                self.0.extend_from_slice(&address.octets());
            }
            IpAddr::V6(address) => {
                self.u8(6);
                self.0.extend_from_slice(&address.octets());
            }
        }
    }

    fn socket(&mut self, value: SocketAddr) {
        match value {
            SocketAddr::V4(address) => {
                self.ip(IpAddr::V4(*address.ip()));
                self.u16(address.port());
            }
            SocketAddr::V6(address) => {
                self.ip(IpAddr::V6(*address.ip()));
                self.u16(address.port());
                // Scope and flow select a concrete IPv6 endpoint even when the address/port match.
                self.u32(address.flowinfo());
                self.u32(address.scope_id());
            }
        }
    }
}

fn sorted_peers(peers: &[PeerConfig]) -> Vec<&PeerConfig> {
    let mut peers: Vec<_> = peers.iter().collect();
    peers.sort_by_key(|peer| peer.id);
    peers
}

impl Config {
    /// Compute the canonical cluster-wide configuration identity.
    ///
    /// Call after [`Config::load_path`] validation. Parsing still returns an error instead of
    /// assuming public address strings cannot have been mutated by a caller.
    pub fn cluster_config_fingerprint(&self) -> anyhow::Result<ClusterConfigFingerprint> {
        let mut canonical = CanonicalBytes::new();

        let peers = sorted_peers(&self.peers);
        canonical.u64(peers.len() as u64);
        for peer in peers {
            canonical.u64(peer.id);
            canonical.socket(
                peer.raft_address
                    .parse()
                    .map(canonical_socket_addr)
                    .with_context(|| format!("invalid raft address for peer {}", peer.id))?,
            );
            canonical.socket(
                peer.client_submit_address
                    .parse()
                    .map(canonical_socket_addr)
                    .with_context(|| format!("invalid submit address for peer {}", peer.id))?,
            );
        }

        // #26: identity follows the effective table consumed by Raft and the OS layer. Hashing
        // raw `interface` + `vlan` would falsely split two configurations that both resolve to
        // the same pre-existing sub-interface (for example `eth0.100` and `eth0` + VLAN 100).
        let vips = self.sorted_vips();
        canonical.u64(vips.len() as u64);
        for (vip, interface) in vips {
            canonical.ip(vip.addr);
            canonical.u8(vip.prefix);
            canonical.text(&interface);
        }

        canonical.u64(self.health.interval_ms);
        canonical.u64(self.health.effective_stale_missed_probes());
        canonical.u64(self.raft.election_timeout_min_ms);
        canonical.u64(self.raft.election_timeout_max_ms);
        canonical.u64(self.raft.heartbeat_interval_ms);
        canonical.u32(self.max_frame_bytes);
        canonical.u64(self.effective_failover_delay_ticks());
        canonical.bool(self.failback);
        // Nopreempt never consults the recovery delay, so normalize that ignored field instead of
        // fencing nodes whose effective placement policy is identical.
        canonical.u64(if self.failback {
            self.effective_failback_delay_ticks()
        } else {
            0
        });

        Ok(ClusterConfigFingerprint {
            version: FINGERPRINT_VERSION,
            digest: Sha256::digest(canonical.0).into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::Config;

    const BASE: &str = r#"
node_id: 1
raft_listen: "127.0.0.1:17101"
client_submit_listen: "127.0.0.1:17102"
cluster_secret: "alpha-secret"
peers:
  - id: 2
    raft_address: "127.0.0.2:17101"
    client_submit_address: "127.0.0.2:17102"
  - id: 1
    raft_address: "127.0.0.1:17101"
    client_submit_address: "127.0.0.1:17102"
vips:
  - address: 2001:db8::100/64
    interface: bond0
    vlan: 200
  - address: 192.0.2.100/24
    interface: eth0
health:
  command: ["/bin/check-local", "--node", "1"]
  interval_ms: 1000
  timeout_ms: 500
  stale_secs: 5
raft:
  election_timeout_min_ms: 400
  election_timeout_max_ms: 800
  heartbeat_interval_ms: 250
max_frame_bytes: 4194304
submit_timeout_ms: 2000
dry_run: false
notify: /usr/local/bin/notify-local
failover_delay_secs: 2
failback: true
failback_delay_secs: 10
"#;

    fn config(yaml: &str) -> Config {
        let mut config: Config = serde_yaml::from_str(yaml).unwrap();
        config.normalize().unwrap();
        config
    }

    fn fingerprint(yaml: &str) -> String {
        config(yaml)
            .cluster_config_fingerprint()
            .unwrap()
            .to_string()
    }

    #[test]
    fn fingerprint_is_stable_across_peer_order_and_node_local_settings() {
        let variant = BASE
            .replace("node_id: 1", "node_id: 2")
            .replace("raft_listen: \"127.0.0.1:17101\"", "raft_listen: \"127.0.0.2:17101\"")
            .replace(
                "client_submit_listen: \"127.0.0.1:17102\"",
                "client_submit_listen: \"127.0.0.2:17102\"",
            )
            .replace("alpha-secret", "different-secret")
            .replace("/bin/check-local", "/bin/other-local-check")
            .replace("--node\", \"1", "--node\", \"2")
            .replace("timeout_ms: 500", "timeout_ms: 900")
            .replace("submit_timeout_ms: 2000", "submit_timeout_ms: 9000")
            .replace(
                "dry_run: false",
                "address_protocol: 245\ndry_run: true",
            )
            .replace(
                "notify: /usr/local/bin/notify-local",
                "notify: /usr/local/bin/other-notify",
            )
            .replace(
                "  - id: 2\n    raft_address: \"127.0.0.2:17101\"\n    client_submit_address: \"127.0.0.2:17102\"\n  - id: 1\n    raft_address: \"127.0.0.1:17101\"\n    client_submit_address: \"127.0.0.1:17102\"",
                "  - id: 1\n    raft_address: \"127.0.0.1:17101\"\n    client_submit_address: \"127.0.0.1:17102\"\n  - id: 2\n    raft_address: \"127.0.0.2:17101\"\n    client_submit_address: \"127.0.0.2:17102\"",
            );

        assert_eq!(fingerprint(BASE), fingerprint(&variant));
    }

    #[test]
    fn fingerprint_canonicalizes_ipv4_mapped_peer_endpoints() {
        let mapped = BASE
            .replace("127.0.0.1:17101", "[::ffff:127.0.0.1]:17101")
            .replace("127.0.0.1:17102", "[::ffff:127.0.0.1]:17102");

        assert_eq!(fingerprint(BASE), fingerprint(&mapped));
    }

    #[test]
    fn fingerprint_is_stable_across_vip_order_and_equivalent_defaults() {
        let explicit = BASE
            .replace("interval_ms: 1000", "interval_ms: 2000")
            .replace("stale_secs: 5", "stale_secs: 6");
        let defaulted = explicit.replace("  stale_secs: 6\n", "");
        assert_eq!(fingerprint(&explicit), fingerprint(&defaulted));

        let reordered = BASE.replace(
            "  - address: 2001:db8::100/64\n    interface: bond0\n    vlan: 200\n  - address: 192.0.2.100/24\n    interface: eth0",
            "  - address: 192.0.2.100/24\n    interface: eth0\n  - address: 2001:db8::100/64\n    interface: bond0\n    vlan: 200",
        );
        assert_eq!(fingerprint(BASE), fingerprint(&reordered));
    }

    #[test]
    fn fingerprint_is_stable_across_equivalent_effective_vlan_interfaces() {
        let direct_subinterface = BASE.replace(
            "    interface: bond0\n    vlan: 200",
            "    interface: bond0.200",
        );

        assert_eq!(fingerprint(BASE), fingerprint(&direct_subinterface));
    }

    #[test]
    fn fingerprint_ignores_failback_delay_when_failback_is_disabled() {
        let first = BASE
            .replace("failback: true", "failback: false")
            .replace("failback_delay_secs: 10", "failback_delay_secs: 1");
        let second = BASE
            .replace("failback: true", "failback: false")
            .replace("failback_delay_secs: 10", "failback_delay_secs: 99");

        assert_eq!(fingerprint(&first), fingerprint(&second));
    }

    #[test]
    fn fingerprint_is_stable_across_equivalent_staleness_thresholds() {
        let ten_seconds = BASE
            .replace("interval_ms: 1000", "interval_ms: 3000")
            .replace("stale_secs: 5", "stale_secs: 10");
        let eleven_seconds = BASE
            .replace("interval_ms: 1000", "interval_ms: 3000")
            .replace("stale_secs: 5", "stale_secs: 11");

        assert_eq!(
            config(&ten_seconds).health.effective_stale_missed_probes(),
            config(&eleven_seconds)
                .health
                .effective_stale_missed_probes()
        );
        assert_eq!(fingerprint(&ten_seconds), fingerprint(&eleven_seconds));
    }

    #[test]
    fn fingerprint_changes_for_every_consensus_input() {
        let base = fingerprint(BASE);
        let variants = [
            BASE.replace("id: 2", "id: 3"),
            BASE.replace("127.0.0.2:17101", "127.0.0.3:17101"),
            BASE.replace("127.0.0.2:17102", "127.0.0.3:17102"),
            BASE.replace("2001:db8::100/64", "2001:db8::101/64"),
            BASE.replace("2001:db8::100/64", "2001:db8::100/96"),
            BASE.replace("interface: bond0", "interface: bond1"),
            BASE.replace("vlan: 200", "vlan: 201"),
            BASE.replace("interval_ms: 1000", "interval_ms: 1100"),
            BASE.replace("stale_secs: 5", "stale_secs: 6"),
            BASE.replace(
                "election_timeout_min_ms: 400",
                "election_timeout_min_ms: 401",
            ),
            BASE.replace(
                "election_timeout_max_ms: 800",
                "election_timeout_max_ms: 801",
            ),
            BASE.replace("heartbeat_interval_ms: 250", "heartbeat_interval_ms: 251"),
            BASE.replace("max_frame_bytes: 4194304", "max_frame_bytes: 4194305"),
            BASE.replace("failover_delay_secs: 2", "failover_delay_secs: 3"),
            BASE.replace("failback: true", "failback: false"),
            BASE.replace("failback_delay_secs: 10", "failback_delay_secs: 11"),
        ];

        for variant in variants {
            assert_ne!(
                base,
                fingerprint(&variant),
                "variant did not affect identity:\n{variant}"
            );
        }
    }

    #[test]
    fn fingerprint_has_a_versioned_fixed_width_display() {
        assert_eq!(
            fingerprint(BASE),
            "v1:f910c8e193e3153962bedd4ad742c10588497f2efd31fdf709a49f7f7872d361"
        );
    }

    #[test]
    fn fingerprint_distinguishes_ipv6_peer_scope_ids() {
        let ipv6 = BASE
            .replace("127.0.0.1:17101", "[2001:db8::1]:17101")
            .replace("127.0.0.1:17102", "[2001:db8::1]:17102")
            .replace("127.0.0.2:17101", "[2001:db8::2]:17101")
            .replace("127.0.0.2:17102", "[2001:db8::2]:17102");
        let scope_three = ipv6.replace("[2001:db8::2]", "[fe80::1%3]");
        let scope_four = ipv6.replace("[2001:db8::2]", "[fe80::1%4]");

        assert_ne!(fingerprint(&scope_three), fingerprint(&scope_four));
    }
}
