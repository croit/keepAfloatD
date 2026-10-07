use super::{Config, duration_to_probe_rounds_ceil};
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
    assert_eq!(c.address_protocol, super::DEFAULT_VIP_ADDRESS_PROTOCOL);
    assert_eq!(
        c.cluster_secret.as_deref(),
        Some("test-secret-0123456789abcdef012345")
    );
    assert_eq!(c.health.effective_stale_secs(), 3);
    assert_eq!(c.health.effective_stale_missed_probes(), 3);
}

fn with_raft(min: u64, max: u64, heartbeat: u64) -> String {
    format!(
        "{MINIMAL_YAML}\nraft:\n  election_timeout_min_ms: {min}\n  election_timeout_max_ms: {max}\n  heartbeat_interval_ms: {heartbeat}\n"
    )
}

#[test]
fn raft_timing_rejects_zero_values() {
    for yaml in [
        with_raft(0, 800, 250),
        with_raft(400, 0, 250),
        with_raft(400, 800, 0),
    ] {
        let err = parse_normalize(&yaml).unwrap_err().to_string();
        assert!(
            err.contains("raft.") && err.contains("must be > 0"),
            "{err}"
        );
    }
}

#[test]
fn raft_timing_requires_heartbeat_below_election_min() {
    let err = parse_normalize(&with_raft(400, 800, 400))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "raft.heartbeat_interval_ms (400) must be < raft.election_timeout_min_ms (400)"
        ),
        "{err}"
    );
    assert!(parse_normalize(&with_raft(400, 800, 399)).is_ok());
}

#[test]
fn raft_timing_requires_election_min_below_max() {
    let err = parse_normalize(&with_raft(800, 800, 250))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "raft.election_timeout_min_ms (800) must be < raft.election_timeout_max_ms (800)"
        ),
        "{err}"
    );
    assert!(parse_normalize(&with_raft(799, 800, 250)).is_ok());
}

#[test]
fn unknown_keys_are_rejected_at_every_level() {
    let cases = [
        format!("{MINIMAL_YAML}\nfailbck: true\n"),
        MINIMAL_YAML.replace("    raft_address:", "    raft_addres:"),
        MINIMAL_YAML.replace("    interface: lo", "    iface: lo"),
        MINIMAL_YAML.replace("  interval_ms: 1000", "  intervall_ms: 1000"),
        with_raft(400, 800, 250).replace("heartbeat_interval_ms", "heartbeat_ms"),
    ];
    for yaml in cases {
        let err = parse_normalize(&yaml).unwrap_err().to_string();
        assert!(err.contains("unknown field"), "{yaml}\n{err}");
    }
}

#[test]
fn fixture_configs_pass_full_validation() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut dirs = vec![
        root.join("examples"),
        root.join("tests/haproxy-e2e/configs"),
    ];
    for entry in std::fs::read_dir(root.join("tests/e2e")).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().starts_with("configs") {
            dirs.push(entry.path());
        }
    }
    let mut checked = 0;
    for dir in dirs {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "yaml") {
                let yaml = std::fs::read_to_string(&path).unwrap().replace(
                    super::INSECURE_CLUSTER_SECRET_PLACEHOLDER,
                    "fixture-only-secret-0123456789abcdef",
                );
                parse_normalize(&yaml).unwrap_or_else(|e| panic!("{}: {e:#}", path.display()));
                checked += 1;
            }
        }
    }
    assert!(
        checked >= 20,
        "expected the fixture configs, found {checked}"
    );
}

#[test]
fn raft_frame_cap_preserves_main_resource_bound() {
    assert!(parse_normalize(&format!("{MINIMAL_YAML}\nmax_frame_bytes: 16777216\n")).is_ok());
    assert!(parse_normalize(&format!("{MINIMAL_YAML}\nmax_frame_bytes: 16777217\n")).is_err());
}

#[test]
fn address_protocol_accepts_a_custom_instance_marker_and_rejects_zero() {
    let custom = MINIMAL_YAML.replace(
        "cluster_secret: \"test-secret-0123456789abcdef012345\"",
        "cluster_secret: \"test-secret-0123456789abcdef012345\"\naddress_protocol: 245",
    );
    assert_eq!(parse_normalize(&custom).unwrap().address_protocol, 245);

    let zero = custom.replace("address_protocol: 245", "address_protocol: 0");
    let error = parse_normalize(&zero).unwrap_err().to_string();
    assert!(error.contains("address_protocol must be between 1 and 255"));
}

#[test]
fn config_without_cluster_secret_is_rejected() {
    // Secure by default: loading a config with no shared secret fails closed.
    let yaml = MINIMAL_YAML.replace(
        "cluster_secret: \"test-secret-0123456789abcdef012345\"\n",
        "",
    );
    let err = parse_normalize(&yaml).unwrap_err().to_string();
    assert!(err.contains("cluster_secret is required"), "{err}");
}

#[test]
fn public_example_cluster_secret_placeholder_is_rejected() {
    let yaml = include_str!("../../config.example.yaml");
    let err = parse_normalize(yaml).unwrap_err().to_string();
    assert!(err.contains("replace cluster_secret placeholder"), "{err}");
}

#[test]
fn public_example_accepts_generated_hex_cluster_secret() {
    let yaml = include_str!("../../config.example.yaml").replace(
        "cluster_secret: \"replace-me-with-a-random-32-byte-string\"",
        "cluster_secret: \"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"",
    );
    parse_normalize(&yaml).unwrap();
}

#[test]
fn vips_sorted_by_address_after_normalize() {
    let yaml = r#"
node_id: 2
raft_listen: "127.0.0.1:3"
client_submit_listen: "127.0.0.1:4"
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
fn oversized_max_frame_rejected() {
    let yaml = format!("{MINIMAL_YAML}\nmax_frame_bytes: 67108865\n");
    let err = parse_normalize(&yaml).unwrap_err().to_string();
    assert!(err.contains("max_frame_bytes must be <= 16 MiB"), "{err}");
}

#[test]
fn stale_secs_smaller_than_interval_rejected() {
    let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret-0123456789abcdef012345"
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

fn with_health_timing(interval_ms: u64, timeout_ms: u64, stale_secs: Option<u64>) -> String {
    let mut yaml = MINIMAL_YAML
        .replace("interval_ms: 1000", &format!("interval_ms: {interval_ms}"))
        .replace("timeout_ms: 500", &format!("timeout_ms: {timeout_ms}"));
    if let Some(stale_secs) = stale_secs {
        yaml.push_str(&format!("  stale_secs: {stale_secs}\n"));
    }
    yaml
}

#[test]
fn health_timeout_must_fit_explicit_stale_window() {
    for (interval_ms, timeout_ms, stale_secs) in [
        (1000, 3000, 3),
        (1000, 3001, 3),
        (500, 1000, 1),
        (500, 5000, 1),
    ] {
        let yaml = with_health_timing(interval_ms, timeout_ms, Some(stale_secs));
        let error = parse_normalize(&yaml)
            .expect_err("a health probe must not consume the entire stale window")
            .to_string();
        assert!(error.contains("health.timeout_ms"), "{error}");
        assert!(error.contains("effective stale window"), "{error}");
    }
}

#[test]
fn health_timeout_must_fit_default_stale_window() {
    for (interval_ms, timeout_ms) in [(1000, 3000), (2000, 6000), (1500, 6000)] {
        let yaml = with_health_timing(interval_ms, timeout_ms, None);
        let error = parse_normalize(&yaml)
            .expect_err("the default stale window must also bound probe time")
            .to_string();
        assert!(error.contains("effective stale window"), "{error}");
    }
}

#[test]
fn health_timeout_uses_whole_probe_rounds_not_raw_stale_seconds() {
    for (interval_ms, timeout_ms, stale_secs, effective_ms) in [
        (1500, 1500, 2, 1500),
        (1500, 1999, 2, 1500),
        (1200, 3600, 4, 3600),
    ] {
        let yaml = with_health_timing(interval_ms, timeout_ms, Some(stale_secs));
        let error = parse_normalize(&yaml)
            .expect_err("unused fractional stale time must not authorize a slow probe")
            .to_string();
        assert!(error.contains(&format!("({timeout_ms} ms)")), "{error}");
        assert!(error.contains(&format!("({effective_ms} ms)")), "{error}");
    }
}

#[test]
fn health_timeout_just_below_stale_window_remains_valid() {
    for (interval_ms, timeout_ms, stale_secs) in [
        (1000, 2999, Some(3)),
        (500, 999, Some(1)),
        (1500, 1499, Some(2)),
        (1200, 3599, Some(4)),
        (1000, 2999, None),
        (2000, 5999, None),
        (1500, 5999, None),
    ] {
        let yaml = with_health_timing(interval_ms, timeout_ms, stale_secs);
        parse_normalize(&yaml).unwrap_or_else(|error| panic!("{yaml}: {error}"));
    }
}

#[test]
fn health_timeout_can_exceed_interval_with_sufficient_stale_window() {
    let yaml = with_health_timing(2000, 5000, Some(10));
    parse_normalize(&yaml).unwrap();
}

#[test]
fn health_timeout_stale_window_arithmetic_does_not_wrap() {
    for interval_ms in [1, 1000, u64::MAX] {
        let timeout_ms = interval_ms.min(u64::MAX - 1);
        let yaml = with_health_timing(interval_ms, timeout_ms, Some(u64::MAX));
        parse_normalize(&yaml).unwrap();
    }
    let yaml = with_health_timing(u64::MAX, u64::MAX, Some(u64::MAX));
    let error = parse_normalize(&yaml)
        .expect_err("a saturated stale window must still reject an equal timeout")
        .to_string();
    assert!(error.contains("effective stale window"), "{error}");
}

#[test]
fn canonical_examples_have_valid_health_timing() {
    for yaml in [
        include_str!("../../config.example.yaml"),
        include_str!("../../examples/node1.yaml"),
        include_str!("../../examples/node2.yaml"),
        include_str!("../../examples/node3.yaml"),
    ] {
        let yaml = yaml.replace(
            super::INSECURE_CLUSTER_SECRET_PLACEHOLDER,
            "example-test-secret-0123456789abcdef012345",
        );
        parse_normalize(&yaml).unwrap();
    }
}

#[test]
fn raft_listen_must_match_local_peer_raft_address() {
    let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:17099"
client_submit_listen: "127.0.0.1:17102"
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
fn mixed_ip_families_in_raft_roster_are_rejected() {
    let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:17101"
client_submit_listen: "127.0.0.1:17102"
cluster_secret: "test-secret-0123456789abcdef012345"
peers:
  - id: 1
    raft_address: "127.0.0.1:17101"
    client_submit_address: "127.0.0.1:17102"
  - id: 2
    raft_address: "[2001:db8::2]:17101"
    client_submit_address: "127.0.0.2:17102"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
    let err = parse_normalize(yaml).unwrap_err().to_string();
    assert!(
        err.contains("raft_address roster must use one IP family"),
        "{err}"
    );
}

#[test]
fn mixed_ip_families_in_submit_roster_are_rejected() {
    let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:17101"
client_submit_listen: "127.0.0.1:17102"
cluster_secret: "test-secret-0123456789abcdef012345"
peers:
  - id: 1
    raft_address: "127.0.0.1:17101"
    client_submit_address: "127.0.0.1:17102"
  - id: 2
    raft_address: "127.0.0.2:17101"
    client_submit_address: "[2001:db8::2]:17102"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;
    let err = parse_normalize(yaml).unwrap_err().to_string();
    assert!(
        err.contains("client_submit_address roster must use one IP family"),
        "{err}"
    );
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
    assert_eq!(hc(1, None).effective_stale_missed_probes(), 3_000);
    assert_eq!(hc(1000, None).effective_stale_secs(), 3);
    assert_eq!(hc(1000, None).effective_stale_missed_probes(), 3);
    assert_eq!(hc(2000, None).effective_stale_secs(), 6);
    assert_eq!(hc(2000, None).effective_stale_missed_probes(), 3);
    assert_eq!(hc(5000, None).effective_stale_secs(), 15);
    assert_eq!(hc(5000, None).effective_stale_missed_probes(), 3);
    // Explicit override is honored and converted to whole missed rounds (floor): eligibility
    // uses `lag > threshold`, so fencing occurs on the first probe after the duration.
    assert_eq!(hc(1000, Some(10)).effective_stale_secs(), 10);
    assert_eq!(hc(1000, Some(10)).effective_stale_missed_probes(), 10);
    assert_eq!(hc(3000, Some(10)).effective_stale_missed_probes(), 3);
    assert_eq!(hc(1500, Some(5)).effective_stale_missed_probes(), 3);
    assert_eq!(hc(500, Some(5)).effective_stale_missed_probes(), 10);
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
fn duplicate_addresses_with_different_prefixes_are_rejected() {
    let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret-0123456789abcdef012345"
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
    let error = parse_normalize(yaml).unwrap_err().to_string();
    assert!(error.contains("conflicting VIP"), "{error}");
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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

#[test]
fn ipv4_mapped_raft_and_ipv4_submit_aliases_are_rejected() {
    let yaml = r#"
node_id: 1
raft_listen: "[::ffff:127.0.0.1]:17001"
client_submit_listen: "127.0.0.1:17001"
cluster_secret: "test-secret-0123456789abcdef012345"
peers:
  - id: 1
    raft_address: "[::ffff:127.0.0.1]:17001"
    client_submit_address: "127.0.0.1:17001"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;

    let error = parse_normalize(yaml)
        .expect_err("Linux treats IPv4-mapped and IPv4 listeners as one endpoint");
    assert!(error.to_string().contains("must differ"));
}

fn two_peer_yaml(second_raft: &str, second_submit: &str) -> String {
    format!(
        r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret-0123456789abcdef012345"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
  - id: 2
    raft_address: "{second_raft}"
    client_submit_address: "{second_submit}"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#
    )
}

#[test]
fn duplicate_peer_raft_endpoints_are_rejected() {
    let err = parse_normalize(&two_peer_yaml("127.0.0.1:1", "127.0.0.1:4"))
        .expect_err("two Raft identities cannot share one endpoint");
    assert!(err.to_string().contains("endpoint"));
}

#[test]
fn duplicate_peer_submit_endpoints_are_rejected() {
    let err = parse_normalize(&two_peer_yaml("127.0.0.1:3", "127.0.0.1:2"))
        .expect_err("two submit identities cannot share one endpoint");
    assert!(err.to_string().contains("endpoint"));
}

#[test]
fn cross_peer_raft_and_submit_endpoint_collision_is_rejected() {
    let err = parse_normalize(&two_peer_yaml("127.0.0.1:2", "127.0.0.1:4"))
        .expect_err("Raft and submit listeners cannot collide across peers");
    assert!(err.to_string().contains("endpoint"));
}

fn vlan_yaml(vlan_line: &str) -> String {
    format!(
        r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
fn invalid_vlan_duplicate_blocks_startup() {
    let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret-0123456789abcdef012345"
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
    let error = parse_normalize(yaml).unwrap_err().to_string();
    assert!(error.contains("vlan 0 is out of range"), "{error}");
}

#[test]
fn vlan_flows_through_sorted_vips_without_regression_on_no_vlan() {
    // Mix: one VLAN VIP, one plain VIP.
    let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
cluster_secret: "test-secret-0123456789abcdef012345"
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
    // interval_ms=1000, interval_secs=1, delay_ticks = ceil(10/1) = 10
    let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret-0123456789abcdef012345"
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

    // interval_ms=3000, interval_secs=3, delay_ticks = ceil(10/3) = 4
    let yaml2 = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret-0123456789abcdef012345"
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

    // interval_ms=500, delay_ticks = ceil(5000/500) = 10
    let yaml3 = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret-0123456789abcdef012345"
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
    assert_eq!(c3.effective_failback_delay_ticks(), 10);

    let yaml4 = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret-0123456789abcdef012345"
peers:
  - id: 1
    raft_address: "127.0.0.1:1"
    client_submit_address: "127.0.0.1:2"
vips:
  - address: 10.0.0.1
    interface: lo
health:
  command: ["/bin/true"]
  interval_ms: 1500
  timeout_ms: 500
failback_delay_secs: 5
"#;
    let c4 = parse_normalize(yaml4).unwrap();
    assert_eq!(c4.effective_failback_delay_ticks(), 4);
}

#[test]
fn failover_delay_defaults_to_zero_and_converts_exact_milliseconds() {
    let defaulted = parse_normalize(MINIMAL_YAML).unwrap();
    assert_eq!(defaulted.failover_delay_secs, 0);
    assert_eq!(defaulted.effective_failover_delay_ticks(), 0);

    let yaml = r#"
node_id: 1
raft_listen: "127.0.0.1:1"
client_submit_listen: "127.0.0.1:2"
cluster_secret: "test-secret-0123456789abcdef012345"
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
failover_delay_secs: 5
"#;
    let configured = parse_normalize(yaml).unwrap();
    assert_eq!(configured.failover_delay_secs, 5);
    assert_eq!(configured.effective_failover_delay_ticks(), 10);
    assert_eq!(duration_to_probe_rounds_ceil(5, 100), 50);
    assert_eq!(duration_to_probe_rounds_ceil(5, 500), 10);
    assert_eq!(duration_to_probe_rounds_ceil(5, 1_500), 4);
    assert_eq!(duration_to_probe_rounds_ceil(5, 3_000), 2);
}
