use super::{Config, VipConfig};

const SECRET: &str = "config-regression-secret-0123456789";
const YAML: &str = r#"
node_id: 1
raft_listen: "127.0.0.1:17101"
client_submit_listen: "127.0.0.1:17102"
cluster_secret: "config-regression-secret-0123456789"
peers:
  - id: 1
    raft_address: "127.0.0.1:17101"
    client_submit_address: "127.0.0.1:17102"
vips:
  - address: "192.0.2.10/24"
    interface: eth0
health:
  command: ["/bin/true"]
  interval_ms: 1000
  timeout_ms: 500
"#;

fn config() -> Config {
    serde_yaml::from_str(YAML).unwrap()
}

fn vip(address: &str, interface: &str, vlan: Option<u16>) -> VipConfig {
    VipConfig {
        address: address.parse().unwrap(),
        interface: interface.into(),
        vlan,
    }
}

#[test]
fn valid_config_normalization_preserves_effective_bindings_and_identity() {
    let mut first = config();
    first.vips = vec![
        vip("2001:db8::10/64", "eth0", None),
        vip("192.0.2.10/24", "eth0", Some(100)),
        vip("192.0.2.10/24", "eth0", Some(100)),
    ];
    first.normalize().unwrap();
    assert_eq!(first.cluster_secret.as_deref(), Some(SECRET));
    assert_eq!(first.sorted_vips().len(), 2);
    assert_eq!(first.sorted_vips()[0].1, "eth0.100");
    let mut second: Config = serde_yaml::from_str(&serde_yaml::to_string(&first).unwrap()).unwrap();
    second.vips.reverse();
    second.normalize().unwrap();
    assert_eq!(first.sorted_vips(), second.sorted_vips());
    assert_eq!(
        first.cluster_config_fingerprint().unwrap(),
        second.cluster_config_fingerprint().unwrap()
    );
}

#[test]
fn debug_never_contains_the_cluster_secret() {
    let c = config();
    for diagnostic in [format!("{c:?}"), format!("{c:#?}")] {
        assert!(!diagnostic.contains(SECRET), "Debug exposed the secret");
        assert!(diagnostic.contains("[REDACTED]"));
        assert!(diagnostic.contains("node_id"));
    }
}

#[test]
fn secret_must_be_long_enough_and_contain_no_whitespace_or_controls() {
    for secret in [
        "a".repeat(31),
        " ".repeat(32),
        format!("{SECRET}\n"),
        format!("{SECRET}\0"),
        format!("{SECRET}\u{2003}"),
    ] {
        let mut c = config();
        c.cluster_secret = Some(secret.clone());
        let error = c
            .normalize()
            .expect_err("unsafe secret accepted")
            .to_string();
        assert!(error.contains("cluster_secret"));
        assert!(!error.contains(&secret));
    }
    for size in [32, 256] {
        let mut c = config();
        c.cluster_secret = Some("a".repeat(size));
        c.normalize().unwrap();
    }
}

#[test]
fn vip_validation_rejects_non_host_addresses_and_invalid_prefixes() {
    for address in [
        "0.0.0.0",
        "127.0.0.1",
        "224.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "ff02::1",
        "192.0.2.0/24",
        "192.0.2.255/24",
        "192.0.2.1/0",
        "2001:db8::1/0",
    ] {
        let mut c = config();
        c.vips = vec![vip(address, "eth0", None)];
        assert!(c.normalize().is_err(), "accepted {address}");
    }
    for address in [
        "192.0.2.0/31",
        "192.0.2.1/31",
        "192.0.2.0/32",
        "192.0.2.255/32",
        "2001:db8::/64",
        "fe80::10/64",
    ] {
        let mut c = config();
        c.vips = vec![vip(address, "eth0", None)];
        c.normalize().unwrap();
    }
}

#[test]
fn interface_validation_covers_base_and_effective_vlan_names() {
    for (name, vlan) in [
        ("", None),
        (".", None),
        ("..", None),
        ("eth/0", None),
        ("eth:0", None),
        ("eth 0", None),
        ("eth\n0", None),
        ("eth\0", None),
        ("abcdefghijklmnoq", None),
        ("abcdefghijk", Some(4094)),
        ("eth\u{2003}0", None),
    ] {
        let mut c = config();
        c.vips = vec![vip("192.0.2.10/24", name, vlan)];
        assert!(c.normalize().is_err(), "accepted {name:?}, {vlan:?}");
    }
    for (name, vlan) in [
        ("abcdefghijklmno", None),
        ("eth0.100", None),
        ("abcdefghij", Some(4094)),
        ("bond-0", Some(1)),
    ] {
        let mut c = config();
        c.vips = vec![vip("192.0.2.10/24", name, vlan)];
        c.normalize().unwrap();
    }
}

#[test]
fn conflicting_duplicate_vips_are_rejected_in_both_orders() {
    for other in [
        vip("192.0.2.10/32", "eth0", None),
        vip("192.0.2.10/24", "eth1", None),
        vip("192.0.2.10/24", "eth0", Some(100)),
    ] {
        for reverse in [false, true] {
            let mut c = config();
            c.vips.push(other.clone());
            if reverse {
                c.vips.reverse();
            }
            let error = c
                .normalize()
                .expect_err("conflicting VIP was hidden")
                .to_string();
            assert!(error.contains("conflicting VIP"), "{error}");
        }
    }
}

#[test]
fn equivalent_duplicate_interfaces_deduplicate_with_stable_identity() {
    let mut c = config();
    c.vips = vec![
        vip("192.0.2.10/24", "eth0", Some(100)),
        vip("192.0.2.10/24", "eth0.100", None),
    ];
    let mut reversed = c.clone();
    reversed.vips.reverse();
    c.normalize().unwrap();
    reversed.normalize().unwrap();
    assert_eq!(c.sorted_vips().len(), 1);
    assert_eq!(c.sorted_vips(), reversed.sorted_vips());
    assert_eq!(
        c.cluster_config_fingerprint().unwrap(),
        reversed.cluster_config_fingerprint().unwrap()
    );
}

#[test]
fn invalid_duplicate_cannot_be_hidden_by_deduplication() {
    let mut c = config();
    c.vips.push(vip("192.0.2.10/24", "eth0", Some(0)));
    assert!(c.normalize().is_err(), "invalid duplicate was discarded");
}

#[test]
fn ipv4_mapped_vips_canonicalize_without_changing_host_prefix() {
    for (mapped, ipv4) in [
        ("::ffff:192.0.2.10", "192.0.2.10/32"),
        ("::ffff:192.0.2.10/120", "192.0.2.10/24"),
    ] {
        let mut c = config();
        c.vips = vec![vip(mapped, "eth0", None), vip(ipv4, "eth0", None)];
        c.normalize().unwrap();
        assert_eq!(
            c.sorted_vips(),
            vec![(ipv4.parse().unwrap(), "eth0".into())]
        );
    }
    let mut c = config();
    c.vips = vec![vip("::ffff:192.0.2.10/95", "eth0", None)];
    assert!(c.normalize().is_err());
}

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("kaf-config-{}-{id}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn load(&self, yaml: &str) -> anyhow::Result<std::sync::Arc<Config>> {
        let path = self.0.join("config.yaml");
        std::fs::write(&path, yaml).unwrap();
        Config::load_path(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn file_yaml(path: &std::path::Path) -> String {
    YAML.replace(
        &format!("cluster_secret: \"{SECRET}\""),
        &format!(
            "cluster_secret_file: {}",
            serde_json::to_string(path).unwrap()
        ),
    )
}

#[test]
fn file_secret_resolves_relative_to_yaml_and_matches_inline_identity() {
    let fixture = Fixture::new();
    let path = fixture.0.join("secret");
    for ending in ["", "\n", "\r\n"] {
        std::fs::write(&path, format!("{SECRET}{ending}")).unwrap();
        for name in [std::path::Path::new("secret"), path.as_path()] {
            let loaded = fixture.load(&file_yaml(name)).unwrap();
            assert_eq!(loaded.cluster_secret.as_deref(), Some(SECRET));
            let inline = fixture.load(YAML).unwrap();
            assert_eq!(
                loaded.cluster_config_fingerprint().unwrap(),
                inline.cluster_config_fingerprint().unwrap()
            );
            assert!(!format!("{loaded:?}").contains(SECRET));
        }
    }
}

#[test]
fn ambiguous_secret_sources_fail_without_disclosing_values() {
    let fixture = Fixture::new();
    let yaml = format!("{YAML}\ncluster_secret_file: absent\n");
    let error = format!("{:#}", fixture.load(&yaml).unwrap_err());
    assert!(error.contains("exactly one"), "{error}");
    assert!(!error.contains(SECRET));
}

#[test]
fn secret_file_failures_are_bounded_and_do_not_disclose_contents() {
    let fixture = Fixture::new();
    let path = fixture.0.join("secret");
    for contents in [
        vec![],
        vec![b'a'; 31],
        vec![b'a'; 259],
        vec![0xff; 32],
        format!("{SECRET}\n\n").into_bytes(),
        format!(" {SECRET}").into_bytes(),
    ] {
        std::fs::write(&path, &contents).unwrap();
        let error = format!("{:#}", fixture.load(&file_yaml(&path)).unwrap_err());
        assert!(error.contains("cluster_secret"), "{error}");
        assert!(!error.contains(SECRET));
    }
    for path in [fixture.0.join("absent"), fixture.0.clone()] {
        let error = format!("{:#}", fixture.load(&file_yaml(&path)).unwrap_err());
        assert!(error.contains("cluster_secret_file"), "{error}");
    }
}

#[test]
fn file_secret_boundaries_and_source_errors_are_explicit() {
    let fixture = Fixture::new();
    let path = fixture.0.join("secret");
    for size in [32, 256] {
        for ending in ["", "\n", "\r\n"] {
            std::fs::write(&path, format!("{}{ending}", "z".repeat(size))).unwrap();
            assert_eq!(
                fixture
                    .load(&file_yaml(&path))
                    .unwrap()
                    .cluster_secret
                    .as_ref()
                    .unwrap()
                    .len(),
                size
            );
        }
    }
    for (contents, expected) in [
        (vec![b'x'; 259], "too large"),
        (vec![b'x'; 257], "32 to 256"),
        (vec![0xff; 32], "UTF-8 text"),
        (format!("{SECRET}\r").into_bytes(), "whitespace"),
        (format!("{SECRET}\n{SECRET}").into_bytes(), "whitespace"),
        (
            b"replace-me-with-a-random-32-byte-string".to_vec(),
            "placeholder",
        ),
    ] {
        std::fs::write(&path, contents).unwrap();
        let error = format!("{:#}", fixture.load(&file_yaml(&path)).unwrap_err());
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains(SECRET));
    }
    for (name, expected) in [
        (std::path::Path::new(""), "must not be empty"),
        (fixture.0.as_path(), "regular file"),
    ] {
        let error = format!("{:#}", fixture.load(&file_yaml(name)).unwrap_err());
        assert!(error.contains(expected), "{error}");
    }
    let yaml = YAML.replace(&format!("cluster_secret: \"{SECRET}\""), "");
    assert!(
        fixture
            .load(&yaml)
            .unwrap_err()
            .to_string()
            .contains("required")
    );
    let mut unresolved = config();
    unresolved.cluster_secret_file = Some(path);
    assert!(
        unresolved
            .normalize()
            .unwrap_err()
            .to_string()
            .contains("resolved")
    );
    unresolved.cluster_secret = None;
    assert!(!format!("{unresolved:?}").contains(SECRET));
}
