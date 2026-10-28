use super::{Config, MINIMAL_YAML, parse_normalize};
use serde_yaml::Value;

fn parse(yaml: &str) -> Config {
    serde_yaml::from_str(yaml).unwrap()
}

fn assert_parse_error(yaml: &str, expected: &str) {
    let error = serde_yaml::from_str::<Config>(yaml).unwrap_err();
    assert!(error.to_string().contains(expected), "{error}");
}

#[test]
fn omitted_fields_and_explicit_defaults_have_the_same_normalized_value() {
    let implicit = parse_normalize(MINIMAL_YAML).unwrap();
    let explicit = parse_normalize(&format!(
        "{MINIMAL_YAML}\nraft:\n  election_timeout_min_ms: 400\n  election_timeout_max_ms: 800\n  heartbeat_interval_ms: 250\nmax_frame_bytes: 4194304\nsubmit_timeout_ms: 2000\naddress_protocol: 246\ndry_run: false\nnotify: null\nfailover_delay_secs: 0\nfailback: true\nfailback_delay_secs: 10\n"
    ))
    .unwrap();
    assert_eq!(implicit.raft.election_timeout_min_ms, 400);
    assert_eq!(implicit.raft.election_timeout_max_ms, 800);
    assert_eq!(implicit.raft.heartbeat_interval_ms, 250);
    assert_eq!(implicit.max_frame_bytes, 4_194_304);
    assert_eq!(implicit.submit_timeout_ms, 2_000);
    assert_eq!(implicit.address_protocol, 246);
    assert!(!implicit.dry_run);
    assert!(implicit.failback);
    assert_eq!(implicit.failover_delay_secs, 0);
    assert_eq!(implicit.failback_delay_secs, 10);
    assert_eq!(
        serde_yaml::to_value(&implicit).unwrap(),
        serde_yaml::to_value(&explicit).unwrap()
    );
}

#[test]
fn optional_null_spellings_match_omission() {
    let baseline = parse_normalize(MINIMAL_YAML).unwrap();
    for null in ["null", "Null", "NULL", "~", ""] {
        let yaml = format!("{MINIMAL_YAML}\nnotify: {null}\ncluster_secret_file: {null}\n")
            .replace(
                "    interface: lo",
                &format!("    interface: lo\n    vlan: {null}"),
            )
            .replace(
                "  timeout_ms: 500",
                &format!("  timeout_ms: 500\n  stale_secs: {null}"),
            );
        let config = parse_normalize(&yaml).unwrap();
        assert!(config.notify.is_none());
        assert!(config.cluster_secret_file.is_none());
        assert!(config.vips[0].vlan.is_none());
        assert!(config.health.stale_secs.is_none());
        assert_eq!(config.health.effective_stale_secs(), 3);
        assert_eq!(
            config.cluster_config_fingerprint().unwrap(),
            baseline.cluster_config_fingerprint().unwrap()
        );
    }
}

#[test]
fn null_secret_parses_as_absent_but_normalization_rejects_it() {
    let absent = MINIMAL_YAML
        .lines()
        .filter(|line| !line.starts_with("cluster_secret:"))
        .collect::<Vec<_>>()
        .join("\n");
    for suffix in ["", "\ncluster_secret: null", "\ncluster_secret: ~"] {
        let yaml = format!("{absent}{suffix}");
        assert!(parse(&yaml).cluster_secret.is_none());
        assert!(
            parse_normalize(&yaml)
                .unwrap_err()
                .to_string()
                .contains("cluster_secret is required")
        );
    }
}

#[test]
fn null_is_not_a_default_for_numbers_booleans_or_collections() {
    for field in [
        "dry_run",
        "failback",
        "max_frame_bytes",
        "submit_timeout_ms",
        "address_protocol",
        "failover_delay_secs",
        "failback_delay_secs",
        "raft",
    ] {
        assert_parse_error(&format!("{MINIMAL_YAML}\n{field}: null\n"), "invalid type");
    }
    for (old, new) in [
        ("node_id: 1", "node_id: null"),
        ("peers:\n  - id: 1", "peers: null\nunused:\n  - id: 1"),
        ("  interval_ms: 1000", "  interval_ms: null"),
        ("  command: [\"/bin/true\"]", "  command: null"),
    ] {
        assert_parse_error(&MINIMAL_YAML.replace(old, new), "invalid type");
    }
    assert_parse_error(&format!("{MINIMAL_YAML}\nraft: {{}}"), "missing field");
    assert_parse_error(
        &format!("{MINIMAL_YAML}\nraft: {{heartbeat_interval_ms: 250}}"),
        "missing field",
    );
}

#[test]
fn duplicate_fields_are_rejected_at_every_mapping_level() {
    let cases = [
        (format!("{MINIMAL_YAML}\nnode_id: 1"), "node_id"),
        (
            format!("{MINIMAL_YAML}\nnotify: null\nnotify: null"),
            "notify",
        ),
        (
            MINIMAL_YAML.replace("  - id: 1", "  - id: 1\n    id: 1"),
            "id",
        ),
        (
            MINIMAL_YAML.replace("    interface: lo", "    interface: lo\n    interface: lo"),
            "interface",
        ),
        (
            MINIMAL_YAML.replace(
                "    interface: lo",
                "    interface: lo\n    vlan: null\n    vlan: null",
            ),
            "vlan",
        ),
        (
            MINIMAL_YAML.replace(
                "  interval_ms: 1000",
                "  interval_ms: 1000\n  interval_ms: 1000",
            ),
            "interval_ms",
        ),
        (
            format!(
                "{MINIMAL_YAML}\nraft:\n  heartbeat_interval_ms: 250\n  heartbeat_interval_ms: 250"
            ),
            "heartbeat_interval_ms",
        ),
    ];
    for (yaml, field) in cases {
        assert_parse_error(&yaml, &format!("duplicate field `{field}`"));
    }
    assert!(serde_yaml::from_str::<Value>("key: 1\nkey: 2").is_err());
}

#[test]
fn unknown_fields_are_rejected_even_when_null() {
    for yaml in [
        format!("{MINIMAL_YAML}\nunknown: null"),
        MINIMAL_YAML.replace("  - id: 1", "  - id: 1\n    unknown: null"),
        MINIMAL_YAML.replace("    interface: lo", "    interface: lo\n    unknown: null"),
        MINIMAL_YAML.replace("  timeout_ms: 500", "  timeout_ms: 500\n  unknown: null"),
        format!("{MINIMAL_YAML}\nraft:\n  unknown: null"),
    ] {
        assert_parse_error(&yaml, "unknown field `unknown`");
    }
}

#[test]
fn boolean_fields_accept_yaml_booleans_but_not_strings_or_yaml_11_words() {
    for field in ["dry_run", "failback"] {
        for (scalar, expected) in [
            ("true", true),
            ("True", true),
            ("TRUE", true),
            ("false", false),
            ("False", false),
            ("FALSE", false),
            ("!!bool true", true),
            ("!!str true", true),
        ] {
            let config = parse_normalize(&format!("{MINIMAL_YAML}\n{field}: {scalar}")).unwrap();
            assert_eq!(
                if field == "dry_run" {
                    config.dry_run
                } else {
                    config.failback
                },
                expected,
                "{field}: {scalar}"
            );
        }
        for scalar in [
            "yes",
            "no",
            "on",
            "off",
            "y",
            "n",
            "1",
            "0",
            "'true'",
            "\"false\"",
            "null",
            "[]",
            "{}",
        ] {
            assert_parse_error(
                &format!("{MINIMAL_YAML}\n{field}: {scalar}"),
                "invalid type",
            );
        }
    }
}

#[test]
fn unsigned_integers_accept_radix_and_plus_but_reject_coercions_and_overflow() {
    for scalar in ["1", "+1", "0x1", "0o1", "0b1", "!!int 1", "!!str 1"] {
        let yaml = MINIMAL_YAML.replace("id: 1", &format!("id: {scalar}"));
        assert_eq!(parse_normalize(&yaml).unwrap().node_id, 1, "{scalar}");
    }
    let max = MINIMAL_YAML.replace("id: 1", "id: 18446744073709551615");
    assert_eq!(parse_normalize(&max).unwrap().node_id, u64::MAX);
    for scalar in [
        "-1",
        "18446744073709551616",
        "1.0",
        "1e0",
        "01",
        "1_000",
        "'1'",
        "\"1\"",
        ".inf",
        ".nan",
        "true",
        "null",
        "[]",
        "{}",
    ] {
        let yaml = MINIMAL_YAML.replace("node_id: 1", &format!("node_id: {scalar}"));
        assert!(
            serde_yaml::from_str::<Config>(&yaml).is_err(),
            "accepted node_id: {scalar}"
        );
    }
    for (field, accepted, rejected) in [
        ("address_protocol", "255", "256"),
        ("failover_delay_secs", "4294967295", "4294967296"),
    ] {
        parse(&format!("{MINIMAL_YAML}\n{field}: {accepted}"));
        assert!(
            serde_yaml::from_str::<Config>(&format!("{MINIMAL_YAML}\n{field}: {rejected}"))
                .is_err()
        );
    }
    let vlan = |scalar| {
        MINIMAL_YAML.replace(
            "    interface: lo",
            &format!("    interface: lo\n    vlan: {scalar}"),
        )
    };
    assert_eq!(parse(&vlan("65535")).vips[0].vlan, Some(u16::MAX));
    assert!(serde_yaml::from_str::<Config>(&vlan("65536")).is_err());
    assert!(parse_normalize(&vlan("65535")).is_err());
}

#[test]
fn direct_string_fields_preserve_scalar_text_and_block_styles() {
    for (scalar, expected) in [
        ("true", "true"),
        ("FALSE", "FALSE"),
        ("123", "123"),
        ("1.25", "1.25"),
        ("0x10", "0x10"),
        ("yes", "yes"),
        ("2026-01-02", "2026-01-02"),
        ("'null'", "null"),
        ("\"\"", ""),
        ("!!str null", "null"),
        ("|-\n  /bin/true", "/bin/true"),
        (">-\n  /bin/true\n  --quiet", "/bin/true --quiet"),
    ] {
        let yaml = format!("{MINIMAL_YAML}\nnotify: {scalar}\n");
        assert_eq!(parse(&yaml).notify.as_deref(), Some(expected), "{scalar}");
    }
    let yaml = MINIMAL_YAML.replace("[\"/bin/true\"]", "[/bin/true, 123, true, 'null']");
    assert_eq!(
        parse(&yaml).health.command,
        ["/bin/true", "123", "true", "null"]
    );
    for scalar in ["[]", "{}"] {
        assert_parse_error(&format!("{MINIMAL_YAML}\nnotify: {scalar}"), "invalid type");
    }
    let null_command = MINIMAL_YAML.replace("[\"/bin/true\"]", "[null, ~, '']");
    assert_eq!(parse(&null_command).health.command, ["null", "~", ""]);
    let value: Value = serde_yaml::from_str(&null_command).unwrap();
    assert!(serde_yaml::from_value::<Config>(value).is_err());
}

#[test]
fn value_deserialization_does_not_coerce_boolean_or_numeric_strings() {
    for scalar in ["true", "123", "1.25"] {
        let yaml = format!("{MINIMAL_YAML}\nnotify: {scalar}");
        assert_eq!(parse(&yaml).notify.as_deref(), Some(scalar));
        let value: Value = serde_yaml::from_str(&yaml).unwrap();
        assert!(!value["notify"].is_string());
        assert!(
            serde_yaml::from_value::<Config>(value).is_err(),
            "coerced {scalar}"
        );
    }
    for scalar in ["'true'", "'123'", "!!str 1.25"] {
        let value: Value =
            serde_yaml::from_str(&format!("{MINIMAL_YAML}\nnotify: {scalar}")).unwrap();
        let expected = value["notify"].as_str().unwrap().to_owned();
        assert_eq!(
            serde_yaml::from_value::<Config>(value)
                .unwrap()
                .notify
                .as_deref(),
            Some(expected.as_str())
        );
    }
}

#[test]
fn aliases_expand_scalars_sequences_and_mappings_before_normalization() {
    let yaml = format!("notify: &probe /bin/true\n{MINIMAL_YAML}")
        .replacen(
            "raft_listen: \"127.0.0.1:17101\"",
            "raft_listen: &raft 127.0.0.1:17101",
            1,
        )
        .replace("raft_address: \"127.0.0.1:17101\"", "raft_address: *raft")
        .replace(
            "  - address: 10.0.0.1\n    interface: lo",
            "  - &vip\n    address: 10.0.0.1\n    interface: lo\n  - *vip",
        )
        .replace("[\"/bin/true\"]", "[*probe]");
    assert_eq!(parse(&yaml).vips.len(), 2);
    let normalized = parse_normalize(&yaml).unwrap();
    assert_eq!(normalized.vips.len(), 1);
    assert_eq!(normalized.health.command, ["/bin/true"]);
    assert_eq!(normalized.notify.as_deref(), Some("/bin/true"));
    assert_eq!(
        normalized.cluster_config_fingerprint().unwrap(),
        parse_normalize(MINIMAL_YAML)
            .unwrap()
            .cluster_config_fingerprint()
            .unwrap()
    );
    let sequence: Value =
        serde_yaml::from_str("first: &items [1, true, null]\nsecond: *items").unwrap();
    assert_eq!(sequence["first"], sequence["second"]);
    assert_parse_error(
        &MINIMAL_YAML.replace("[\"/bin/true\"]", "[*missing]"),
        "unknown anchor",
    );
}

#[test]
fn merge_keys_are_not_implicit_but_value_apply_merge_honors_overrides() {
    let yaml = MINIMAL_YAML.replace(
        "  - address: 10.0.0.1\n    interface: lo",
        "  - &vip\n    address: 10.0.0.1\n    interface: lo\n  - <<: *vip\n    address: 10.0.0.2",
    );
    assert_parse_error(&yaml, "unknown field `<<`");
    let mut value: Value = serde_yaml::from_str(&yaml).unwrap();
    assert!(
        value["vips"][1]
            .as_mapping()
            .unwrap()
            .contains_key(Value::from("<<"))
    );
    assert!(serde_yaml::from_value::<Config>(value.clone()).is_err());
    value.apply_merge().unwrap();
    assert_eq!(value["vips"][1]["address"].as_str(), Some("10.0.0.2"));
    assert_eq!(value["vips"][1]["interface"].as_str(), Some("lo"));
    let mut merged: Config = serde_yaml::from_value(value).unwrap();
    merged.normalize().unwrap();
    let explicit = parse_normalize(&MINIMAL_YAML.replace(
        "    interface: lo",
        "    interface: lo\n  - address: 10.0.0.2\n    interface: lo",
    ))
    .unwrap();
    assert_eq!(
        serde_yaml::to_value(merged).unwrap(),
        serde_yaml::to_value(explicit).unwrap()
    );
}

#[test]
fn malformed_yaml_and_non_mapping_documents_are_rejected() {
    for yaml in [
        "",
        "# comment only\n",
        "null",
        "[]",
        "scalar",
        "node_id: [1",
        "node_id: {id: 1",
        "node_id:\n\t1",
        "node_id: \"unterminated",
        "node_id: *missing",
        "? [node_id]\n: 1",
    ] {
        assert!(
            serde_yaml::from_str::<Config>(yaml).is_err(),
            "accepted {yaml:?}"
        );
    }
    assert_eq!(serde_yaml::from_str::<Value>("").unwrap(), Value::Null);
    assert_eq!(
        serde_yaml::from_str::<Value>("# comment only\n").unwrap(),
        Value::Null
    );
}

#[test]
fn single_document_markers_are_allowed_but_multiple_documents_are_rejected() {
    let marked = format!("---\n{MINIMAL_YAML}\n...\n# trailing comment\n");
    assert_eq!(
        serde_yaml::to_value(parse(&marked)).unwrap(),
        serde_yaml::to_value(parse(MINIMAL_YAML)).unwrap()
    );
    for second in ["", "null", MINIMAL_YAML] {
        let yaml = format!("{MINIMAL_YAML}\n---\n{second}");
        assert_parse_error(&yaml, "more than one document");
        assert!(serde_yaml::from_str::<Value>(&yaml).is_err());
    }
}

#[test]
fn value_mutation_and_roundtrip_preserve_normalized_config_and_fingerprint() {
    let baseline = parse_normalize(MINIMAL_YAML).unwrap();
    assert_eq!(
        baseline.cluster_config_fingerprint().unwrap().to_string(),
        "v1:a7f8f3eb1cd5a74a5daada63a71eb4c75eb6c26fb7e44413ea88b9086a67aeb4"
    );
    let mut value: Value = serde_yaml::from_str(MINIMAL_YAML).unwrap();
    value["notify"] = Value::from("/bin/notify");
    value["dry_run"] = Value::Bool(true);
    value["health"]["stale_secs"] = Value::from(3_u64);
    value["vips"][0]["address"] = Value::from("::ffff:10.0.0.1/128");
    let duplicate = value["vips"][0].clone();
    value["vips"].as_sequence_mut().unwrap().push(duplicate);
    let serialized = serde_yaml::to_string(&value).unwrap();
    assert_eq!(serde_yaml::from_str::<Value>(&serialized).unwrap(), value);
    let from_text = parse_normalize(&serialized).unwrap();
    let mut from_value: Config = serde_yaml::from_value(value).unwrap();
    from_value.normalize().unwrap();
    assert_eq!(from_text.vips.len(), 1);
    assert_eq!(from_text.vips[0].address.to_string(), "10.0.0.1/32");
    assert_eq!(from_text.notify.as_deref(), Some("/bin/notify"));
    assert!(from_text.dry_run);
    assert_eq!(
        from_text.cluster_config_fingerprint().unwrap(),
        baseline.cluster_config_fingerprint().unwrap()
    );
    let expected = serde_yaml::to_value(&from_text).unwrap();
    assert_eq!(serde_yaml::to_value(from_value).unwrap(), expected);
    let roundtrip = parse_normalize(&serde_yaml::to_string(&from_text).unwrap()).unwrap();
    assert_eq!(serde_yaml::to_value(&roundtrip).unwrap(), expected);
    assert_eq!(
        roundtrip.cluster_config_fingerprint().unwrap(),
        baseline.cluster_config_fingerprint().unwrap()
    );
    let mut changed = serde_yaml::to_value(roundtrip).unwrap();
    changed["failover_delay_secs"] = Value::from(1_u32);
    let changed = parse_normalize(&serde_yaml::to_string(&changed).unwrap()).unwrap();
    assert_ne!(
        changed.cluster_config_fingerprint().unwrap(),
        baseline.cluster_config_fingerprint().unwrap()
    );
}
