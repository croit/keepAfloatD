//! Invalid secrets and VIPs must fail before host effects and listeners.

use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{process::Command, time::timeout};

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[tokio::test]
async fn invalid_config_fails_before_startup_effects() {
    let dir = Fixture(std::env::temp_dir().join(format!("kaf-preflight-{}", std::process::id())));
    fs::create_dir(&dir.0).unwrap();
    let raft = TcpListener::bind("127.0.0.1:0").unwrap();
    let submit = TcpListener::bind("127.0.0.1:0").unwrap();
    let raft = raft.local_addr().unwrap();
    let submit = submit.local_addr().unwrap();
    let secret = "startup-regression-fixture-0123456789";
    let baseline = serde_json::json!({
        "node_id": 1, "raft_listen": raft, "client_submit_listen": submit,
        "peers": [{"id": 1, "raft_address": raft, "client_submit_address": submit}],
        "cluster_secret": secret, "dry_run": true,
        "vips": [{"address": "192.0.2.10/24", "interface": "lo"}],
        "health": {"command": ["unused-probe"], "interval_ms": 1000, "timeout_ms": 500}
    });
    for (case, expected) in [
        ("short", "32 to 256"),
        ("ambiguous", "exactly one"),
        ("file", "open secret file"),
        ("multicast", "unicast"),
        ("network", "subnet network"),
        ("interface", "Linux name"),
        ("conflict", "conflicting VIP"),
    ] {
        let mut config = baseline.clone();
        match case {
            "short" => config["cluster_secret"] = "short-fixture-secret".into(),
            "ambiguous" => config["cluster_secret_file"] = "absent".into(),
            "file" => {
                config.as_object_mut().unwrap().remove("cluster_secret");
                config["cluster_secret_file"] = "absent".into();
            }
            "multicast" => config["vips"][0]["address"] = "224.0.0.1".into(),
            "network" => config["vips"][0]["address"] = "192.0.2.0/24".into(),
            "interface" => config["vips"][0]["interface"] = "bad/name".into(),
            "conflict" => config["vips"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({"address": "192.0.2.10/32", "interface": "lo"})),
            _ => unreachable!(),
        }
        let path = dir.0.join("config.json");
        fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let logs = run_invalid(&path).await;
        assert!(
            logs.contains("load config") && logs.contains(expected),
            "{case}: {logs}"
        );
        assert!(
            !logs.contains(secret) && !logs.contains("short-fixture-secret"),
            "secret leaked"
        );
        assert!(
            !logs.contains("dry-run:"),
            "VIP effect before validation: {logs}"
        );
        assert!(
            !logs.contains("start raft"),
            "listeners before validation: {logs}"
        );
    }
}

async fn run_invalid(path: &Path) -> String {
    let output = timeout(
        Duration::from_secs(2),
        Command::new(env!("CARGO_BIN_EXE_keepafloatd"))
            .arg("--config")
            .arg(path)
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("invalid config must fail immediately")
    .unwrap();
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.status.code(), Some(1), "{logs}");
    logs
}

#[cfg(unix)]
#[tokio::test]
async fn permission_warnings_only_name_insecure_secret_bearing_files() {
    use std::os::unix::fs::PermissionsExt;
    let dir = Fixture(std::env::temp_dir().join(format!("kaf-permissions-{}", std::process::id())));
    fs::create_dir(&dir.0).unwrap();
    let secret = "permission-regression-fixture-0123456789";
    let secret_path = dir.0.join("cluster.secret");
    fs::write(&secret_path, secret).unwrap();
    let config_path = dir.0.join("config.json");
    let raft = TcpListener::bind("127.0.0.1:0").unwrap();
    let submit = TcpListener::bind("127.0.0.1:0").unwrap();
    for file_backed in [false, true] {
        let mut config = serde_json::json!({
            "node_id": 1, "raft_listen": raft.local_addr().unwrap(),
            "client_submit_listen": submit.local_addr().unwrap(),
            "peers": [{"id": 1, "raft_address": raft.local_addr().unwrap(),
                "client_submit_address": submit.local_addr().unwrap()}],
            "dry_run": true,
            "vips": [{"address": "224.0.0.1", "interface": "lo"}],
            "health": {"command": ["unused-probe"], "interval_ms": 1000, "timeout_ms": 500}
        });
        if file_backed {
            config["cluster_secret_file"] = secret_path.to_str().unwrap().into();
        } else {
            config["cluster_secret"] = secret.into();
        }
        fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
        fs::set_permissions(&config_path, fs::Permissions::from_mode(0o644)).unwrap();
        let source = if file_backed {
            &secret_path
        } else {
            &config_path
        };
        for mode in [0o600, 0o644] {
            fs::set_permissions(source, fs::Permissions::from_mode(mode)).unwrap();
            let logs = run_invalid(&config_path).await;
            assert!(logs.contains("unicast"), "{logs}");
            assert_eq!(
                logs.contains("restrict permissions to 0600"),
                mode == 0o644,
                "{logs}"
            );
            if mode == 0o644 {
                assert!(logs.contains(source.to_str().unwrap()), "{logs}");
            }
            assert!(!logs.contains(secret), "secret leaked");
        }
    }
}
