//! Reject unsafe health timing before touching VIPs or starting listeners.

use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;

struct Fixture(PathBuf);

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[tokio::test]
async fn unsafe_health_timing_fails_before_startup_effects() {
    let path =
        std::env::temp_dir().join(format!("keepafloatd-config-timing-{}", std::process::id()));
    fs::create_dir(&path).unwrap();
    let dir = Fixture(path);
    // Keep both ports reserved so a missing validation cannot start a live daemon.
    let raft = TcpListener::bind("127.0.0.1:0").unwrap();
    let submit = TcpListener::bind("127.0.0.1:0").unwrap();
    let raft_addr = raft.local_addr().unwrap().to_string();
    let submit_addr = submit.local_addr().unwrap().to_string();
    for (interval, probe_timeout, stale, effective) in [
        (1000, 3000, None, 3000),
        (500, 5000, Some(1), 1000),
        (1500, 1999, Some(2), 1500),
    ] {
        let mut config = serde_json::json!({
            "node_id": 1,
            "raft_listen": raft_addr,
            "client_submit_listen": submit_addr,
            "peers": [{"id": 1, "raft_address": raft_addr,
                       "client_submit_address": submit_addr}],
            "vips": [{"address": "192.0.2.100", "interface": "lo"}],
            "cluster_secret": "timing-regression-fixture-0123456789",
            "dry_run": true,
            "health": {"command": ["unused-probe"], "interval_ms": interval,
                       "timeout_ms": probe_timeout}
        });
        if let Some(stale) = stale {
            config["health"]["stale_secs"] = stale.into();
        }
        let path = dir.0.join("config.json");
        fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let output = timeout(
            Duration::from_secs(2),
            Command::new(env!("CARGO_BIN_EXE_keepafloatd"))
                .arg("--config")
                .arg(&path)
                .env("RUST_LOG", "info")
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("invalid configuration must fail immediately")
        .unwrap();
        let logs = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.status.code(), Some(1), "{logs}");
        assert!(logs.contains("load config"), "{logs}");
        assert!(
            logs.contains(&format!("health.timeout_ms ({probe_timeout} ms)")),
            "{logs}"
        );
        assert!(
            logs.contains(&format!("effective stale window ({effective} ms)")),
            "{logs}"
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
