use super::{Config, INSECURE_CLUSTER_SECRET_PLACEHOLDER};
use anyhow::Context;
use std::fmt;
use std::fs::{File, Metadata, OpenOptions};
use std::io::Read;
use std::path::Path;

const MIN_SECRET_BYTES: usize = 32;
const MAX_SECRET_BYTES: usize = 256;

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("node_id", &self.node_id)
            .field("raft_listen", &self.raft_listen)
            .field("client_submit_listen", &self.client_submit_listen)
            .field("peers", &self.peers)
            .field("vips", &self.vips)
            .field("health", &self.health)
            .field("raft", &self.raft)
            .field(
                "cluster_secret",
                &self.cluster_secret.as_ref().map(|_| "[REDACTED]"),
            )
            .field("cluster_secret_file", &self.cluster_secret_file)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("submit_timeout_ms", &self.submit_timeout_ms)
            .field("address_protocol", &self.address_protocol)
            .field("dry_run", &self.dry_run)
            .field("notify", &self.notify)
            .field("failover_delay_secs", &self.failover_delay_secs)
            .field("failback", &self.failback)
            .field("failback_delay_secs", &self.failback_delay_secs)
            .finish()
    }
}

impl Config {
    pub(super) fn resolve_secret(&mut self, config_path: &Path) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.cluster_secret.is_none() || self.cluster_secret_file.is_none(),
            "configure exactly one of cluster_secret and cluster_secret_file"
        );
        if let Some(path) = self.cluster_secret_file.take() {
            anyhow::ensure!(
                !path.as_os_str().is_empty(),
                "cluster_secret_file must not be empty"
            );
            let path = config_path.parent().unwrap_or(Path::new(".")).join(path);
            self.cluster_secret = Some(read_secret(&path).context("read cluster_secret_file")?);
        } else if self.cluster_secret.is_some() {
            let metadata = std::fs::metadata(config_path).context("inspect config permissions")?;
            warn_permissions(&metadata, config_path, "configuration");
        }
        Ok(())
    }

    pub(super) fn validate_secret(&self) -> anyhow::Result<()> {
        // Secure by default: a loaded config must carry a shared secret. Without it the Raft and
        // submit listeners would accept unauthenticated requests from any reachable host, so we
        // fail closed at load time rather than run an open control plane.
        anyhow::ensure!(
            self.cluster_secret_file.is_none(),
            "cluster_secret_file must be resolved at load time"
        );
        let secret = self
            .cluster_secret
            .as_deref()
            .context("cluster_secret is required (inline or file)")?;
        anyhow::ensure!(
            ![
                INSECURE_CLUSTER_SECRET_PLACEHOLDER,
                "lab-shared-secret-please-change"
            ]
            .contains(&secret),
            "replace cluster_secret placeholder with a unique random secret"
        );
        anyhow::ensure!(
            (MIN_SECRET_BYTES..=MAX_SECRET_BYTES).contains(&secret.len()),
            "cluster_secret must contain 32 to 256 UTF-8 bytes"
        );
        anyhow::ensure!(
            !secret.chars().any(|c| c.is_whitespace() || c.is_control()),
            "cluster_secret must not contain whitespace or control characters"
        );
        Ok(())
    }
}

fn read_secret(path: &Path) -> anyhow::Result<String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY);
    }
    let file: File = options.open(path).context("open secret file")?;
    let metadata = file.metadata().context("inspect secret file")?;
    anyhow::ensure!(
        metadata.is_file(),
        "cluster_secret_file must be a regular file"
    );
    warn_permissions(&metadata, path, "secret");
    let mut bytes = Vec::new();
    file.take((MAX_SECRET_BYTES + 3) as u64)
        .read_to_end(&mut bytes)
        .context("read secret file")?;
    anyhow::ensure!(
        bytes.len() <= MAX_SECRET_BYTES + 2,
        "cluster_secret_file is too large"
    );
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    String::from_utf8(bytes)
        .map_err(|_| anyhow::anyhow!("cluster_secret_file must contain UTF-8 text"))
}

fn warn_permissions(metadata: &Metadata, path: &Path, kind: &str) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if insecure_permissions(metadata.permissions().mode()) {
            tracing::warn!(path = %path.display(), "{kind} file is accessible to group or others; restrict permissions to 0600");
        }
    }
    #[cfg(not(unix))]
    let _ = (metadata, path, kind);
}

fn insecure_permissions(mode: u32) -> bool {
    mode & 0o077 != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_check_ignores_owner_and_file_type_bits() {
        for mode in [0o600, 0o400, 0o100600] {
            assert!(!insecure_permissions(mode));
        }
        for mode in [0o640, 0o604, 0o601, 0o610, 0o777] {
            assert!(insecure_permissions(mode));
        }
    }
}
