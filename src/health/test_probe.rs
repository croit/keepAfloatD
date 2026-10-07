use super::{HealthConfig, run_health_check};
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::unix::AsyncFd;
use tokio::task::JoinHandle;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);
const DEADLINE: Duration = Duration::from_secs(2);

pub(super) struct Probe {
    path: PathBuf,
    reader: AsyncFd<File>,
    task: Option<JoinHandle<bool>>,
    group: Option<i32>,
}

impl Probe {
    pub(super) async fn start(ending: &str, timeout_ms: u64) -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "keepafloatd-health-{}-{id}.fifo",
            std::process::id()
        ));
        let name = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: name is a valid C string; the FIFO is private to this fixture.
        #[allow(unsafe_code)]
        let result = unsafe { libc::mkfifo(name.as_ptr(), 0o600) };
        assert_eq!(
            result,
            0,
            "mkfifo {}: {}",
            path.display(),
            io::Error::last_os_error()
        );
        let reader = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&path)
            .unwrap();
        let mut probe = Self {
            path,
            reader: AsyncFd::new(reader).unwrap(),
            task: None,
            group: None,
        };
        // Keep EOF disabled until the shell and its descendant inherit the writer.
        let bootstrap = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&probe.path)
            .unwrap();
        let script = format!("{{ sleep 30 & printf '%s\\n' \"$$\" >&3; {ending}; }} 3>\"$1\"");
        let cfg = HealthConfig {
            command: vec![
                "/bin/sh".into(),
                "-c".into(),
                script,
                "health-probe".into(),
                probe.path.to_str().unwrap().into(),
            ],
            interval_ms: 1_000,
            timeout_ms,
            stale_secs: None,
        };
        // The fixture aborts the task and terminates its process group on drop.
        probe.task = Some(tokio::spawn(async move { run_health_check(&cfg).await }));
        let pid = tokio::time::timeout(DEADLINE, async {
            let mut bytes = Vec::new();
            loop {
                match probe.read_byte().await.unwrap() {
                    Some(b'\n') => return String::from_utf8(bytes).unwrap(),
                    Some(byte) if bytes.len() < 20 => bytes.push(byte),
                    other => panic!("invalid probe readiness message: {other:?}"),
                }
            }
        })
        .await
        .expect("probe did not open the witness channel");
        let pid: i32 = pid.parse().unwrap();
        assert!(pid > 1);
        probe.group = Some(pid);
        drop(bootstrap);
        probe
    }

    fn try_read_byte(&self) -> io::Result<Option<u8>> {
        let mut byte = [0];
        self.reader
            .get_ref()
            .read(&mut byte)
            .map(|n| (n != 0).then_some(byte[0]))
    }

    async fn read_byte(&self) -> io::Result<Option<u8>> {
        loop {
            let mut ready = self.reader.readable().await?;
            match ready.try_io(|_| self.try_read_byte()) {
                Ok(result) => return result,
                Err(_) => continue,
            }
        }
    }

    pub(super) fn assert_running(&self) {
        assert_eq!(
            self.try_read_byte().unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "a live fixture must keep the witness channel open"
        );
    }

    pub(super) async fn cancel(&mut self) {
        let task = self.task.take().unwrap();
        task.abort();
        assert!(
            tokio::time::timeout(DEADLINE, task)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
    }

    pub(super) async fn finish(&mut self) -> bool {
        tokio::time::timeout(DEADLINE, self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
    }

    pub(super) async fn assert_stopped(&mut self) {
        // Both processes retain fd 3 until exit; zombies hold no pipe writers.
        assert_eq!(
            tokio::time::timeout(DEADLINE, self.read_byte())
                .await
                .expect("probe or descendant still holds the witness channel")
                .unwrap(),
            None
        );
        self.group = None;
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        if let Some(group) = self.group {
            // SAFETY: the positive PID comes from this fixture's grouped shell.
            #[allow(unsafe_code)]
            let result = unsafe { libc::kill(-group, libc::SIGKILL) };
            if result != 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    eprintln!("failed to clean up health fixture group {group}: {error}");
                }
            }
        }
        if let Err(error) = std::fs::remove_file(&self.path) {
            eprintln!("failed to remove health fixture FIFO: {error}");
        }
    }
}
