//! Progress-based systemd stop leases, never an independent keepalive task.

use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::{SocketAddr, UnixDatagram};
use std::time::Duration;

/// Keep the packaged no-progress guard in addition to the next operation's bounded work.
const HANG_GUARD: Duration = Duration::from_secs(15);

/// Call once when teardown starts, including a signal-driven restart or fatal task exit.
pub(crate) fn stopping() {
    report(true, Duration::ZERO);
}

/// Call only before the next finite work item, never from a timer or unbounded retry loop.
pub(crate) fn checkpoint(work: Duration) {
    report(false, work);
}

fn report(stopping: bool, work: Duration) {
    if let Err(error) = notify(std::env::var_os("NOTIFY_SOCKET").as_deref(), stopping, work) {
        tracing::warn!(%error, "could not extend systemd stop budget; cleanup continues with the existing deadline");
    }
}

fn message(stopping: bool, work: Duration) -> io::Result<String> {
    let micros = work.saturating_add(HANG_GUARD).as_micros();
    // Leave timestamp headroom so adding the manager's monotonic clock cannot mean infinity.
    let micros = i64::try_from(micros)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "stop budget is too large"))?;
    let state = if stopping { "STOPPING=1\n" } else { "" };
    Ok(format!("{state}EXTEND_TIMEOUT_USEC={micros}"))
}

fn notify(path: Option<&OsStr>, stopping: bool, work: Duration) -> io::Result<()> {
    notify_with(path, stopping, work, |address, message| {
        let socket = UnixDatagram::unbound()?;
        socket.set_nonblocking(true)?;
        socket.send_to_addr(message, address)?;
        Ok(())
    })
}

fn notify_with(
    path: Option<&OsStr>,
    stopping: bool,
    work: Duration,
    send: impl FnOnce(&SocketAddr, &[u8]) -> io::Result<()>,
) -> io::Result<()> {
    let Some(path) = path.filter(|path| !path.is_empty()) else {
        return Ok(());
    };
    let address = socket_address(path)?;
    let message = message(stopping, work)?;
    send(&address, message.as_bytes())?;
    tracing::debug!(
        stopping,
        work_ms = work.as_millis(),
        "systemd stop budget checkpoint sent"
    );
    Ok(())
}

fn socket_address(path: &OsStr) -> io::Result<SocketAddr> {
    #[cfg(target_os = "linux")]
    if let Some(name) = path.as_bytes().strip_prefix(b"@") {
        use std::os::linux::net::SocketAddrExt;
        return SocketAddr::from_abstract_name(name);
    }
    if path.as_bytes().starts_with(b"/") {
        return SocketAddr::from_pathname(path);
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "NOTIFY_SOCKET must be an absolute path or an abstract socket",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT: AtomicUsize = AtomicUsize::new(0);

    struct SocketFixture {
        dir: std::path::PathBuf,
        path: std::path::PathBuf,
        socket: UnixDatagram,
    }

    impl SocketFixture {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "keepafloatd-stop-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&dir).unwrap();
            let path = dir.join("notify");
            let socket = UnixDatagram::bind(&path).unwrap();
            socket.set_nonblocking(true).unwrap();
            Self { dir, path, socket }
        }

        fn receive(&self) -> String {
            let mut bytes = [0; 256];
            let size = self.socket.recv(&mut bytes).unwrap();
            String::from_utf8(bytes[..size].to_vec()).unwrap()
        }
    }

    impl Drop for SocketFixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }

    #[test]
    fn no_supervisor_needs_no_notification() {
        notify(None, true, Duration::ZERO).unwrap();
        notify(Some(OsStr::new("")), false, Duration::MAX).unwrap();
    }

    #[test]
    fn leases_include_work_and_keep_a_finite_guard() {
        assert_eq!(
            message(true, Duration::ZERO).unwrap(),
            "STOPPING=1\nEXTEND_TIMEOUT_USEC=15000000"
        );
        assert_eq!(
            message(false, Duration::from_millis(1250)).unwrap(),
            "EXTEND_TIMEOUT_USEC=16250000"
        );
        assert_eq!(
            message(false, Duration::from_secs(60)).unwrap(),
            "EXTEND_TIMEOUT_USEC=75000000"
        );
        assert_eq!(
            message(false, Duration::MAX).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn pathname_socket_receives_stopping_and_progress() {
        let fixture = SocketFixture::new();
        notify(Some(fixture.path.as_os_str()), true, Duration::ZERO).unwrap();
        assert_eq!(
            fixture.receive(),
            "STOPPING=1\nEXTEND_TIMEOUT_USEC=15000000"
        );
        notify(
            Some(fixture.path.as_os_str()),
            false,
            Duration::from_secs(12),
        )
        .unwrap();
        assert_eq!(fixture.receive(), "EXTEND_TIMEOUT_USEC=27000000");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn abstract_socket_receives_the_lease() {
        let name = format!(
            "@keepafloatd-stop-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let address = socket_address(OsStr::new(&name)).unwrap();
        let receiver = UnixDatagram::bind_addr(&address).unwrap();
        receiver.set_nonblocking(true).unwrap();
        notify(Some(OsStr::new(&name)), false, Duration::ZERO).unwrap();
        let mut bytes = [0; 256];
        let size = receiver.recv(&mut bytes).unwrap();
        assert_eq!(&bytes[..size], b"EXTEND_TIMEOUT_USEC=15000000");
    }

    #[test]
    fn invalid_and_unavailable_sockets_return_errors() {
        assert_eq!(
            notify(Some(OsStr::new("relative")), false, Duration::ZERO)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        let fixture = SocketFixture::new();
        let missing = fixture.dir.join("missing");
        assert_eq!(
            notify(Some(missing.as_os_str()), false, Duration::ZERO)
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
    }

    #[test]
    fn full_supervisor_queue_never_blocks_cleanup() {
        let mut sends = 0;
        let error = notify_with(
            Some(OsStr::new("/supervisor-notify")),
            false,
            Duration::ZERO,
            |_, _| {
                sends += 1;
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            },
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(
            sends, 1,
            "a full supervisor queue must not cause a wait or retry"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn finite_progress_outlives_the_initial_guard_but_a_stall_gets_no_renewal() {
        let fixture = SocketFixture::new();
        let start = tokio::time::Instant::now();
        let mut deadline = start + HANG_GUARD;
        for _ in 0..20 {
            assert!(tokio::time::Instant::now() < deadline);
            notify(
                Some(fixture.path.as_os_str()),
                false,
                Duration::from_secs(12),
            )
            .unwrap();
            let message = fixture.receive();
            let micros = message
                .strip_prefix("EXTEND_TIMEOUT_USEC=")
                .unwrap()
                .parse()
                .unwrap();
            deadline = tokio::time::Instant::now() + Duration::from_micros(micros);
            tokio::time::sleep(Duration::from_secs(12)).await;
        }
        assert!(tokio::time::Instant::now() > start + HANG_GUARD);
        tokio::time::sleep_until(deadline).await;
        assert!(
            fixture.socket.recv(&mut [0; 256]).is_err(),
            "a stalled operation must not renew itself"
        );
    }
}
