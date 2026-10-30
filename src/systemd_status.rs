//! The systemd status text: whether this member still waits to vote in its cluster again (#47).
//!
//! Supervisors read it with `systemctl show -p StatusText`, for example to hold a rolling reboot
//! until a restarted member takes part in its cluster again.

use std::ffi::OsStr;
use std::io;

pub(crate) const AWAITING_ADMISSION: &str = "awaiting admission";
pub(crate) const ADMITTED: &str = "admitted";

/// Report `status` to the service manager, answering whether it was taken; a daemon without one
/// has nobody to tell.
pub(crate) fn report(status: &'static str) -> bool {
    report_to(std::env::var_os("NOTIFY_SOCKET").as_deref(), status)
}

fn report_to(path: Option<&OsStr>, status: &'static str) -> bool {
    match notify(path, status) {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(%error, status, "could not report the systemd status text");
            false
        }
    }
}

fn notify(path: Option<&OsStr>, status: &str) -> io::Result<()> {
    let Some(path) = path.filter(|path| !path.is_empty()) else {
        return Ok(());
    };
    let address = crate::stop_budget::socket_address(path)?;
    crate::stop_budget::send(&address, format!("STATUS={status}").as_bytes())?;
    tracing::debug!(status, "systemd status text sent");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixDatagram;

    #[test]
    fn no_supervisor_needs_no_status() {
        notify(None, ADMITTED).unwrap();
        notify(Some(OsStr::new("")), ADMITTED).unwrap();
    }

    #[test]
    fn the_status_reaches_the_notify_socket() {
        let dir = std::env::temp_dir().join(format!("keepafloatd-status-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("notify");
        let _ = std::fs::remove_file(&path);
        let socket = UnixDatagram::bind(&path).unwrap();
        for status in [AWAITING_ADMISSION, ADMITTED] {
            notify(Some(path.as_os_str()), status).unwrap();
            let mut bytes = [0; 64];
            let size = socket.recv(&mut bytes).unwrap();
            assert_eq!(&bytes[..size], format!("STATUS={status}").as_bytes());
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_report_says_whether_the_supervisor_took_it() {
        assert!(report_to(None, ADMITTED));
        assert!(!report_to(Some(OsStr::new("notify")), ADMITTED));
    }

    #[test]
    fn a_relative_notify_socket_is_refused() {
        assert_eq!(
            notify(Some(OsStr::new("notify")), ADMITTED)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
