use super::*;
use std::sync::{Arc, Mutex};
use tracing::instrument::WithSubscriber;

#[derive(Clone, Default)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl io::Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn vip(ip: &str, iface: &str) -> (VipAddr, String) {
    (
        VipAddr {
            addr: ip.parse().unwrap(),
            prefix: if ip.contains(':') { 64 } else { 24 },
        },
        iface.into(),
    )
}

async fn capture(future: impl Future<Output = ()>) -> String {
    let buffer = LogBuffer::default();
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    future.with_subscriber(subscriber).await;
    String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap()
}

#[tokio::test]
async fn disabled_promotion_warns_once_per_ipv4_interface_including_vlans() {
    let vips = [
        vip("192.0.2.30", "test0.100"),
        vip("192.0.2.31", "test0.100"),
        vip("192.0.2.32", "test1"),
        vip("2001:db8::30", "ipv6-only"),
    ];
    let mut reads = Vec::new();
    let logs = capture(check_with(&vips, false, |iface| {
        reads.push(iface);
        std::future::ready(Ok(false))
    }))
    .await;
    assert_eq!(reads, ["all", "test0.100", "test1"]);
    assert_eq!(logs.matches("promotion is disabled").count(), 2, "{logs}");
    assert!(logs.contains("iface=test0.100"), "{logs}");
    assert!(logs.contains("iface=test1"), "{logs}");
    assert!(logs.contains("same subnet"), "{logs}");
    assert!(logs.contains("enable promote_secondaries"), "{logs}");
}

#[tokio::test]
async fn either_enabled_setting_suppresses_the_warning() {
    let vips = [vip("192.0.2.30", "test0")];
    for global in [true, false] {
        let mut reads = Vec::new();
        let logs = capture(check_with(&vips, false, |iface| {
            reads.push(iface.clone());
            std::future::ready(Ok(if iface == "all" { global } else { true }))
        }))
        .await;
        assert!(logs.is_empty(), "{logs}");
        assert_eq!(reads.len(), if global { 1 } else { 2 });
    }
}

#[tokio::test]
async fn dry_run_empty_and_ipv6_only_skip_all_reads() {
    let ipv4 = [vip("192.0.2.30", "test0")];
    let ipv6 = [vip("2001:db8::30", "test0")];
    for (vips, dry_run) in [(&ipv4[..], true), (&ipv6[..], false), (&[][..], false)] {
        let logs = capture(check_with(
            vips,
            dry_run,
            |_| -> std::future::Ready<io::Result<bool>> { panic!("irrelevant sysctl read") },
        ))
        .await;
        assert!(logs.is_empty());
    }
}

#[tokio::test]
async fn read_errors_warn_unless_the_other_setting_proves_promotion() {
    let vips = [vip("192.0.2.30", "test0")];
    for (global, local, warns) in [
        (Some(false), None, true),
        (None, Some(false), true),
        (None, None, true),
        (None, Some(true), false),
        (Some(true), None, false),
    ] {
        let logs = capture(check_with(&vips, false, |iface| {
            std::future::ready(
                (if iface == "all" { global } else { local })
                    .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "not readable")),
            )
        }))
        .await;
        assert_eq!(logs.contains("could not verify"), warns, "{logs}");
        assert!(!logs.contains("promotion is disabled"), "{logs}");
        if warns {
            assert!(logs.contains("not readable"), "{logs}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn stuck_reads_are_bounded_and_reported() {
    let vips = [vip("192.0.2.30", "test0")];
    let start = tokio::time::Instant::now();
    let logs = capture(check_with(&vips, false, |_| std::future::pending())).await;
    assert_eq!(
        start.elapsed(),
        super::super::effects::IP_COMMAND_TIMEOUT * 2
    );
    assert!(logs.contains("could not verify"), "{logs}");
    assert!(logs.contains("sysctl read timed out"), "{logs}");
}

#[tokio::test]
async fn file_reader_handles_boolean_values_invalid_data_and_missing_files() {
    let root = std::env::temp_dir().join(format!("keepafloatd-secondary-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let interface = root.join("test0.100");
    std::fs::create_dir(&interface).unwrap();
    let path = interface.join("promote_secondaries");
    for (bytes, expected) in [(b"0\n".as_slice(), false), (b"1\n".as_slice(), true)] {
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(
            read_enabled(&root, "test0.100".into()).await.unwrap(),
            expected
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
    for bytes in [b"".as_slice(), b"2", b"garbage", b"\xff", &[b' '; 33]] {
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(
            read_enabled(&root, "test0.100".into())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
    assert_eq!(
        read_enabled(&root, "missing0".into())
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    std::fs::remove_file(path).unwrap();
    std::fs::remove_dir(interface).unwrap();
    std::fs::remove_dir(root).unwrap();
}
