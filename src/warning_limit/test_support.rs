use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone, Default)]
pub(crate) struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl LogCapture {
    pub(crate) fn start(filter: &str) -> (Self, tracing::subscriber::DefaultGuard) {
        // Keep callsite registration from using another thread's default as its only subscriber.
        static SILENT_DISPATCH: OnceLock<tracing::Dispatch> = OnceLock::new();
        SILENT_DISPATCH
            .get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
        let output = Self::default();
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_env_filter(filter)
            .with_writer(move || writer.clone())
            .finish();
        (output, tracing::subscriber::set_default(subscriber))
    }

    pub(crate) fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

#[tokio::test]
async fn capture_observes_callsites_first_used_without_a_thread_subscriber() {
    const NAME: &str = concat!(
        module_path!(),
        "::capture_observes_callsites_first_used_without_a_thread_subscriber"
    );
    let test_name = NAME.strip_prefix("keepafloatd::").unwrap();
    if std::env::var("KEEPAFLOATD_CAPTURE_TEST").as_deref() != Ok(test_name) {
        // A fresh process prevents other tests' subscribers from hiding the registration race.
        let child = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", test_name, "--nocapture", "--color=never"])
                .env("KEEPAFLOATD_CAPTURE_TEST", test_name)
                .stdin(std::process::Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("isolated capture regression timed out")
        .unwrap();
        let stdout = String::from_utf8_lossy(&child.stdout);
        let stderr = String::from_utf8_lossy(&child.stderr);
        assert!(child.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.contains(&format!("test {test_name} ... ok")),
            "{stdout}"
        );
        return;
    }

    fn emit(marker: &str) {
        tracing::warn!(marker, "capture regression");
    }

    for _ in 0..2 {
        let (output, _guard) = LogCapture::start("warn");
        std::thread::spawn(|| emit("other-thread")).join().unwrap();
        emit("captured-thread");
        {
            let (filtered, _guard) = LogCapture::start("error");
            emit("filtered-warning");
            assert!(filtered.text().is_empty());
        }
        emit("restored-thread");
        let text = output.text();
        assert_eq!(text.lines().count(), 2, "{text}");
        assert!(text.contains("captured-thread"), "{text}");
        assert!(text.contains("restored-thread"), "{text}");
        assert!(!text.contains("other-thread"), "{text}");
        assert!(!text.contains("filtered-warning"), "{text}");
    }
}
