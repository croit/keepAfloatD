use super::*;

#[tokio::test(start_paused = true)]
async fn counts_repeats_without_extending_the_interval() {
    let limiter = WarningLimiter::default();
    assert_eq!(limiter.record("handshake"), Some(0));
    for _ in 0..29 {
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(limiter.record("handshake"), None);
    }
    tokio::time::advance(Duration::from_millis(999)).await;
    assert_eq!(limiter.record("handshake"), None);
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(limiter.record("handshake"), Some(30));
    assert_eq!(limiter.record("handshake"), None);
    tokio::time::advance(INTERVAL).await;
    assert_eq!(limiter.record("handshake"), Some(1));
    tokio::time::advance(INTERVAL * 3).await;
    assert_eq!(limiter.record("handshake"), Some(0));
}

#[test]
fn sites_and_listeners_are_independent_but_clones_share_counts() {
    let raft = WarningLimiter::default();
    let submit = WarningLimiter::default();
    assert_eq!(raft.record("handshake"), Some(0));
    assert_eq!(raft.clone().record("handshake"), None);
    assert_eq!(raft.record("quota"), Some(0));
    assert_eq!(submit.record("handshake"), Some(0));
    assert_eq!(raft.windows.lock().unwrap().len(), 2);
    assert_eq!(submit.windows.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn concurrent_events_share_one_window_and_an_exact_count() {
    let limiter = WarningLimiter::default();
    let start = std::sync::Barrier::new(8);
    let runtime = tokio::runtime::Handle::current();
    let emitted: usize = std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for _ in 0..8 {
            threads.push(scope.spawn(|| {
                let _runtime = runtime.enter();
                start.wait();
                (0..100)
                    .filter(|_| match limiter.record("handshake") {
                        Some(count) => {
                            assert_eq!(count, 0);
                            true
                        }
                        None => false,
                    })
                    .count()
            }));
        }
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .sum()
    });
    assert_eq!(emitted, 1);
    tokio::time::advance(INTERVAL).await;
    assert_eq!(limiter.record("handshake"), Some(799));
}

#[tokio::test(start_paused = true)]
async fn suppressed_count_saturates_and_resets() {
    let limiter = WarningLimiter::default();
    limiter.record("handshake");
    limiter
        .windows
        .lock()
        .unwrap()
        .get_mut("handshake")
        .unwrap()
        .suppressed = u64::MAX;
    assert_eq!(limiter.record("handshake"), None);
    tokio::time::advance(INTERVAL).await;
    assert_eq!(limiter.record("handshake"), Some(u64::MAX));
    tokio::time::advance(INTERVAL).await;
    assert_eq!(limiter.record("handshake"), Some(0));
}

#[test]
fn poisoned_diagnostic_state_does_not_stop_request_handling() {
    let limiter = WarningLimiter::default();
    assert!(
        std::panic::catch_unwind(|| {
            let _guard = limiter.windows.lock().unwrap();
            panic!("poison diagnostic state");
        })
        .is_err()
    );
    assert_eq!(limiter.record("handshake"), Some(0));
    assert_eq!(limiter.record("handshake"), None);
}

#[tokio::test(start_paused = true)]
async fn warning_output_keeps_fields_and_reports_suppressed_events() {
    fn emit(limiter: &WarningLimiter, port: u16) {
        warn_limited!(limiter, port, "handshake rejected");
    }

    let limiter = WarningLimiter::default();
    let (output, _guard) =
        test_support::LogCapture::start("keepafloatd::warning_limit::tests=warn");
    emit(&limiter, 1);
    emit(&limiter, 2);
    emit(&limiter, 3);
    tokio::time::advance(INTERVAL).await;
    emit(&limiter, 4);
    let text = output.text();
    assert_eq!(text.lines().count(), 2, "{text}");
    assert!(text.contains("suppressed=0 port=1"), "{text}");
    assert!(text.contains("suppressed=2 port=4"), "{text}");
    assert_eq!(limiter.windows.lock().unwrap().len(), 1);
}

#[test]
fn filtered_warnings_do_not_consume_a_window() {
    let limiter = WarningLimiter::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::ERROR)
        .with_writer(std::io::sink)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        warn_limited!(limiter, "filtered warning");
    });
    assert!(limiter.windows.lock().unwrap().is_empty());
}
