//! Cancellation-safe accept retry for the Raft and submit listeners.

use crate::warning_limit::{WarningLimiter, warn_limited};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Instant, sleep_until};

const ACCEPT_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Keeps a test endpoint reserved until its server takes ownership.
#[derive(Default)]
pub(crate) enum ListenerSource {
    #[default]
    Configured,
    #[cfg(test)]
    Bound(TcpListener),
}

impl ListenerSource {
    pub(crate) async fn bind(self, address: SocketAddr) -> io::Result<TcpListener> {
        match self {
            Self::Configured => TcpListener::bind(address).await,
            #[cfg(test)]
            Self::Bound(listener) => {
                if listener.local_addr()? != address {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "bound listener does not match configured address",
                    ));
                }
                Ok(listener)
            }
        }
    }
}

/// Accept must be cancellation-safe when raced with connection tasks.
pub(crate) trait ConnectionListener: Send + Sync + 'static {
    fn accept(&self) -> impl Future<Output = io::Result<(TcpStream, SocketAddr)>> + Send;
}

impl ConnectionListener for TcpListener {
    async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        TcpListener::accept(self).await
    }
}

/// Retains the retry deadline when another branch wins a server's select.
#[derive(Default)]
pub(crate) struct AcceptBackoff {
    retry_at: Option<Instant>,
    warnings: WarningLimiter,
}

impl AcceptBackoff {
    pub(crate) async fn accept(
        &mut self,
        listener: &impl ConnectionListener,
    ) -> io::Result<(TcpStream, SocketAddr)> {
        if let Some(retry_at) = self.retry_at {
            sleep_until(retry_at).await;
            self.retry_at = None;
        }
        listener.accept().await
    }

    pub(crate) fn failed(&mut self, listener: &str, error: io::Error) {
        self.retry_at = Some(Instant::now() + ACCEPT_RETRY_DELAY);
        warn_limited!(
            self.warnings,
            listener,
            %error,
            retry_ms = ACCEPT_RETRY_DELAY.as_millis(),
            "accept failed; retrying"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use test_support::FaultyListener;

    #[tokio::test]
    async fn bound_listener_retains_ownership_and_accepts_queued_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let source = ListenerSource::Bound(listener);
        assert_eq!(
            TcpListener::bind(address).await.unwrap_err().kind(),
            io::ErrorKind::AddrInUse,
        );
        let peer = TcpStream::connect(address).await.unwrap();
        let listener = source.bind(address).await.unwrap();
        assert_eq!(listener.local_addr().unwrap(), address);
        let (accepted, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(accepted.peer_addr().unwrap(), peer.local_addr().unwrap());
        assert_eq!(
            TcpListener::bind(address).await.unwrap_err().kind(),
            io::ErrorKind::AddrInUse,
        );
    }

    #[tokio::test]
    async fn mismatched_listener_is_rejected_and_released() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let configured: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let error = ListenerSource::Bound(listener)
            .bind(configured)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        TcpListener::bind(address).await.unwrap();
    }

    #[tokio::test]
    async fn configured_listener_preserves_bind_errors() {
        let listener = ListenerSource::Configured
            .bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let error = ListenerSource::Configured
            .bind(listener.local_addr().unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    }

    #[tokio::test(start_paused = true)]
    async fn accept_warning_counts_do_not_change_retry_deadlines() {
        let (output, _guard) =
            crate::warning_limit::test_support::LogCapture::start("keepafloatd::listener=warn");
        let mut raft = AcceptBackoff::default();
        for _ in 0..3 {
            raft.failed("Raft", io::ErrorKind::ConnectionAborted.into());
            assert_eq!(raft.retry_at, Some(Instant::now() + ACCEPT_RETRY_DELAY));
            tokio::time::advance(ACCEPT_RETRY_DELAY).await;
        }
        assert_eq!(output.text().lines().count(), 1);
        let mut submit = AcceptBackoff::default();
        submit.failed("submit", io::ErrorKind::Interrupted.into());
        tokio::time::advance(Duration::from_secs(27)).await;
        raft.failed("Raft", io::ErrorKind::ConnectionAborted.into());
        assert_eq!(raft.retry_at, Some(Instant::now() + ACCEPT_RETRY_DELAY));
        let text = output.text();
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 3, "{text}");
        assert!(
            lines[0].contains("suppressed=0 listener=\"Raft\""),
            "{text}"
        );
        assert!(
            lines[1].contains("suppressed=0 listener=\"submit\""),
            "{text}"
        );
        assert!(
            lines[2].contains("suppressed=2 listener=\"Raft\""),
            "{text}"
        );
        assert!(
            lines.iter().all(|line| line.contains("retry_ms=1000")),
            "{text}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_errors_wait_before_each_attempt() {
        let faults = Arc::new(AtomicUsize::new(3));
        let listener = FaultyListener {
            inner: TcpListener::bind("127.0.0.1:0").await.unwrap(),
            faults: faults.clone(),
        };
        let mut backoff = AcceptBackoff::default();
        for remaining in (0..3).rev() {
            let mut accept = Box::pin(backoff.accept(&listener));
            if remaining != 2 {
                assert!(futures::poll!(&mut accept).is_pending());
                tokio::time::advance(Duration::from_millis(999)).await;
                assert!(futures::poll!(&mut accept).is_pending());
                assert_eq!(faults.load(Ordering::SeqCst), remaining + 1);
                tokio::time::advance(Duration::from_millis(1)).await;
            }
            let error = accept.await.unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EMFILE));
            assert_eq!(faults.load(Ordering::SeqCst), remaining);
            backoff.failed("test", error);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_retry_preserves_deadline_without_extending_it() {
        let faults = Arc::new(AtomicUsize::new(1));
        let listener = FaultyListener {
            inner: TcpListener::bind("127.0.0.1:0").await.unwrap(),
            faults: faults.clone(),
        };
        let mut backoff = AcceptBackoff::default();
        backoff.failed("test", io::ErrorKind::ConnectionAborted.into());
        for _ in 0..10 {
            let mut accept = Box::pin(backoff.accept(&listener));
            assert!(futures::poll!(&mut accept).is_pending());
            assert_eq!(faults.load(Ordering::SeqCst), 1);
            drop(accept);
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        assert_eq!(
            backoff.accept(&listener).await.unwrap_err().raw_os_error(),
            Some(libc::EMFILE)
        );
        assert!(backoff.retry_at.is_none());
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub(crate) struct FaultyListener {
        pub(crate) inner: TcpListener,
        pub(crate) faults: Arc<AtomicUsize>,
    }

    impl ConnectionListener for FaultyListener {
        async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
            if self
                .faults
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                    count.checked_sub(1)
                })
                .is_ok()
            {
                return Err(io::Error::from_raw_os_error(libc::EMFILE));
            }
            self.inner.accept().await
        }
    }
}
