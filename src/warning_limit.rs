//! Listener-scoped limits for repeatable network warnings.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

const INTERVAL: Duration = Duration::from_secs(30);

#[derive(Default)]
struct Window {
    last_emitted: Option<Instant>,
    suppressed: u64,
}

/// Share across accepted connections; keys are static warning sites, never remote input.
#[derive(Clone, Default)]
pub(crate) struct WarningLimiter {
    windows: Arc<Mutex<BTreeMap<&'static str, Window>>>,
}

impl WarningLimiter {
    pub(crate) fn record(&self, site: &'static str) -> Option<u64> {
        // Diagnostic state must not stop connection handling after a poisoned lock.
        let mut windows = self
            .windows
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let window = windows.entry(site).or_default();
        let now = Instant::now();
        if window
            .last_emitted
            .is_none_or(|last| now.duration_since(last) >= INTERVAL)
        {
            window.last_emitted = Some(now);
            Some(std::mem::take(&mut window.suppressed))
        } else {
            window.suppressed = window.suppressed.saturating_add(1);
            None
        }
    }
}

macro_rules! warn_limited {
    ($limiter:expr, $($fields:tt)*) => {
        if tracing::enabled!(tracing::Level::WARN)
            && let Some(suppressed) = $limiter.record(concat!(file!(), ":", line!(), ":", column!()))
        {
            tracing::warn!(suppressed, $($fields)*);
        }
    };
}

pub(crate) use warn_limited;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_support;
