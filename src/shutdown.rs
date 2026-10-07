//! Register signals before startup effects and defer restart until cleanup completes.

use anyhow::Context;

#[cfg(unix)]
pub struct Signals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    hangup: tokio::signal::unix::Signal,
    quit: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl Signals {
    pub fn install() -> anyhow::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt()).context("listen for SIGINT")?,
            terminate: signal(SignalKind::terminate()).context("listen for SIGTERM")?,
            hangup: signal(SignalKind::hangup()).context("listen for SIGHUP")?,
            quit: signal(SignalKind::quit()).context("listen for SIGQUIT")?,
        })
    }

    pub async fn wait(mut self) -> anyhow::Result<()> {
        let (name, received) = tokio::select! {
            received = self.interrupt.recv() => ("SIGINT", received),
            received = self.terminate.recv() => ("SIGTERM", received),
            received = self.hangup.recv() => ("SIGHUP", received),
            received = self.quit.recv() => ("SIGQUIT", received),
        };
        received.context("shutdown signal stream closed")?;
        tracing::info!("shutting down on {}", name);
        if matches!(name, "SIGHUP" | "SIGQUIT") {
            anyhow::bail!("{name} requested daemon restart");
        }
        Ok(())
    }
}

#[cfg(not(unix))]
pub struct Signals;

#[cfg(not(unix))]
impl Signals {
    pub fn install() -> anyhow::Result<Self> {
        Ok(Self)
    }

    pub async fn wait(self) -> anyhow::Result<()> {
        tokio::signal::ctrl_c().await.context("wait for ctrl_c")?;
        tracing::info!("shutting down on SIGINT");
        Ok(())
    }
}
