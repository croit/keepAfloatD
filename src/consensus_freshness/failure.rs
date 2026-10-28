//! Internal failure classes; absence of transport evidence fails closed.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureKind {
    Unavailable,
    Rejected,
}

#[derive(Debug)]
struct Rejected;

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("observed protocol rejection")
    }
}

impl std::error::Error for Rejected {}

pub(crate) fn rejected(error: anyhow::Error) -> anyhow::Error {
    error.context(Rejected)
}

pub(crate) fn unavailable(message: &'static str) -> anyhow::Error {
    std::io::Error::new(std::io::ErrorKind::NotConnected, message).into()
}

pub(crate) fn failure_kind(error: &anyhow::Error) -> FailureKind {
    if error.downcast_ref::<Rejected>().is_some()
        || error
            .downcast_ref::<crate::raft::admission::AdmissionDenied>()
            .is_some()
    {
        return FailureKind::Rejected;
    }
    if error
        .downcast_ref::<crate::connection_admission::ConnectFailed>()
        .is_some()
        || error
            .downcast_ref::<crate::raft::network::DestinationBootChanged>()
            .is_some()
    {
        return FailureKind::Unavailable;
    }
    match error.downcast_ref::<std::io::Error>() {
        Some(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::HostUnreachable
                    | std::io::ErrorKind::NetworkUnreachable
                    | std::io::ErrorKind::NetworkDown
            ) =>
        {
            FailureKind::Unavailable
        }
        _ if error
            .downcast_ref::<tokio::time::error::Elapsed>()
            .is_some() =>
        {
            FailureKind::Unavailable
        }
        _ => FailureKind::Rejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_failure_is_unavailable_without_weakening_protocol_rejection() {
        let connect_error = || {
            anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::InvalidInput))
                .context(crate::connection_admission::ConnectFailed)
        };
        assert_eq!(failure_kind(&connect_error()), FailureKind::Unavailable);
        assert_eq!(
            failure_kind(&rejected(connect_error())),
            FailureKind::Rejected
        );
        assert_eq!(
            failure_kind(
                &connect_error()
                    .context(crate::raft::admission::AdmissionDenied("invalid context"))
            ),
            FailureKind::Rejected
        );
        assert_eq!(
            failure_kind(&std::io::Error::from(std::io::ErrorKind::InvalidInput).into()),
            FailureKind::Rejected
        );
    }

    #[test]
    fn known_transport_failures_are_unavailable_but_invalid_data_is_rejected() {
        for kind in [
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::TimedOut,
        ] {
            let error = anyhow::Error::new(std::io::Error::new(kind, "transport"))
                .context("publication could not complete");
            assert_eq!(failure_kind(&error), FailureKind::Unavailable);
        }
        for kind in [std::io::ErrorKind::InvalidData, std::io::ErrorKind::Other] {
            assert_eq!(
                failure_kind(&std::io::Error::new(kind, "invalid").into()),
                FailureKind::Rejected
            );
        }
        assert_eq!(
            failure_kind(&anyhow::anyhow!("unknown failure")),
            FailureKind::Rejected
        );
    }

    #[test]
    fn unreachable_network_does_not_revoke_an_existing_health_proof() {
        for kind in [
            std::io::ErrorKind::HostUnreachable,
            std::io::ErrorKind::NetworkUnreachable,
            std::io::ErrorKind::NetworkDown,
        ] {
            let error = anyhow::Error::new(std::io::Error::from(kind))
                .context("connecting to a cluster peer");
            assert_eq!(failure_kind(&error), FailureKind::Unavailable, "{kind:?}");
            assert_eq!(failure_kind(&rejected(error)), FailureKind::Rejected);
        }
    }

    #[test]
    fn observed_rejection_takes_precedence_over_a_transport_cause() {
        let error = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        assert_eq!(
            failure_kind(&rejected(error).context("decode")),
            FailureKind::Rejected
        );
        let error = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))
            .context(crate::raft::admission::AdmissionDenied("invalid context"));
        assert_eq!(failure_kind(&error), FailureKind::Rejected);
        let hidden = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
        );
        assert_eq!(failure_kind(&hidden.into()), FailureKind::Rejected);
    }

    #[tokio::test(start_paused = true)]
    async fn elapsed_remains_unavailable_through_context() {
        let error = tokio::time::timeout(std::time::Duration::ZERO, std::future::pending::<()>())
            .await
            .unwrap_err();
        assert_eq!(
            failure_kind(&anyhow::Error::new(error).context("phase timed out")),
            FailureKind::Unavailable
        );
    }
}
