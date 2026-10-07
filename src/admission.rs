//! Separate source-address budgets for unauthenticated and authenticated connections.
//!
//! Only configured peer addresses hold a budget. Listeners reject unknown sources before asking
//! for a permit, and this module refuses them as well so an unknown source can never consume
//! capacity even if a caller forgets that check.

use crate::connection_admission::{
    ConnectionAdmission as PhaseAdmission, UnauthenticatedConnection,
};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

/// Per-phase allowance for persistent RPCs and concurrent short-lived status/submit requests.
pub(crate) const CONNECTIONS_PER_PEER: usize = 8;

/// Source IPs reserve capacity only; every admitted connection must still authenticate.
pub(crate) struct ConnectionAdmission {
    peers: HashMap<IpAddr, PhaseAdmission>,
}

impl ConnectionAdmission {
    pub(crate) fn new(addresses: impl IntoIterator<Item = SocketAddr>) -> Self {
        let mut limits = HashMap::<_, usize>::new();
        for address in addresses {
            *limits.entry(address.ip().to_canonical()).or_default() += CONNECTIONS_PER_PEER;
        }
        let peers = limits
            .into_iter()
            .map(|(ip, limit)| (ip, PhaseAdmission::new(limit, limit)))
            .collect();
        Self { peers }
    }

    /// Never queues a task; returns `None` for an unknown address or an exhausted budget.
    pub(crate) fn try_begin(&self, ip: IpAddr) -> Option<UnauthenticatedConnection> {
        self.peers.get(&ip.to_canonical())?.try_begin()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peers_have_independent_budgets_and_unknown_sources_are_refused() {
        let admission = ConnectionAdmission::new([
            "192.0.2.1:123".parse().unwrap(),
            "192.0.2.2:123".parse().unwrap(),
        ]);
        let a = "192.0.2.1".parse().unwrap();
        let b = "192.0.2.2".parse().unwrap();
        let unknown: IpAddr = "192.0.2.99".parse().unwrap();
        assert!(
            admission.try_begin(unknown).is_none(),
            "an unknown source must never receive a permit"
        );
        let mut held = Vec::new();
        for _ in 0..CONNECTIONS_PER_PEER {
            held.push(admission.try_begin(a).unwrap());
            held.push(admission.try_begin(b).unwrap());
        }
        for ip in [a, b, unknown] {
            assert!(admission.try_begin(ip).is_none());
        }
        assert_eq!(admission.peers.len(), 2);
        drop(held);
        assert!(admission.try_begin(a).is_some());
        assert!(admission.try_begin(b).is_some());
        assert!(admission.try_begin(unknown).is_none());
    }

    #[test]
    fn shared_and_ipv4_mapped_addresses_use_the_combined_peer_budget() {
        let admission = ConnectionAdmission::new([
            "192.0.2.1:123".parse().unwrap(),
            "[::ffff:192.0.2.1]:456".parse().unwrap(),
        ]);
        let held: Vec<_> = (0..2 * CONNECTIONS_PER_PEER)
            .map(|_| admission.try_begin("192.0.2.1".parse().unwrap()).unwrap())
            .collect();
        assert!(
            admission
                .try_begin("::ffff:192.0.2.1".parse().unwrap())
                .is_none()
        );
        assert_eq!(admission.peers.len(), 1);
        let authenticated: Vec<_> = held
            .into_iter()
            .map(|permit| permit.try_authenticate().ok().unwrap())
            .collect();
        assert!(
            admission
                .try_begin("::ffff:192.0.2.1".parse().unwrap())
                .is_some()
        );
        assert!(
            admission
                .try_begin("192.0.2.1".parse().unwrap())
                .unwrap()
                .try_authenticate()
                .is_err()
        );
        drop(authenticated);
        assert!(
            admission
                .try_begin("::ffff:192.0.2.1".parse().unwrap())
                .unwrap()
                .try_authenticate()
                .is_ok()
        );
    }

    #[test]
    fn phases_are_independently_bounded_and_failed_transitions_release_capacity() {
        let admission = ConnectionAdmission::new([
            "192.0.2.1:123".parse().unwrap(),
            "192.0.2.2:123".parse().unwrap(),
        ]);
        let ip = "192.0.2.1".parse().unwrap();
        let mut authenticated: Vec<_> = (0..CONNECTIONS_PER_PEER)
            .map(|_| {
                admission
                    .try_begin(ip)
                    .unwrap()
                    .try_authenticate()
                    .ok()
                    .unwrap()
            })
            .collect();
        let mut pending: Vec<_> = (0..CONNECTIONS_PER_PEER)
            .map(|_| admission.try_begin(ip).unwrap())
            .collect();
        assert!(admission.try_begin(ip).is_none());
        assert!(pending.pop().unwrap().try_authenticate().is_err());
        let recovered = admission.try_begin(ip).unwrap();
        assert!(admission.try_begin(ip).is_none());
        assert!(
            admission
                .try_begin("192.0.2.2".parse().unwrap())
                .unwrap()
                .try_authenticate()
                .is_ok()
        );
        authenticated.pop();
        assert!(recovered.try_authenticate().is_ok());
        assert!(admission.try_begin(ip).is_some());
    }

    #[tokio::test]
    async fn cancellation_releases_each_phase() {
        let admission = ConnectionAdmission::new(["192.0.2.1:123".parse().unwrap()]);
        let ip = "192.0.2.1".parse().unwrap();
        for authenticate in [false, true] {
            let permits: Vec<_> = (0..CONNECTIONS_PER_PEER)
                .map(|_| admission.try_begin(ip).unwrap())
                .collect();
            let (ready, started) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                if authenticate {
                    let _held: Vec<_> = permits
                        .into_iter()
                        .map(|permit| permit.try_authenticate().ok().unwrap())
                        .collect();
                    ready.send(()).unwrap();
                    std::future::pending::<()>().await;
                } else {
                    let _held = permits;
                    ready.send(()).unwrap();
                    std::future::pending::<()>().await;
                }
            });
            started.await.unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            let _recovered: Vec<_> = (0..CONNECTIONS_PER_PEER)
                .map(|_| {
                    admission
                        .try_begin(ip)
                        .unwrap()
                        .try_authenticate()
                        .ok()
                        .unwrap()
                })
                .collect();
        }
    }
}
