//! Fixed source-address budgets shared by the unauthenticated and authenticated connection phases.
//!
//! Only configured peer addresses hold a budget. Listeners reject unknown sources before asking
//! for a permit, and this module refuses them as well so an unknown source can never consume
//! capacity even if a caller forgets that check.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Allows persistent peer RPCs and concurrent short-lived status/submit requests.
pub(crate) const CONNECTIONS_PER_PEER: usize = 8;

/// Source IPs reserve capacity only; every admitted connection must still authenticate.
pub(crate) struct ConnectionAdmission {
    peers: HashMap<IpAddr, Arc<Semaphore>>,
}

impl ConnectionAdmission {
    pub(crate) fn new(addresses: impl IntoIterator<Item = SocketAddr>) -> Self {
        let mut peers = HashMap::<_, Arc<Semaphore>>::new();
        for address in addresses {
            peers
                .entry(address.ip().to_canonical())
                .or_insert_with(|| Arc::new(Semaphore::new(0)))
                .add_permits(CONNECTIONS_PER_PEER);
        }
        Self { peers }
    }

    /// Never queues a task; returns `None` for an unknown address or an exhausted budget.
    pub(crate) fn try_acquire(&self, ip: IpAddr) -> Option<OwnedSemaphorePermit> {
        self.peers
            .get(&ip.to_canonical())?
            .clone()
            .try_acquire_owned()
            .ok()
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
            admission.try_acquire(unknown).is_none(),
            "an unknown source must never receive a permit"
        );
        let mut held = Vec::new();
        for _ in 0..CONNECTIONS_PER_PEER {
            held.push(admission.try_acquire(a).unwrap());
            held.push(admission.try_acquire(b).unwrap());
        }
        for ip in [a, b, unknown] {
            assert!(admission.try_acquire(ip).is_none());
        }
        assert_eq!(admission.peers.len(), 2);
        drop(held);
        assert!(admission.try_acquire(a).is_some());
        assert!(admission.try_acquire(b).is_some());
        assert!(admission.try_acquire(unknown).is_none());
    }

    #[test]
    fn shared_and_ipv4_mapped_addresses_use_the_combined_peer_budget() {
        let admission = ConnectionAdmission::new([
            "192.0.2.1:123".parse().unwrap(),
            "[::ffff:192.0.2.1]:456".parse().unwrap(),
        ]);
        let held: Vec<_> = (0..2 * CONNECTIONS_PER_PEER)
            .map(|_| admission.try_acquire("192.0.2.1".parse().unwrap()).unwrap())
            .collect();
        assert!(
            admission
                .try_acquire("::ffff:192.0.2.1".parse().unwrap())
                .is_none()
        );
        assert_eq!(admission.peers.len(), 1);
        drop(held);
        assert!(
            admission
                .try_acquire("::ffff:192.0.2.1".parse().unwrap())
                .is_some()
        );
    }
}
