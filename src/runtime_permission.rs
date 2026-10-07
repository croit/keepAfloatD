//! Process-local authority that cannot be restored from a snapshot or late response.

use crate::raft::TypeConfig;
use openraft::alias::LogIdOf;
use tokio::sync::watch;
use tokio::time::Instant;

mod admission;
mod timing;
pub(crate) use admission::RuntimeAuthority;
pub(crate) use timing::LeaseTiming;

/// Immutable identity of one admitted runtime; never part of replicated state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AuthorityContext {
    pub(crate) boot_nonce: [u8; 32],
    pub(crate) cluster_epoch: u128,
    pub(crate) genesis_digest: [u8; 32],
    pub(crate) admission_generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Joining(LogIdOf<TypeConfig>),
    Voting,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Quarantined(Instant),
    Active(AuthorityContext, Stage, Instant),
    Sealed,
}

impl State {
    fn seal_expired(&mut self, now: Instant) -> bool {
        if matches!(self, Self::Active(_, _, until) if now >= *until) {
            *self = Self::Sealed;
            true
        } else {
            false
        }
    }
}

/// The admission controller supplies verified deadlines; this fence does not verify receipts.
pub(crate) struct RuntimePermission {
    state: watch::Sender<State>,
}

impl RuntimePermission {
    pub(crate) fn quarantined(until: Instant) -> Self {
        Self {
            state: watch::channel(State::Quarantined(until)).0,
        }
    }

    /// Admit exactly once, after quarantine, using an already verified quorum grant.
    pub(crate) fn admit(&self, context: AuthorityContext, until: Instant) -> bool {
        let mut admitted = false;
        self.state.send_if_modified(|state| {
            let now = Instant::now();
            if let State::Quarantined(ready) = *state
                && now >= ready
                && now < until
            {
                *state = State::Active(context, Stage::Voting, until);
                admitted = true;
                return true;
            }
            state.seal_expired(now)
        });
        admitted
    }

    /// Admit or extend the same unexpired learner operation, never voting authority.
    pub(crate) fn join(
        &self,
        context: AuthorityContext,
        prepared: LogIdOf<TypeConfig>,
        until: Instant,
    ) -> bool {
        let mut accepted = false;
        self.state.send_if_modified(|state| {
            let now = Instant::now();
            if state.seal_expired(now) {
                return true;
            }
            accepted = match *state {
                State::Quarantined(ready) => now >= ready && now < until,
                State::Active(current, Stage::Joining(operation), previous) => {
                    current == context && operation == prepared && until > previous
                }
                _ => false,
            };
            if accepted {
                *state = State::Active(context, Stage::Joining(prepared), until);
            }
            accepted
        });
        accepted
    }

    /// The caller verifies final committed membership before marking the learner promoted.
    pub(crate) fn mark_promoted(
        &self,
        context: AuthorityContext,
        prepared: LogIdOf<TypeConfig>,
    ) -> bool {
        let mut promoted = false;
        self.state.send_if_modified(|state| {
            if state.seal_expired(Instant::now()) {
                return true;
            }
            if let State::Active(current, Stage::Joining(operation), until) = *state
                && current == context
                && operation == prepared
            {
                *state = State::Active(current, Stage::Voting, until);
                promoted = true;
            }
            promoted
        });
        promoted
    }

    /// Extend current authority, never a retired runtime or another authority context.
    pub(crate) fn renew(&self, context: AuthorityContext, until: Instant) -> bool {
        let mut renewed = false;
        self.state.send_if_modified(|state| {
            if state.seal_expired(Instant::now()) {
                return true;
            }
            if let State::Active(current, Stage::Voting, previous) = *state
                && current == context
                && until >= previous
            {
                renewed = true;
                if until > previous {
                    *state = State::Active(context, Stage::Voting, until);
                    return true;
                }
            }
            false
        });
        renewed
    }

    /// Check immediately before dispatch; expiry is terminal even without a watcher.
    pub(crate) fn current(&self) -> Option<(AuthorityContext, Instant)> {
        let mut current = None;
        self.state.send_if_modified(|state| {
            let changed = state.seal_expired(Instant::now());
            if let State::Active(context, _, until) = *state {
                current = Some((context, until));
            }
            changed
        });
        current
    }

    /// Revoke before any awaited cleanup or task shutdown.
    pub(crate) fn seal(&self) -> bool {
        self.state.send_if_modified(|state| {
            if *state == State::Sealed {
                false
            } else {
                *state = State::Sealed;
                true
            }
        })
    }

    pub(crate) async fn wait_until_sealed(&self) {
        let mut receiver = self.state.subscribe();
        loop {
            let state = *receiver.borrow_and_update();
            match state {
                State::Sealed => return,
                State::Quarantined(_) => {
                    if receiver.changed().await.is_err() {
                        self.seal();
                        return;
                    }
                }
                State::Active(_, _, until) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(until) => {
                            self.current();
                        }
                        changed = receiver.changed() => {
                            if changed.is_err() {
                                self.seal();
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::Instant;

    fn context() -> AuthorityContext {
        AuthorityContext {
            boot_nonce: [1; 32],
            cluster_epoch: 2,
            genesis_digest: [3; 32],
            admission_generation: 4,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn quarantine_cannot_be_bypassed_by_an_early_grant() {
        let start = Instant::now();
        let permission = RuntimePermission::quarantined(start + Duration::from_secs(10));
        assert_eq!(permission.current(), None);
        assert!(!permission.admit(context(), start + Duration::from_secs(30)));
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(permission.admit(context(), start + Duration::from_secs(30)));
        assert_eq!(
            permission.current(),
            Some((context(), start + Duration::from_secs(30)))
        );
        assert!(!permission.admit(context(), start + Duration::from_secs(40)));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_permission_cannot_be_revived_by_a_late_reply() {
        let start = Instant::now();
        let permission = RuntimePermission::quarantined(start);
        assert!(permission.admit(context(), start + Duration::from_secs(5)));
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(!permission.renew(context(), start + Duration::from_secs(30)));
        assert_eq!(permission.current(), None);
        assert!(!permission.admit(context(), start + Duration::from_secs(30)));
        permission.wait_until_sealed().await;
    }

    #[tokio::test(start_paused = true)]
    async fn renewal_preserves_exact_context_and_never_shortens_the_deadline() {
        let start = Instant::now();
        let first = start + Duration::from_secs(5);
        let renewed = start + Duration::from_secs(10);
        let permission = RuntimePermission::quarantined(start);
        assert!(!permission.renew(context(), first));
        assert!(permission.admit(context(), first));
        let mut changed = [context(); 4];
        changed[0].boot_nonce[31] ^= 1;
        changed[1].cluster_epoch += 1;
        changed[2].genesis_digest[31] ^= 1;
        changed[3].admission_generation += 1;
        for foreign in changed {
            assert!(!permission.renew(foreign, renewed));
            assert_eq!(permission.current(), Some((context(), first)));
        }
        assert!(permission.renew(context(), first));
        assert!(!permission.renew(context(), start));
        assert_eq!(permission.current(), Some((context(), first)));
        assert!(permission.renew(context(), renewed));
        assert_eq!(permission.current(), Some((context(), renewed)));
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_initial_deadlines_do_not_create_authority() {
        let now = Instant::now();
        let permission = RuntimePermission::quarantined(now);
        assert!(!permission.admit(context(), now));
        assert!(!permission.admit(context(), now - Duration::from_secs(1)));
        assert_eq!(permission.current(), None);
        assert!(permission.admit(context(), now + Duration::from_secs(1)));
    }

    #[tokio::test(start_paused = true)]
    async fn explicit_seal_is_terminal_before_or_after_activation() {
        for activate in [false, true] {
            let start = Instant::now();
            let permission = RuntimePermission::quarantined(start);
            if activate {
                assert!(permission.admit(context(), start + Duration::from_secs(5)));
            }
            assert!(permission.seal());
            assert!(!permission.seal());
            assert_eq!(permission.current(), None);
            assert!(!permission.admit(context(), start + Duration::from_secs(30)));
            assert!(!permission.renew(context(), start + Duration::from_secs(30)));
            permission.wait_until_sealed().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_waiter_observes_renewal_but_never_misses_sealing() {
        let now = Instant::now();
        let permission = RuntimePermission::quarantined(now);
        let waiting = permission.wait_until_sealed();
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        assert!(permission.admit(context(), now + Duration::from_secs(5)));
        assert!(futures::poll!(&mut waiting).is_pending());
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(permission.renew(context(), now + Duration::from_secs(10)));
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(futures::poll!(&mut waiting).is_pending());
        tokio::time::advance(Duration::from_secs(4)).await;
        waiting.await;
        assert_eq!(permission.current(), None);
        assert!(!permission.renew(context(), now + Duration::from_secs(20)));
    }

    #[tokio::test(start_paused = true)]
    async fn current_query_also_seals_expiry_without_a_waiter() {
        let now = Instant::now();
        let permission = RuntimePermission::quarantined(now);
        assert!(permission.admit(context(), now + Duration::from_secs(1)));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(permission.current(), None);
        assert!(!permission.seal());
    }

    #[tokio::test(start_paused = true)]
    async fn joining_extension_requires_same_operation_and_promotion_is_one_way() {
        let now = Instant::now();
        let prepared = openraft::testing::log_id::<crate::raft::TypeConfig>(
            1,
            crate::raft::types::test_replica(1),
            3,
        );
        let permission = RuntimePermission::quarantined(now);
        assert!(permission.join(context(), prepared, now + Duration::from_secs(5)));
        assert!(!permission.admit(context(), now + Duration::from_secs(10)));
        assert!(!permission.join(context(), prepared, now + Duration::from_secs(5)));
        assert!(!permission.renew(context(), now + Duration::from_secs(10)));
        let mut changed = [context(); 4];
        changed[0].boot_nonce[0] ^= 1;
        changed[1].cluster_epoch += 1;
        changed[2].genesis_digest[0] ^= 1;
        changed[3].admission_generation += 1;
        for foreign in changed {
            assert!(!permission.join(foreign, prepared, now + Duration::from_secs(10)));
            assert!(!permission.mark_promoted(foreign, prepared));
        }
        let mut other = prepared;
        other.index += 1;
        assert!(!permission.join(context(), other, now + Duration::from_secs(10)));
        assert!(!permission.mark_promoted(context(), other));
        assert!(permission.join(context(), prepared, now + Duration::from_secs(10)));
        assert!(permission.mark_promoted(context(), prepared));
        assert!(!permission.join(context(), prepared, now + Duration::from_secs(20)));
        assert!(permission.renew(context(), now + Duration::from_secs(20)));
        permission.seal();
        assert!(!permission.join(context(), prepared, now + Duration::from_secs(30)));
        assert!(!permission.mark_promoted(context(), prepared));
    }

    #[tokio::test(start_paused = true)]
    async fn joining_expiry_is_terminal_even_without_reading_the_fence() {
        let now = Instant::now();
        let prepared = openraft::testing::log_id::<crate::raft::TypeConfig>(
            1,
            crate::raft::types::test_replica(1),
            3,
        );
        let permission = RuntimePermission::quarantined(now);
        assert!(permission.join(context(), prepared, now + Duration::from_secs(5)));
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(!permission.join(context(), prepared, now + Duration::from_secs(20)));
        assert!(!permission.mark_promoted(context(), prepared));
        assert_eq!(permission.current(), None);
        permission.wait_until_sealed().await;
    }

    #[tokio::test(start_paused = true)]
    async fn joining_respects_quarantine_deadlines_and_wakes_the_expiry_watcher() {
        let now = Instant::now();
        let prepared = openraft::testing::log_id::<crate::raft::TypeConfig>(
            1,
            crate::raft::types::test_replica(1),
            3,
        );
        let permission = RuntimePermission::quarantined(now + Duration::from_secs(1));
        assert!(!permission.join(context(), prepared, now + Duration::from_secs(5)));
        assert!(!permission.mark_promoted(context(), prepared));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!permission.join(context(), prepared, Instant::now()));
        let waiting = permission.wait_until_sealed();
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());
        assert!(permission.join(context(), prepared, now + Duration::from_secs(5)));
        assert!(futures::poll!(&mut waiting).is_pending());
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(permission.join(context(), prepared, now + Duration::from_secs(10)));
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(futures::poll!(&mut waiting).is_pending());
        assert!(permission.mark_promoted(context(), prepared));
        assert!(futures::poll!(&mut waiting).is_pending());
        tokio::time::advance(Duration::from_secs(4)).await;
        waiting.await;
        assert_eq!(permission.current(), None);
    }
}
