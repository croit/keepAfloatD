//! Log-storage half of the keepafloatd Raft store (`RaftLogReader` + `RaftLogStorage`).
//!
//! Holds the Raft log, the persisted vote, and the committed frontier. The volatile in-memory log
//! lives in the shared [`KafStorageState`]; this half only implements the openraft 0.10 log traits
//! over it. All the diskless restart/reform defensive guards (purge-floor seeding/derivation,
//! drop-below-purge, inverted-range normalisation, conflict-truncation floor lowering) live here.

use super::super::types::TypeConfig;
use super::state::KafStorageState;
use openraft::alias::{EntryOf, LogIdOf, VoteOf};
use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{LogId, OptionalSend};
use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Log-storage handle over the shared in-memory Raft state.
///
/// Cloneable: `get_log_reader` hands openraft another handle onto the same `Arc`.
#[derive(Clone)]
pub struct KafLogStore {
    pub(super) state: Arc<RwLock<KafStorageState>>,
}

impl KafLogStore {
    pub(super) fn new(state: Arc<RwLock<KafStorageState>>) -> Self {
        Self { state }
    }
}

impl RaftLogReader<TypeConfig> for KafLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error> {
        // `BTreeMap::range` PANICS if the resolved start bound is greater than the end bound.
        // openraft can hand us such a range during stale-survivor incarnation churn (a fenced node
        // whose committed/log view briefly disagrees with an incoming RPC), so normalise first and
        // return an empty slice for an empty/inverted range rather than panicking RaftCore.
        let start = match range.start_bound() {
            Bound::Included(&i) => Some(i),
            Bound::Excluded(&i) => i.checked_add(1),
            Bound::Unbounded => None,
        };
        let end = match range.end_bound() {
            Bound::Included(&i) => Some(i),
            Bound::Excluded(&i) => i.checked_sub(1),
            Bound::Unbounded => None,
        };
        if let (Some(s), Some(e)) = (start, end) {
            if s > e {
                return Ok(Vec::new());
            }
        }
        let state = self.state.read().await;
        Ok(state.log.range(range).map(|(_, e)| e.clone()).collect())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        Ok(self.state.read().await.vote)
    }
}

impl RaftLogStorage<TypeConfig> for KafLogStore {
    type LogReader = Self;

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        self.state.write().await.vote = Some(*vote);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        // openraft calls this before every apply; persist it so a restart can resume the commit
        // frontier instead of booting with `committed = None`.
        self.state.write().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
        // Resume the engine's commit frontier on startup. The in-memory store is volatile, so after
        // a restart `committed` is `None` even though the leader will immediately backfill a
        // committed suffix whose floor is non-zero. Report the highest of {saved committed,
        // last_applied, the physical log floor}: this guarantees the engine's first commit-driven
        // apply reads `get_log_entries(frontier.next_index()..)` at or above the log floor, never
        // index 0 — which is what trips `Defensive(LogIndexNotFound { want: 0 })` on a wiped node.
        let state = self.state.read().await;
        // Return the highest of {saved committed, applied frontier, purge floor}. The purge floor
        // (last_purged_log_id, seeded to `lowest_log_index - 1` on backfill) is the key term for a
        // wiped-then-backfilled node: it makes openraft boot with `committed = Some(floor)` so the
        // first commit-driven apply reads `get_log_entries(floor.next_index()..)` at/above the log
        // floor, never index 0 (which trips Defensive(LogIndexNotFound { want: 0 })). Clamping up to
        // last_applied keeps committed >= applied; the floor never sits below an already-purged
        // prefix, so openraft's startup reapply range [last_applied.next..committed.next) only spans
        // entries physically present.
        // `committed` and `last_applied` are authoritative: openraft saved the former and the state
        // machine reached the latter, so both always point at a real, servable position. Start the
        // frontier from the higher of the two; never clamp below this.
        let mut best = state.committed;
        if let Some(applied) = state.last_applied_log {
            if best.map(|b| b.index()).unwrap_or(0) < applied.index() || best.is_none() {
                best = Some(applied);
            }
        }

        // `last_purged_log_id` is only a compaction boundary marker, NOT a servable entry. On a
        // wiped/holed survivor it can sit ABOVE every entry the log actually holds. Folding it into
        // the frontier unconditionally over-reports a `committed` the store cannot serve: openraft
        // seeds its startup `committed` to that stale high-INDEX id, then a reform commit at a
        // higher term but LOWER index (LogId Ord is term-first) satisfies `update_committed`,
        // producing `since = already_committed.next_index() > end = upto.index + 1`. The apply path
        // requests the inverted range `since..end`, the defensive check treats it as an empty
        // range, and `entries[entries.len() - 1]` underflows to `usize::MAX`, panicking RaftCore at
        // raft_core.rs:761.
        //
        // Only let the purge floor RAISE the frontier when the log actually holds entries at/above
        // it (the restart-then-backfill case: the leader resent the committed suffix, so the log
        // tail covers the floor and reading `get_log_entries(floor.next..)` stays in-bounds — which
        // is what avoids `LogIndexNotFound { want: 0 }`). When the log is empty or sits below the
        // floor, the purged entries are unreachable, so the floor must NOT bump the frontier.
        let log_tail = state.log.iter().next_back().map(|(_, e)| e.log_id);
        if let Some(purged) = state.last_purged_log_id {
            let log_covers_floor = log_tail
                .map(|t| t.index() >= purged.index())
                .unwrap_or(false);
            if log_covers_floor
                && (best.map(|b| b.index()).unwrap_or(0) < purged.index() || best.is_none())
            {
                best = Some(purged);
            }
        }
        Ok(best)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let state = self.state.read().await;
        let last_log_id = state.log.iter().next_back().map(|(_, e)| e.log_id);
        // Read-side safety net for the restart-then-backfill case: if the in-memory store was
        // wiped (last_purged_log_id = None) but the log floor is non-zero (a committed suffix was
        // backfilled above the lost purge point), derive the purge floor at `lowest - 1`. Without
        // this, openraft sees None + a non-zero last_log_id, scans from index 0, and trips
        // Defensive(LogIndexNotFound { want: 0, got: Some(<floor>) }). Keeps last_purged <=
        // last_log_id. Complements the source-side seed in `append`.
        let last_purged_log_id = match state.last_purged_log_id {
            Some(p) => Some(p),
            // Only derive a floor when the log starts ABOVE index 1 (a 1-based start has nothing
            // purged below it). idx > 1 ⇒ indices 1..idx-1 were purged pre-restart.
            None => state.log.iter().next().and_then(|(&idx, e)| {
                (idx > 1).then(|| LogId::new(*e.log_id.committed_leader_id(), idx - 1))
            }),
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut state = self.state.write().await;
            let purged = state.last_purged_log_id.map(|l| l.index());
            for entry in entries {
                // Never resurrect the compacted prefix. openraft only appends above the purge point;
                // an entry at or below it would re-open a hole below last_purged_log_id and desync the
                // log view, so drop it and surface the broken upstream invariant.
                if let Some(p) = purged {
                    if entry.log_id.index() <= p {
                        tracing::warn!(
                            index = entry.log_id.index(),
                            purged_upto = p,
                            "append: ignoring entry at or below the purge point"
                        );
                        continue;
                    }
                }
                // Restart-then-backfill seed: after a restart the in-memory store is empty
                // (last_purged_log_id = None). When the leader backfills the committed suffix starting
                // ABOVE the lost purge floor (index > 1) into a still-empty log, seed
                // last_purged_log_id = index - 1 so the floor matches the physical log minimum.
                // Otherwise the store reports None with a non-zero log floor and openraft reads index 0,
                // tripping Defensive(LogIndexNotFound). A normal log begins at index 1 (no purge below
                // it), so only index > 1 signals a real compacted gap; we never seed for a 1-based
                // start, and only when the floor is unset and the log is still empty.
                if state.last_purged_log_id.is_none()
                    && state.log.is_empty()
                    && entry.log_id.index() > 1
                {
                    state.last_purged_log_id = Some(LogId::new(
                        *entry.log_id.committed_leader_id(),
                        entry.log_id.index() - 1,
                    ));
                }
                state.log.insert(entry.log_id.index(), entry);
            }
        }
        // Volatile store: the entries are durable enough the instant they are in the BTreeMap, so
        // signal flush completion inline. Must fire after the insert and before returning Ok.
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        // openraft 0.10 `truncate_after(Some(L))` removes every entry STRICTLY AFTER L (indices
        // `L.index()+1..`); `truncate_after(None)` removes the whole log. This replaces 0.9's
        // `delete_conflict_logs_since(log_id)` which removed `log_id.index..` (inclusive of the id).
        let mut state = self.state.write().await;
        let start = last_log_id.map(|l| l.index() + 1).unwrap_or(0);
        let rm: Vec<u64> = state.log.range(start..).map(|(k, _)| *k).collect();
        for i in rm {
            state.log.remove(&i);
        }
        // Lower the purge floor if this truncation rolls the kept log below it. After a full cluster
        // reform the leader truncates a survivor's log back to a low index and replays from there;
        // if `last_purged_log_id` stayed at the old high floor, `append`'s drop-guard would silently
        // drop every resent low entry and the log would never repopulate — leaving a permanent
        // `LogIndexNotFound { want: 0 }` crash loop. After truncate_after, everything above
        // `last_log_id.index()` is gone, so the floor must be at most that index (or `None` when the
        // whole log was truncated). Lower it only when it currently sits above the kept tail.
        let kept_upto = last_log_id.map(|l| l.index());
        if state.last_purged_log_id.map(|l| l.index()) > kept_upto {
            state.last_purged_log_id = last_log_id;
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        let mut state = self.state.write().await;
        let rm: Vec<u64> = state
            .log
            .range(..=log_id.index())
            .map(|(k, _)| *k)
            .collect();
        for i in rm {
            state.log.remove(&i);
        }
        // last_purged_log_id is monotonic; never move the boundary backwards.
        if state.last_purged_log_id.map(|l| l.index()) < Some(log_id.index()) {
            state.last_purged_log_id = Some(log_id);
        }
        Ok(())
    }
}
