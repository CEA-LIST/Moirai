use std::collections::VecDeque;

use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    replica::ReplicaIdx,
};

/// Timestamp stability over fixed, dense local member indices.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct LamportTracker {
    lamport_values: Vec<usize>,
    // Each queue contains consecutive timestamps after that member's stable seq.
    pending: Vec<VecDeque<usize>>,
    last_stable_timestamp: usize,
    last_stable_frontier: Option<Version>,
    committed: Option<CausalFrontier>,
}

impl LamportTracker {
    pub fn member_count(&self) -> usize {
        self.lamport_values.len()
    }

    pub fn new(member_count: usize) -> Self {
        assert!(
            member_count > 0,
            "Commitment requires nonempty fixed membership"
        );
        Self {
            lamport_values: vec![0; member_count],
            pending: vec![VecDeque::new(); member_count],
            ..Self::default()
        }
    }

    pub fn update_timestamp(&mut self, version: &Version) {
        assert!(
            !self.lamport_values.is_empty(),
            "Configure fixed membership before handling events"
        );
        assert_eq!(
            version.iter().count(),
            self.lamport_values.len(),
            "Fixed membership changed"
        );
        let frontier = self.last_stable_frontier.get_or_insert_with(|| {
            // Preserve the incoming clock's fixed identity metadata for the child
            // log and TCSB. Commitment only reads and updates numeric components.
            let mut cut = CausalFrontier::from(version);
            for (idx, _) in version.iter() {
                cut.set_by_idx(idx, 0);
            }
            Version::from((cut, ReplicaIdx(0)))
        });
        let origin = version.origin_idx();
        let trace = &mut self.pending[origin.0];
        assert_eq!(
            version.origin_seq(),
            frontier.seq_by_idx(origin) + trace.len() + 1,
            "Events must arrive once, in causal order"
        );
        let timestamp = version.sum();
        self.lamport_values[origin.0] = timestamp;
        trace.push_back(timestamp);

        // Unseen members retain timestamp zero and block commitment.
        let stable = *self.lamport_values.iter().min().unwrap();
        assert!(
            stable >= self.last_stable_timestamp,
            "Stable timestamp regressed"
        );
        self.last_stable_timestamp = stable;
        for (idx, trace) in self.pending.iter_mut().enumerate() {
            let idx = ReplicaIdx(idx);
            while trace.front().is_some_and(|timestamp| *timestamp <= stable) {
                trace.pop_front();
                frontier.set_by_idx(idx, frontier.seq_by_idx(idx) + 1);
            }
        }
    }

    pub fn ready_to_commit(&self) -> Vec<&Version> {
        let Some(frontier) = &self.last_stable_frontier else {
            return Vec::new();
        };
        if frontier.sum() == 0
            || self
                .committed
                .as_ref()
                .is_some_and(|cut| frontier.iter().all(|(idx, seq)| seq == cut.seq_by_idx(idx)))
        {
            Vec::new()
        } else {
            vec![frontier]
        }
    }

    pub fn get_stable_frontier(&self) -> Option<&Version> {
        self.last_stable_frontier.as_ref()
    }

    pub fn set_committed(&mut self, version: &Version) {
        let frontier = self
            .last_stable_frontier
            .as_ref()
            .expect("No events delivered");
        assert!(
            frontier.iter().eq(version.iter()),
            "Checkpoint must be the stable frontier"
        );
        self.committed = Some(CausalFrontier::from(version));
    }

    pub fn last_committed(&self) -> Option<&CausalFrontier> {
        self.committed.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{broadcast::internalizer::Interner, replica::ReplicaId};

    fn clock(names: &[&str]) -> Version {
        let mut interner = Interner::new();
        for name in names {
            interner.intern(&ReplicaId::from(*name));
        }
        Version::new(ReplicaIdx(0), interner.resolver().clone())
    }

    fn version(clock: &Version, origin: usize, seqs: &[usize]) -> Version {
        let mut v = Version::from((CausalFrontier::from(clock), ReplicaIdx(origin)));
        for (i, seq) in seqs.iter().enumerate() {
            v.set_by_idx(ReplicaIdx(i), *seq);
        }
        v
    }

    #[test]
    fn frontier_uses_sequences_and_waits_for_unseen_members() {
        let clock = clock(&["A", "B"]);
        let mut tracker = LamportTracker::new(2);
        assert!(tracker.get_stable_frontier().is_none());
        tracker.update_timestamp(&version(&clock, 0, &[1, 0]));
        tracker.update_timestamp(&version(&clock, 0, &[2, 0]));
        assert!(tracker.ready_to_commit().is_empty());
        tracker.update_timestamp(&version(&clock, 1, &[2, 1]));
        // Minimum latest timestamp is 2; B's first event has timestamp 3.
        let frontier = tracker.get_stable_frontier().unwrap().clone();
        assert_eq!(frontier.seq_by_idx(ReplicaIdx(0)), 2);
        assert_eq!(frontier.seq_by_idx(ReplicaIdx(1)), 0);
        tracker.set_committed(&frontier);
        assert!(tracker.ready_to_commit().is_empty());
    }

    #[test]
    fn pending_sequences_continue_after_repeated_commitment() {
        let clock = clock(&["B", "A"]);
        let mut tracker = LamportTracker::new(2);
        tracker.update_timestamp(&version(&clock, 0, &[1, 0]));
        tracker.update_timestamp(&version(&clock, 1, &[1, 1]));
        let first = tracker.get_stable_frontier().unwrap().clone();
        tracker.set_committed(&first);
        tracker.update_timestamp(&version(&clock, 0, &[2, 1]));
        let second = tracker.get_stable_frontier().unwrap().clone();
        assert_eq!(second.seq_by_idx(ReplicaIdx(0)), 1);
        assert_eq!(second.seq_by_idx(ReplicaIdx(1)), 1);
        tracker.set_committed(&second);
        tracker.update_timestamp(&version(&clock, 1, &[2, 2]));
        assert_eq!(
            tracker
                .get_stable_frontier()
                .unwrap()
                .seq_by_idx(ReplicaIdx(0)),
            2
        );
        assert_eq!(tracker.lamport_values, vec![3, 4]);
        assert_eq!(tracker.pending[1].len(), 1);
    }

    #[test]
    #[should_panic(expected = "Events must arrive once, in causal order")]
    fn duplicate_events_are_rejected_even_after_their_timestamps_are_pruned() {
        let clock = clock(&["A"]);
        let mut tracker = LamportTracker::new(1);
        tracker.update_timestamp(&version(&clock, 0, &[1]));
        tracker.update_timestamp(&version(&clock, 0, &[1]));
    }

    #[test]
    fn incremental_queues_match_a_full_timestamp_history() {
        let clock = clock(&["B", "C", "A"]);
        let mut tracker = LamportTracker::new(3);
        let mut counters = [0; 3];
        let mut history = [Vec::new(), Vec::new(), Vec::new()];
        for step in 0..200 {
            let origin = (step * 7 + step / 5) % 3;
            counters[origin] += 1;
            let event = version(&clock, origin, &counters);
            history[origin].push(event.sum());
            tracker.update_timestamp(&event);
            let stable = history
                .iter()
                .map(|trace| trace.last().copied().unwrap_or(0))
                .min()
                .unwrap();
            let frontier = tracker.get_stable_frontier().unwrap();
            for (idx, trace) in history.iter().enumerate() {
                assert_eq!(
                    frontier.seq_by_idx(ReplicaIdx(idx)),
                    trace
                        .iter()
                        .filter(|timestamp| **timestamp <= stable)
                        .count()
                );
            }
            if !tracker.ready_to_commit().is_empty() {
                tracker.set_committed(&frontier.clone());
                assert!(tracker.ready_to_commit().is_empty());
            }
        }
    }
}
