use std::collections::{BTreeMap, BTreeSet};

use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version}, replica::ReplicaIdx,
};

use super::{versioned_vote::VersionedVote, vote_origin::VoteOrigin};

/// Causally delivered votes and incremental support for candidate leaders.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct VoteLog {
    pub(crate) causal_votes: BTreeMap<ReplicaIdx, BTreeMap<usize, ReplicaIdx>>,
    /// Presence means still supporting; true means the first successor has arrived.
    pub(crate) partial_leaders: BTreeMap<VersionedVote, BTreeMap<ReplicaIdx, bool>>,
    pub(crate) last_committed: Option<VersionedVote>,
}

impl VoteLog {
    pub fn is_empty(&self) -> bool {
        self.causal_votes.values().all(BTreeMap::is_empty)
            && self.partial_leaders.is_empty()
            && self.last_committed.is_none()
    }

    pub fn insert(&mut self, vertex: VersionedVote) {
        assert!(vertex.origin_seq() > 0, "Votes must identify actual events");
        let trace = self.causal_votes.entry(vertex.origin_idx()).or_default();
        let previous_seq = trace.last_key_value().map_or(0, |(seq, _)| *seq);
        assert_eq!(previous_seq, vertex.origin_seq() - 1, "Votes must be delivered once, in causal order");
        trace.insert(vertex.origin_seq(), vertex.vote());
        self.update_partial_states(&vertex);

        // A delayed candidate concurrent with a committed checkpoint can never
        // enter its committed chain. Its old predecessor votes may be pruned.
        // Still record its vote above: later candidates can depend on it.
        if vertex.is_partial() && self.after_committed(&vertex) {
            let state = self.get_partial_state(&vertex);
            self.partial_leaders.insert(vertex, state);
        }
    }

    fn after_committed(&self, vertex: &VersionedVote) -> bool {
        self.last_committed.as_ref().is_none_or(|committed| {
            vertex.vote_origin() != committed.vote_origin()
                && committed.version().iter().all(|(r, seq)| {
                    vertex.version().seq_by_idx(r) >= seq
                })
        })
    }

    fn already_committed(&self, id: &VoteOrigin) -> bool {
        self.last_committed.as_ref().is_some_and(|v| {
            id.seq() <= v.version().seq_by_idx(id.id())
        })
    }

    /// No successor of a newly delivered vertex has been delivered yet.
    fn get_partial_state(&self, vertex: &VersionedVote) -> BTreeMap<ReplicaIdx, bool> {
        let mut state = BTreeMap::new();
        for (&replica, trace) in &self.causal_votes {
            let previous = if replica == vertex.origin_idx() {
                vertex.origin_seq() - 1
            } else {
                vertex.version().seq_by_idx(replica)
            };
            if previous == 0 {
                continue;
            }
            // For a candidate strictly after last_committed, the metadata
            // retention boundary preserves this predecessor. Never interpret
            // a missing/pruned trace as vacuous support.
            assert!(trace.contains_key(&previous), "Missing candidate predecessor vote");
            if trace.range(previous..).all(|(_, vote)| *vote == vertex.origin_idx()) {
                state.insert(replica, false);
            }
        }
        state
    }

    fn update_partial_states(&mut self, vertex: &VersionedVote) {
        let origin = vertex.origin_idx();
        for (candidate, state) in &mut self.partial_leaders {
            if state.get(&origin) != Some(&false) {
                continue;
            }
            if vertex.vote() != candidate.origin_idx() {
                state.remove(&origin);
                continue;
            }
            if vertex.version().seq_by_idx(candidate.origin_idx()) >= candidate.origin_seq() {
                state.insert(origin, true);
            }
        }
    }

    pub fn set_committed(&mut self, version: &Version) {
        if let Some(vertex) = self.get_partial_leader_by_version(version) {
            assert!(self.after_committed(vertex), "Commit checkpoints must advance causally");
            self.last_committed = Some(vertex.clone());
        } else {
            return
        }

    }

    /// Follow immutable sender-chosen predecessors, stopping at the committed cut.
    pub fn ready_to_commit(&self, members: &[ReplicaIdx]) -> Vec<&Version> {
        assert!(!members.is_empty(), "Commitment requires a fixed nonempty membership");
        let Some((mut anchor, _)) = self.partial_leaders.iter().rev().find(|(v, state)| {
            self.after_committed(v) && Self::is_leader(state, members)
        }) else {
            return Vec::new();
        };
        let mut ready = BTreeSet::new();
        ready.insert(anchor);
        while let Some(id) = anchor.last_partial() {
            if self.already_committed(id) {
                break;
            }
            let previous = self.get_partial_by_vote_origin(id)
                .expect("Missing uncommitted predecessor checkpoint");
            assert!(
                previous.vote_origin() != anchor.vote_origin()
                    && previous.version().iter().all(|(r, seq)| anchor.version().seq_by_idx(r) >= seq),
                "Checkpoint predecessor must be in its strict causal past"
            );
            assert!(ready.insert(previous), "Cycle in checkpoint chain");
            anchor = previous;
        }
        ready.iter()
            .map(|versioned_vote| versioned_vote.version())
            .collect()
    }

    pub fn get_leaders(&self, members: &[ReplicaIdx]) -> BTreeSet<&VersionedVote> {
        assert!(!members.is_empty(), "Commitment requires a fixed nonempty membership");
        self.partial_leaders.iter()
            .filter(|(_, state)| Self::is_leader(state, members))
            .map(|(vertex, _)| vertex)
            .collect()
    }

    fn get_partial_by_vote_origin(&self, id: &VoteOrigin) -> Option<&VersionedVote> {
        self.partial_leaders.keys().find(|v| v.vote_origin() == id)
    }

    fn get_partial_leader_by_version(&self, version: &Version) -> Option<&VersionedVote> {
        self.partial_leaders.keys().find(|v| {
            v.origin_idx() == version.origin_idx() && v.origin_seq() == version.origin_seq()
        })
    }

    /// Metadata retention boundary ONLY; not a causally closed application cut.
    /// Retain previous(v) and v, even though their application operations commit.
    pub fn get_causal_frontier(&self, version: &Version) -> CausalFrontier {
        let mut frontier = CausalFrontier::from(version);
        for (r, seq) in version.iter() {
            let retained = if r == version.origin_idx() { 2 } else { 1 };
            frontier.set_by_idx(r, seq.saturating_sub(retained));
        }
        frontier
    }

    pub fn prune(&mut self, frontier: &CausalFrontier) {
        for (&replica, trace) in &mut self.causal_votes {
            let through = frontier.seq_by_idx(replica);
            trace.retain(|seq, _| *seq > through);
        }
        self.partial_leaders.retain(|v, _| {
            v.origin_seq() > frontier.seq_by_idx(v.origin_idx())
        });
    }

    fn is_leader(state: &BTreeMap<ReplicaIdx, bool>, members: &[ReplicaIdx]) -> bool {
        members.iter().filter(|r| state.get(*r) == Some(&true)).count() > members.len() / 2
    }

    /// Deliberately select the predecessor in past(new_vertex), before its vote.
    /// This defines the recursive closure documented in CORRECTNESS.md.
    pub fn is_potential(&self, my_id: ReplicaIdx, members: &[ReplicaIdx]) -> (bool, Option<VoteOrigin>) {
        assert!(!members.is_empty(), "Commitment requires a fixed nonempty membership");
        let votes = members.iter().filter(|r| {
            self.causal_votes.get(*r)
                .and_then(|trace| trace.last_key_value())
                .is_some_and(|(_, vote)| *vote == my_id)
        }).count();
        if votes <= members.len() / 2 {
            return (false, None);
        }
        let previous = self.partial_leaders.iter().rev()
            .find(|(_, state)| Self::is_current_partial(state, members))
            .map(|(v, _)| v.vote_origin().clone());
        (true, previous)
    }

    pub fn latest_frontier(&self) -> Vec<(ReplicaIdx, usize, ReplicaIdx)> {
        self.causal_votes.iter().filter_map(|(r, trace)| {
            trace.last_key_value().map(|(seq, vote)| (*r, *seq, *vote))
        }).collect()
    }

    fn is_current_partial(state: &BTreeMap<ReplicaIdx, bool>, members: &[ReplicaIdx]) -> bool {
        members.iter().filter(|r| state.contains_key(*r)).count() > members.len() / 2
    }
}
