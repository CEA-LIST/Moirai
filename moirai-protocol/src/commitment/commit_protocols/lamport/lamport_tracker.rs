use std::collections::BTreeMap;

use crate::{
    broadcast::internalizer::Resolver,
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    replica::ReplicaIdx,
};

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct LamportTracker {
    lamport_values: BTreeMap<ReplicaIdx, usize>,
    pending: BTreeMap<ReplicaIdx, BTreeMap<usize, usize>>,
    last_stable_timestamp: usize,
    last_stable_frontier: Option<Version>,
    committed: Option<CausalFrontier>,
    resolver: Option<Resolver>,
}

impl LamportTracker {
    pub fn update_timestamp(&mut self, version: &Version) {
        self.update_resolver(version.resolver().clone());
        let origin = version.origin_idx();
        let trace = self.pending.entry(origin).or_default();
        let previous = trace.last_key_value().map_or_else(
            || {
                self.last_stable_frontier
                    .as_ref()
                    .unwrap()
                    .seq_by_idx(origin)
            },
            |(seq, _)| *seq,
        );
        assert_eq!(
            version.origin_seq(),
            previous + 1,
            "Events must arrive once, in causal order"
        );
        self.lamport_values.insert(origin, version.sum());
        trace.insert(version.origin_seq(), version.sum());
        self.compute_stable_frontier();
    }

    pub fn ready_to_commit(&self, members: &[ReplicaIdx]) -> Vec<&Version> {
        let Some(resolver) = &self.resolver else {
            return Vec::new();
        };
        assert_eq!(
            members.len(),
            resolver.len(),
            "Membership and resolver differ"
        );
        assert!(
            (0..resolver.len()).all(|i| members.contains(&ReplicaIdx(i))),
            "Invalid membership"
        );
        let frontier = self.last_stable_frontier.as_ref().unwrap();
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

    fn compute_stable_frontier(&mut self) {
        let resolver = self.resolver.as_ref().unwrap();
        // An unseen member has timestamp zero, and must block commitment.
        let stable = (0..resolver.len())
            .map(|i| {
                self.lamport_values
                    .get(&ReplicaIdx(i))
                    .copied()
                    .unwrap_or(0)
            })
            .min()
            .unwrap_or(0);
        assert!(
            stable >= self.last_stable_timestamp,
            "Stable timestamp regressed"
        );
        self.last_stable_timestamp = stable;
        let frontier = self.last_stable_frontier.as_mut().unwrap();
        for (&idx, trace) in &mut self.pending {
            while let Some((&seq, &timestamp)) = trace.first_key_value() {
                if timestamp > stable {
                    break;
                }
                frontier.set_by_idx(idx, seq);
                trace.pop_first();
            }
        }
    }

    pub fn get_stable_frontier(&self) -> Version {
        self.last_stable_frontier
            .clone()
            .unwrap_or_else(|| Version::new(ReplicaIdx(0), Resolver::default()))
    }

    pub fn set_committed(&mut self, version: &Version) {
        assert_eq!(self.last_stable_frontier.as_ref(), Some(version));
        self.committed = Some(CausalFrontier::from(version));
    }

    pub fn last_committed(&self) -> Option<&CausalFrontier> {
        self.committed.as_ref()
    }

    pub fn update_resolver(&mut self, resolver: Resolver) {
        if let Some(old) = &self.resolver {
            if old == &resolver {
                return;
            }
            assert_eq!(
                old.len(),
                resolver.len(),
                "Lamport commitment requires fixed membership"
            );
            let remap = |idx| {
                resolver
                    .get(old.resolve(idx).expect("Unknown old index"))
                    .expect("Lamport commitment membership changed")
            };
            let mut frontier = Version::new(ReplicaIdx(0), resolver.clone());
            for (idx, seq) in self.last_stable_frontier.as_ref().unwrap().iter() {
                frontier.set_by_idx(remap(idx), seq);
            }
            if let Some(cut) = &self.committed {
                let mut new_cut = CausalFrontier::new(resolver.clone());
                for (idx, seq) in cut.iter() {
                    new_cut.set_by_idx(remap(idx), seq);
                }
                self.committed = Some(new_cut);
            }
            self.lamport_values = std::mem::take(&mut self.lamport_values)
                .into_iter()
                .map(|(idx, value)| (remap(idx), value))
                .collect();
            self.pending = std::mem::take(&mut self.pending)
                .into_iter()
                .map(|(idx, trace)| (remap(idx), trace))
                .collect();
            self.last_stable_frontier = Some(frontier);
        } else {
            assert!(
                !resolver.is_empty(),
                "Commitment requires nonempty membership"
            );
            self.last_stable_frontier = Some(Version::new(ReplicaIdx(0), resolver.clone()));
        }
        self.resolver = Some(resolver);
    }

    pub fn get_resolver(&self) -> Option<&Resolver> {
        self.resolver.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{broadcast::internalizer::Interner, replica::ReplicaId};

    fn resolver(names: &[&str]) -> Resolver {
        let mut interner = Interner::new();
        for name in names {
            interner.intern(&ReplicaId::from(*name));
        }
        interner.resolver().clone()
    }

    fn version(resolver: &Resolver, origin: usize, seqs: &[usize]) -> Version {
        let mut v = Version::new(ReplicaIdx(origin), resolver.clone());
        for (i, seq) in seqs.iter().enumerate() {
            v.set_by_idx(ReplicaIdx(i), *seq);
        }
        v
    }

    #[test]
    fn frontier_uses_sequences_and_waits_for_unseen_members() {
        let r = resolver(&["A", "B"]);
        let mut tracker = LamportTracker::default();
        tracker.update_timestamp(&version(&r, 0, &[1, 0]));
        tracker.update_timestamp(&version(&r, 0, &[2, 0]));
        assert!(
            tracker
                .ready_to_commit(&[ReplicaIdx(0), ReplicaIdx(1)])
                .is_empty()
        );
        tracker.update_timestamp(&version(&r, 1, &[2, 1]));
        // Minimum latest timestamp is 2; B's first event has timestamp 3.
        let frontier = tracker.get_stable_frontier();
        assert_eq!(frontier.seq_by_idx(ReplicaIdx(0)), 2);
        assert_eq!(frontier.seq_by_idx(ReplicaIdx(1)), 0);
        tracker.set_committed(&frontier);
        assert!(
            tracker
                .ready_to_commit(&[ReplicaIdx(0), ReplicaIdx(1)])
                .is_empty()
        );
    }

    #[test]
    fn resolver_reordering_preserves_pending_and_committed_sequences() {
        let ab = resolver(&["A", "B"]);
        let ba = resolver(&["B", "A"]);
        let mut tracker = LamportTracker::default();
        tracker.update_timestamp(&version(&ab, 0, &[1, 0]));
        tracker.update_timestamp(&version(&ab, 1, &[1, 1]));
        tracker.set_committed(&tracker.get_stable_frontier());
        tracker.update_resolver(ba.clone());
        assert_eq!(
            tracker.last_committed().unwrap().seq_by_idx(ReplicaIdx(1)),
            1
        );
        // A's second event, now at index 1, releases B's pending first event.
        tracker.update_timestamp(&version(&ba, 1, &[1, 2]));
        let frontier = tracker.get_stable_frontier();
        assert_eq!(frontier.seq_by_idx(ReplicaIdx(0)), 1);
        assert_eq!(frontier.seq_by_idx(ReplicaIdx(1)), 1);
    }
}
