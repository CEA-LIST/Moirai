use std::{collections::VecDeque, fmt::Debug, range::Range};

use rustc_hash::FxHashMap;

use crate::{
    clock::version_vector::{Seq, Version},
    event::{Event, id::EventId, tagged_op::TaggedOp},
    replica::ReplicaIdx,
    state::unstable_state::{IsUnstableCausal, IsUnstableCore, IsUnstablePrune},
};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct EventHistory<O> {
    /// Events are inserted in order without gaps within each origin. Prefix
    /// compaction preserves their absolute sequence numbers.
    history: FxHashMap<ReplicaIdx, ReplicaTrace<O>>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
struct ReplicaTrace<O> {
    /// Last pruned event's sequence number.
    /// Sequence number `s` is at index `s - pruned_through - 1`.
    pruned_through: Seq,
    /// Last pruned event's causal version, kept as a baseline for observation and append checks.
    boundary_version: Option<Version>,
    /// Retained operations and their causal versions, ordered by increasing sequence number.
    events: VecDeque<(TaggedOp<O>, Version)>,
}

impl<O> Default for ReplicaTrace<O> {
    fn default() -> Self {
        Self {
            pruned_through: 0,
            boundary_version: None,
            events: VecDeque::new(),
        }
    }
}

impl<O> ReplicaTrace<O> {
    fn next_seq(&self) -> Seq {
        self.pruned_through + self.events.len() + 1
    }

    fn get(&self, seq: Seq) -> Option<&(TaggedOp<O>, Version)> {
        let index = seq.checked_sub(self.pruned_through)?.checked_sub(1)?;
        self.events.get(index)
    }

    fn version_with_boundary(&self, seq: Seq) -> Option<&Version> {
        if seq == self.pruned_through {
            self.boundary_version.as_ref()
        } else {
            self.get(seq).map(|(_, version)| version)
        }
    }

    fn last_version(&self) -> Option<&Version> {
        self.events
            .back()
            .map(|(_, version)| version)
            .or(self.boundary_version.as_ref())
    }

    /// Intersect an absolute, zero-based range with the retained suffix.
    fn retained_range(&self, range: Range<Seq>) -> impl Iterator<Item = &(TaggedOp<O>, Version)> {
        let start = range
            .start
            .saturating_sub(self.pruned_through)
            .min(self.events.len());
        let end = range
            .end
            .saturating_sub(self.pruned_through)
            .min(self.events.len());
        self.events.range(start..end)
    }

    /// Unlike retained_range, require the entire requested range to exist.
    /// Silently truncating a vote interval could turn an incomplete `.all()`
    /// check into a false positive.
    fn range(&self, range: Range<Seq>) -> impl Iterator<Item = &(TaggedOp<O>, Version)> {
        assert!(
            range.start <= range.end
                && range.start >= self.pruned_through
                && range.end <= self.pruned_through + self.events.len(),
            "EventHistory range must be fully retained"
        );
        self.retained_range(range)
    }

    fn first_observing(&self, event: &EventId) -> Option<&TaggedOp<O>> {
        let index = self
            .events
            .partition_point(|(_, version)| version.seq_by_idx(event.idx()) < event.seq());
        self.events.get(index).map(|(event, _)| event)
    }

    /// Remove all events with sequence number <= `seq`.
    fn prune_through(&mut self, seq: Seq) {
        let count = seq
            .saturating_sub(self.pruned_through)
            .min(self.events.len());
        if count > 0 {
            let (_, version) = self.events.drain(..count).next_back().unwrap();
            self.boundary_version = Some(version);
            self.pruned_through += count;
            self.maybe_shrink();
        }
    }

    fn maybe_shrink(&mut self) {
        const MIN_CAPACITY: usize = 64;

        let len = self.events.len();
        let capacity = self.events.capacity();

        if capacity > MIN_CAPACITY && len <= capacity / 4 {
            let target = len.saturating_mul(2).max(MIN_CAPACITY);
            self.events.shrink_to(target);
        }
    }
}

impl<O> Default for EventHistory<O>
where
    O: Clone + Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<O> EventHistory<O>
where
    O: Clone + Debug,
{
    fn new() -> Self {
        Self {
            history: FxHashMap::default(),
        }
    }

    /// Returns how many events from `replica` are strictly in `event`'s causal past.
    fn strict_past_seq(&self, event: &EventId, replica: ReplicaIdx) -> Option<Seq> {
        if replica == event.idx() {
            event.seq().checked_sub(1)
        } else {
            Some(self.version(event)?.seq_by_idx(replica))
        }
    }
}

impl<O> IsUnstableCore<O> for EventHistory<O>
where
    O: Clone,
{
    fn append(&mut self, event: Event<O>) {
        let origin = event.id().idx();
        let sequence = event.id().seq();
        let history = self.history.entry(origin).or_default();
        let expected_sequence = history.next_seq();

        assert!(
            sequence == expected_sequence,
            "EventHistory requires dense per-origin insertion: expected sequence {expected_sequence}, got {sequence} for replica {origin:?}"
        );

        if let Some(previous_version) = history.last_version() {
            for (replica, previous_sequence) in previous_version.iter() {
                let sequence = event.version().seq_by_idx(replica);
                assert!(
                    sequence >= previous_sequence,
                    "EventHistory requires componentwise-monotone versions per origin: component {replica:?} regressed from {previous_sequence} to {sequence}"
                );
            }
        }

        let tagged_op = TaggedOp::from(&event);

        history
            .events
            .push_back((tagged_op, event.version().clone()));
    }

    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        let (tagged_op, _) = self.history.get(&event_id.idx())?.get(event_id.seq())?;

        (tagged_op.id() == event_id).then_some(tagged_op)
    }

    fn predecessors(&self, version: &Version) -> Vec<&TaggedOp<O>>
    where
        O: Clone,
    {
        self.history
            .iter()
            .flat_map(|(r, history)| {
                let k = version.seq_by_idx(*r);

                history.retained_range((0..k).into()).map(|(t, _)| t)
            })
            .collect()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.history
            .values()
            .flat_map(|history| history.events.iter().map(|(t, _)| t))
    }

    /// The range uses absolute zero-based coordinates: `0..1` selects sequence
    /// 1, regardless of compaction. Panics if any of the range is not retained.
    fn replica_events<'a>(
        &'a self,
        replica_idx: ReplicaIdx,
        range: Range<Seq>,
    ) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.history[&replica_idx].range(range).map(|(to, _)| to)
    }

    fn len(&self) -> usize {
        self.history
            .values()
            .map(|history| history.events.len())
            .sum()
    }

    fn is_empty(&self) -> bool {
        self.history
            .values()
            .all(|history| history.events.is_empty())
    }
}

impl<O> IsUnstableCausal<O> for EventHistory<O>
where
    O: Clone + Debug,
{
    fn direct_predecessors(&self, _event_id: &EventId) -> Vec<EventId> {
        todo!()
    }

    fn frontier(&self) -> Vec<TaggedOp<O>> {
        todo!()
    }

    /// `previous(v,r)` returns the event `e` from replica `r` such that `v` has `e`
    /// in its causal past and there is no `e'` from `r` such that `e` -> `e'` -> `v`.
    /// # Complexity
    /// `O(1)`
    fn previous(&self, event_id: &EventId, r: ReplicaIdx) -> Option<&TaggedOp<O>> {
        let k = self.strict_past_seq(event_id, r)?;

        self.history.get(&r)?.get(k).map(|e| &e.0)
    }

    /// `next(v, r)` returns the first event `e` from replica `r` such that `e` has `v` in its causal past.
    /// # Complexity
    /// `O(log e.r)` where `e.r` is the set of events from replica `r`
    fn next(&self, event_id: &EventId, r: ReplicaIdx) -> Option<&TaggedOp<O>> {
        // Get all events from replica `r`
        let history = self.history.get(&r)?;

        // If the event has been produced by `r`
        if r == event_id.idx() {
            return history
                .get(event_id.seq().checked_add(1)?)
                .map(|(event, _)| event);
        }

        // Return the first event `e` from replica `r` such that `e` has the input event in its causal past
        history.first_observing(event_id)
    }

    fn newly_observed_by(&self, observer: &EventId) -> Vec<&TaggedOp<O>> {
        if self.version(observer).is_none() {
            return Vec::new();
        }

        let previous = observer.seq().checked_sub(1).and_then(|seq| {
            self.history
                .get(&observer.idx())?
                .version_with_boundary(seq)
        });

        let mut newly_observed = Vec::new();

        for (candidate_origin, history) in &self.history {
            let first = previous
                .map(|version| {
                    if *candidate_origin == observer.idx() {
                        version.origin_seq().saturating_sub(1)
                    } else {
                        version.seq_by_idx(*candidate_origin)
                    }
                })
                .unwrap_or(0);
            let last = self
                .strict_past_seq(observer, *candidate_origin)
                .unwrap_or(0);

            if last > first {
                newly_observed.extend(
                    history
                        .retained_range((first..last).into())
                        .map(|(candidate, _)| candidate),
                );
            }
        }

        newly_observed
    }

    fn version(&self, event_id: &EventId) -> Option<&Version> {
        let (event, version) = self.history.get(&event_id.idx())?.get(event_id.seq())?;

        (event.id() == event_id).then_some(version)
    }
}

impl<O> IsUnstablePrune<O> for EventHistory<O>
where
    O: Clone,
{
    fn remove(&mut self, event_id: &EventId) {
        if let Some(trace) = self.history.get_mut(&event_id.idx()) {
            trace.prune_through(event_id.seq());
        }
    }

    /// Retain a suffix per origin; panic before pruning if any removal would create a gap.
    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T) {
        // Validate every origin before changing any trace.
        let prefixes: Vec<_> = self
            .history
            .values_mut()
            .map(|trace| {
                let mut cutoff = trace.pruned_through;
                let mut kept = false;
                for (event, _) in &trace.events {
                    if predicate(event) {
                        kept = true;
                    } else {
                        assert!(
                            !kept,
                            "EventHistory::retain can only remove per-origin prefixes"
                        );
                        cutoff = event.id().seq();
                    }
                }
                (trace, cutoff)
            })
            .collect();
        for (trace, cutoff) in prefixes {
            trace.prune_through(cutoff);
        }
    }

    fn clear(&mut self) {
        self.history.clear();
    }
}
