use crate::{
    clock::version_vector::{Seq, Version},
    event::{Event, id::EventId, tagged_op::TaggedOp},
    replica::ReplicaIdx,
    state::unstable_state::{IsUnstableCausal, IsUnstableCore, IsUnstablePrune},
    utils::hashmap::HashMap,
};
use std::{fmt::Debug, range::Range};

#[derive(Debug, Clone)]
pub struct EventHistory<O> {
    /// Note: we assume events are inserted in order without gaps.
    /// The store should not be pruned as the index of the vec is used to determine the sequence number of an event.
    store: HashMap<ReplicaIdx, Vec<(TaggedOp<O>, Version)>>,
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
            store: HashMap::default(),
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
        let expected_sequence = self.store.get(&origin).map_or(1, |events| events.len() + 1);

        assert!(
            sequence == expected_sequence,
            "EventHistory requires dense per-origin insertion: expected sequence {expected_sequence}, got {sequence} for replica {origin:?}"
        );

        if let Some((_, previous_version)) =
            self.store.get(&origin).and_then(|events| events.last())
        {
            for (replica, previous_sequence) in previous_version.iter() {
                let sequence = event.version().seq_by_idx(replica);
                assert!(
                    sequence >= previous_sequence,
                    "EventHistory requires componentwise-monotone versions per origin: component {replica:?} regressed from {previous_sequence} to {sequence}"
                );
            }
        }

        let tagged_op = TaggedOp::from(&event);

        self.store
            .entry(origin)
            .or_default()
            .push((tagged_op, event.version().clone()));
    }

    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        let index = event_id.seq().checked_sub(1)?;
        let (tagged_op, _) = self.store.get(&event_id.idx())?.get(index)?;

        (tagged_op.id() == event_id).then_some(tagged_op)
    }

    fn predecessors(&self, version: &Version) -> Vec<&TaggedOp<O>>
    where
        O: Clone,
    {
        self.store
            .iter()
            .flat_map(|(r, events)| {
                let k = version.seq_by_idx(*r);

                events[..k.min(events.len())].iter().map(|(t, _)| t)
            })
            .collect()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.store
            .values()
            .flat_map(|events| events.iter().map(|(t, _)| t))
    }

    fn replica_events<'a>(
        &'a self,
        replica_idx: ReplicaIdx,
        range: Range<Seq>,
    ) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.store[&replica_idx][range].iter().map(|(to, _)| to)
    }

    fn len(&self) -> usize {
        self.store.values().map(Vec::len).sum()
    }

    fn is_empty(&self) -> bool {
        self.store.values().all(Vec::is_empty)
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

        if k == 0 {
            None
        } else {
            self.store.get(&r)?.get(k - 1).map(|e| &e.0)
        }
    }

    /// `next(v, r)` returns the first event `e` from replica `r` such that `e` has `v` in its causal past.
    /// # Complexity
    /// `O(log e.r)` where `e.r` is the set of events from replica `r`
    fn next(&self, event_id: &EventId, r: ReplicaIdx) -> Option<&TaggedOp<O>> {
        // Get all events from replica `r`
        let events = self.store.get(&r)?;

        // If the event has been produced by `r`
        if r == event_id.idx() {
            // seq k is at index k - 1 (e.g., event 1 from `r` is at index 0), so its strict successor k + 1
            // is at index k.
            return events.get(event_id.seq()).map(|(event, _)| event);
        }

        // Return the first event `e` from replica `r` such that `e` has the input event in its causal past
        let index = events
            .partition_point(|(_, version)| version.seq_by_idx(event_id.idx()) < event_id.seq());

        events.get(index).map(|(event, _)| event)
    }

    /// # Complexity
    /// Expected `O(N + E)` time and `O(E)` auxiliary space, where:
    ///
    /// - `N` is the number of replicas in the system that have issued events.
    /// - `E` is the number of events newly added to the observer replica's
    ///   causal past by `observer`.
    fn newly_observed_by(&self, observer: &EventId) -> Vec<&TaggedOp<O>> {
        if self.version(observer).is_none() {
            return Vec::new();
        }

        let previous = self.previous(observer, observer.idx()).map(TaggedOp::id);

        let mut newly_observed = Vec::new();
        for (candidate_origin, events) in &self.store {
            let first = previous
                .and_then(|event| self.strict_past_seq(event, *candidate_origin))
                .unwrap_or(0)
                .min(events.len());
            let last = self
                .strict_past_seq(observer, *candidate_origin)
                .unwrap_or(0)
                .min(events.len());

            if last > first {
                newly_observed.extend(events[first..last].iter().map(|(candidate, _)| candidate));
            }
        }

        newly_observed
    }

    fn version(&self, event_id: &EventId) -> Option<&Version> {
        let index = event_id.seq().checked_sub(1)?;
        let (event, version) = self.store.get(&event_id.idx())?.get(index)?;

        (event.id() == event_id).then_some(version)
    }
}

impl<O> IsUnstablePrune<O> for EventHistory<O>
where
    O: Clone,
{
    fn remove(&mut self, _event_id: &EventId) {}

    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, _predicate: T) {}

    fn clear(&mut self) {}
}
