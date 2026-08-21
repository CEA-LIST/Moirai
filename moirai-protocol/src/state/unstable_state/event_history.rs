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
    // TODO: use a vec rather than a hashmap? But then some vec entries may be empty (because no op from these replicas)
    /// Note: we assume events are inserted in order without gaps.
    /// The store should not be pruned as the index of the vec is used to determine the sequence number of an event.
    pub store: HashMap<ReplicaIdx, Vec<(TaggedOp<O>, Version)>>,
}

impl<O> Default for EventHistory<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O> EventHistory<O> {
    fn new() -> Self {
        Self {
            store: HashMap::default(),
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
    O: Debug + Clone,
{
    fn direct_predecessors(&self, _event_id: &EventId) -> Vec<EventId> {
        todo!()
    }

    fn frontier(&self) -> Vec<TaggedOp<O>> {
        todo!()
    }

    fn previous(&self, version: &Version, r: ReplicaIdx) -> Option<&TaggedOp<O>> {
        let k = version.seq_by_idx(r);

        if k == 0 {
            None
        } else {
            self.store.get(&r)?.get(k - 1).map(|e| &e.0)
        }
    }

    fn next(&self, event_id: &EventId, r: ReplicaIdx) -> Option<&TaggedOp<O>> {
        if event_id.seq() == 0 {
            return None;
        }

        let events = self.store.get(&r)?;

        let index = events
            .partition_point(|(_, version)| version.seq_by_idx(event_id.idx()) < event_id.seq());

        events.get(index).map(|(event, _)| event)
    }

    fn versioned_events<'a>(&'a self) -> impl Iterator<Item = (&'a O, &'a Version)>
    where
        O: 'a,
    {
        self.store
            .values()
            .flat_map(|events| events.iter().map(|(t, v)| (t.op(), v)))
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
