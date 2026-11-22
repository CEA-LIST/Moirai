use std::{cmp::Ordering, collections::BTreeSet, fmt::Debug, range::Range};

use crate::{
    clock::version_vector::{Seq, Version}, crdt::replicated_data_type::ReplicatedDataType, event::{Event, id::EventId, tagged_op::TaggedOp}, replica::ReplicaIdx, state::{po_log::POLog, unstable_state::{IsUnstableCore, IsUnstableDelivery, IsUnstablePrune}},
};

pub type BTreeLog<R> = POLog<R, BTreeSet<TaggedOp<<R as ReplicatedDataType>::Update>>>;

impl<O> PartialEq for TaggedOp<O> {
    fn eq(&self, other: &Self) -> bool {
        self.tag() == other.tag()
    }
}

impl<O> Eq for TaggedOp<O> {}

impl<O> PartialOrd for TaggedOp<O> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<O> Ord for TaggedOp<O> {
    fn cmp(&self, other: &Self) -> Ordering {
        let id = self.id();
        let other_id = other.id();
        self.lamport()
            .cmp(other.lamport())
            .then_with(|| id.origin_id().cmp(other_id.origin_id()))
            .then_with(|| id.seq().cmp(&other_id.seq()))
            .then_with(|| id.disambiguator().cmp(&other_id.disambiguator()))
    }
}

impl<O> IsUnstableCore<O> for BTreeSet<TaggedOp<O>>
where
    O: Debug + Clone,
{
    fn append(&mut self, event: Event<O>) {
        self.insert(TaggedOp::from(&event));
    }

    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.iter().find(|to| to.id() == event_id)
    }

    /// # Complexity
    /// O(n) where n is the number of events in the unstable state.
    fn predecessors(&self, version: &Version) -> Vec<&TaggedOp<O>> {
        self.iter()
            .filter(|to| to.id().is_predecessor_of(version))
            .collect()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        BTreeSet::iter(self)
    }

    fn replica_events<'a>(
        &'a self,
        replica_idx: ReplicaIdx,
        range: Range<Seq>,
    ) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.iter()
            .filter(move |to| to.id().idx() == replica_idx && range.contains(&to.id().seq()))
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<O> IsUnstablePrune<O> for BTreeSet<TaggedOp<O>>
where
    O: Debug + Clone,
{
    fn remove(&mut self, event_id: &EventId) {
        self.retain(|to| to.id() != event_id);
    }

    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T) {
        BTreeSet::retain(self, predicate);
    }

    fn clear(&mut self) {
        BTreeSet::clear(self);
    }
}

impl<O> IsUnstableDelivery<O> for BTreeSet<TaggedOp<O>>
where
    O: Debug + Clone,
{
    fn delivery_order(&self, event_id: &EventId) -> Option<usize> {
        self.iter().position(|to| to.id() == event_id)
    }
}
