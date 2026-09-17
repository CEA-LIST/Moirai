use std::{fmt::Debug, range::Range};

use crate::{
    clock::version_vector::{Seq, Version},
    event::{Event, id::EventId, tagged_op::TaggedOp},
    replica::ReplicaIdx,
    state::unstable_state::{IsUnstableCore, IsUnstablePrune},
    utils::hashmap::HashMap,
};

impl<O> IsUnstableCore<O> for HashMap<EventId, TaggedOp<O>>
where
    O: Debug + Clone,
{
    fn append(&mut self, event: Event<O>) {
        let tagged_op = TaggedOp::from(&event);
        self.insert(tagged_op.id().clone(), tagged_op);
    }

    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.get(event_id)
    }

    fn predecessors(&self, version: &Version) -> Vec<&TaggedOp<O>> {
        self.values()
            .filter(|to| to.id().is_predecessor_of(version))
            .collect()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.values()
    }

    fn replica_events<'a>(
        &'a self,
        replica_idx: ReplicaIdx,
        range: Range<Seq>,
    ) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.iter().filter_map(move |(id, to)| {
            if id.idx() == replica_idx && range.contains(&id.seq()) {
                Some(to)
            } else {
                None
            }
        })
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<O> IsUnstablePrune<O> for HashMap<EventId, TaggedOp<O>>
where
    O: Debug + Clone,
{
    fn remove(&mut self, event_id: &EventId) {
        HashMap::remove(self, event_id);
    }

    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T) {
        HashMap::retain(self, |_, to| predicate(to));
    }

    fn clear(&mut self) {
        HashMap::clear(self);
    }
}
