use std::fmt::Debug;

use crate::{
    HashMap,
    clock::version_vector::Version,
    event::{Event, id::EventId, tagged_op::TaggedOp},
    state::unstable_state::{
        IsUnstableCausal, IsUnstableCore, IsUnstableDelivery, IsUnstablePrune,
    },
};

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

    fn predecessors_cloned(&self, version: &Version) -> Vec<TaggedOp<O>>
    where
        O: Clone,
    {
        self.values()
            .filter(|to| to.id().is_predecessor_of(version))
            .cloned()
            .collect()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.values()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<O> IsUnstableCausal<O> for HashMap<EventId, TaggedOp<O>>
where
    O: Debug + Clone,
{
    fn parents(&self, _event_id: &EventId) -> Vec<EventId> {
        unimplemented!()
    }

    fn frontier(&self) -> Vec<TaggedOp<O>> {
        unimplemented!()
    }
}

impl<O> IsUnstableDelivery<O> for HashMap<EventId, TaggedOp<O>>
where
    O: Debug + Clone,
{
    fn delivery_order(&self, _event_id: &EventId) -> Option<usize> {
        unimplemented!()
    }
}
