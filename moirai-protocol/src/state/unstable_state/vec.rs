use std::fmt::Debug;

use crate::{
    clock::version_vector::Version,
    event::{Event, id::EventId, tagged_op::TaggedOp},
    state::unstable_state::{
        IsUnstableCausal, IsUnstableCore, IsUnstableDelivery, IsUnstablePrune,
    },
};

impl<O> IsUnstableCore<O> for Vec<TaggedOp<O>>
where
    O: Debug + Clone,
{
    fn append(&mut self, event: Event<O>) {
        let tagged_op = TaggedOp::from(&event);
        self.push(tagged_op);
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

    fn predecessors_cloned(&self, version: &Version) -> Vec<TaggedOp<O>>
    where
        O: Clone,
    {
        self.iter()
            .filter(|to| to.id().is_predecessor_of(version))
            .cloned()
            .collect()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.as_slice().iter()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<O> IsUnstablePrune<O> for Vec<TaggedOp<O>>
where
    O: Debug + Clone,
{
    fn remove(&mut self, event_id: &EventId) {
        let maybe_pos = self.iter().position(|to| to.id() == event_id);
        if let Some(pos) = maybe_pos {
            self.remove(pos);
        }
    }

    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T) {
        Vec::retain(self, predicate);
    }

    fn clear(&mut self) {
        Vec::clear(self);
    }
}

impl<O> IsUnstableCausal<O> for Vec<TaggedOp<O>>
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

impl<O> IsUnstableDelivery<O> for Vec<TaggedOp<O>>
where
    O: Debug + Clone,
{
    fn delivery_order(&self, event_id: &EventId) -> Option<usize> {
        self.as_slice().iter().position(|to| to.id() == event_id)
    }
}
