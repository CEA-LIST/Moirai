use std::fmt::Debug;

use crate::{
    protocol::{
        clock::version_vector::Version,
        event::{id::EventId, tagged_op::TaggedOp, Event},
    },
    HashMap,
};

pub trait IsUnstableState<O> {
    fn append(&mut self, event: Event<O>);
    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>>;
    fn remove(&mut self, event_id: &EventId);
    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a;
    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn clear(&mut self);

    // TODO: separate in another trait the graph-like operations

    /// Returns the set of tagged operations that are predecessors of the given version (cut).
    fn predecessors(&self, version: &Version) -> Vec<TaggedOp<O>>;
    /// Returns the parents of the given event ID.
    fn parents(&self, event_id: &EventId) -> Vec<EventId>;
    /// Returns the delivery order index of the given event ID.
    fn delivery_order(&self, event_id: &EventId) -> usize;
    fn frontier(&self) -> Vec<TaggedOp<O>>;
}

impl<O> IsUnstableState<O> for Vec<TaggedOp<O>>
where
    O: Debug + Clone,
{
    /// # Complexity
    /// `O(1)`
    fn append(&mut self, event: Event<O>) {
        let tagged_op = TaggedOp::from(&event);
        self.push(tagged_op);
    }

    /// # Complexity
    /// `O(n)`
    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.iter().find(|to| to.id() == event_id)
    }

    /// # Complexity
    /// `O(n)`
    fn remove(&mut self, event_id: &EventId) {
        let maybe_pos = self.iter().position(|to| to.id() == event_id);
        if let Some(pos) = maybe_pos {
            self.remove(pos);
        }
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.as_slice().iter()
    }

    fn retain<'a, T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T) {
        self.retain(predicate);
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }

    fn clear(&mut self) {
        self.clear();
    }

    /// # Complexity
    /// `O(n)`
    fn predecessors(&self, version: &Version) -> Vec<TaggedOp<O>> {
        self.iter()
            .filter(|to| to.id().is_predecessor_of(version))
            .cloned()
            .collect()
    }

    fn parents(&self, _event_id: &EventId) -> Vec<EventId> {
        unimplemented!()
    }

    /// # Complexity
    /// `O(n)`
    fn delivery_order(&self, event_id: &EventId) -> usize {
        self.iter().position(|to| to.id() == event_id).unwrap()
    }

    fn frontier(&self) -> Vec<TaggedOp<O>> {
        unimplemented!()
    }
}

impl<O> IsUnstableState<O> for HashMap<EventId, TaggedOp<O>>
where
    O: Debug + Clone,
{
    /// # Complexity
    /// `O(1)`
    fn append(&mut self, event: Event<O>) {
        let tagged_op = TaggedOp::from(&event);
        self.insert(tagged_op.id().clone(), tagged_op);
    }

    /// # Complexity
    /// `O(1)`
    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.get(event_id)
    }

    /// # Complexity
    /// `O(1)`
    fn remove(&mut self, event_id: &EventId) {
        self.remove(event_id);
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.values()
    }

    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T) {
        self.retain(|_, to| predicate(to));
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn clear(&mut self) {
        self.clear();
    }

    /// # Complexity
    /// `O(n)`
    fn predecessors(&self, version: &Version) -> Vec<TaggedOp<O>> {
        self.values()
            .filter(|to| to.id().is_predecessor_of(version))
            .cloned()
            .collect()
    }

    fn parents(&self, _event_id: &EventId) -> Vec<EventId> {
        unimplemented!()
    }

    fn delivery_order(&self, _event_id: &EventId) -> usize {
        unimplemented!()
    }

    fn frontier(&self) -> Vec<TaggedOp<O>> {
        unimplemented!()
    }
}
