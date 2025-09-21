use std::{collections::HashMap, fmt::Debug};

use tracing::info;

use crate::protocol::{
    clock::version_vector::Version,
    event::{id::EventId, tagged_op::TaggedOp, Event},
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
    fn predecessors(&self, version: &Version) -> Vec<TaggedOp<O>>;
    fn parents(&self, event_id: &EventId) -> Vec<EventId>;
    fn delivery_order(&self, event_id: &EventId) -> usize;
}

impl<O> IsUnstableState<O> for Vec<TaggedOp<O>>
where
    O: Debug + Clone,
{
    fn append(&mut self, event: Event<O>) {
        let tagged_op = TaggedOp::from(&event);
        self.push(tagged_op);
    }

    // TODO: O(n)
    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.iter().find(|to| to.id() == event_id)
    }

    // TODO: very very slow
    fn remove(&mut self, event_id: &EventId) {
        info!("Removing event: {}", event_id);
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

    fn predecessors(&self, version: &Version) -> Vec<TaggedOp<O>> {
        self.iter()
            .filter(|to| to.id().is_predecessor_of(version))
            .cloned()
            .collect()
    }

    fn parents(&self, _event_id: &EventId) -> Vec<EventId> {
        unimplemented!()
    }

    fn delivery_order(&self, event_id: &EventId) -> usize {
        self.iter().position(|to| to.id() == event_id).unwrap()
    }
}

impl<O> IsUnstableState<O> for HashMap<EventId, TaggedOp<O>>
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
}
