use std::{fmt::Debug, hash::Hash};

use crate::{
    HashMap,
    clock::version_vector::Version,
    event::{Event, id::EventId, tagged_op::TaggedOp},
};

pub trait IsUnstableState<O>: Debug {
    type Key;

    fn append(&mut self, event: Event<O>);
    fn key_of(&self, tagged_op: &TaggedOp<O>) -> Self::Key;
    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>>;
    fn get_by_key(&self, key: &Self::Key) -> Option<&TaggedOp<O>>;
    fn remove(&mut self, event_id: &EventId);
    fn remove_by_key(&mut self, key: &Self::Key);
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

pub trait HasDerivedKey: Debug + Clone {
    type DerivedKey: Clone + Eq + Hash + Debug;

    fn derived_key(&self) -> Self::DerivedKey;
}

#[derive(Debug, Clone)]
pub struct DerivedKeyState<O>
where
    O: HasDerivedKey,
{
    ops: HashMap<(EventId, O::DerivedKey), TaggedOp<O>>,
    order: Vec<(EventId, O::DerivedKey)>,
}

impl<O> Default for DerivedKeyState<O>
where
    O: HasDerivedKey,
{
    fn default() -> Self {
        Self {
            ops: HashMap::default(),
            order: Vec::new(),
        }
    }
}

impl<O> IsUnstableState<O> for DerivedKeyState<O>
where
    O: HasDerivedKey,
{
    type Key = (EventId, O::DerivedKey);

    fn append(&mut self, event: Event<O>) {
        let tagged_op = TaggedOp::from(&event);
        let key = self.key_of(&tagged_op);
        self.order.push(key.clone());
        self.ops.insert(key, tagged_op);
    }

    fn key_of(&self, tagged_op: &TaggedOp<O>) -> Self::Key {
        (tagged_op.id().clone(), tagged_op.op().derived_key())
    }

    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.order
            .iter()
            .find(|(id, _)| id == event_id)
            .and_then(|key| self.ops.get(key))
    }

    fn get_by_key(&self, key: &Self::Key) -> Option<&TaggedOp<O>> {
        self.ops.get(key)
    }

    fn remove(&mut self, event_id: &EventId) {
        if let Some(pos) = self.order.iter().position(|(id, _)| id == event_id) {
            let key = self.order.remove(pos);
            self.ops.remove(&key);
        }
    }

    fn remove_by_key(&mut self, key: &Self::Key) {
        self.ops.remove(key);
        if let Some(pos) = self.order.iter().position(|existing| existing == key) {
            self.order.remove(pos);
        }
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.order.iter().filter_map(|key| self.ops.get(key))
    }

    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T) {
        self.order.retain(|key| match self.ops.get(key) {
            Some(tagged_op) if predicate(tagged_op) => true,
            Some(_) => {
                self.ops.remove(key);
                false
            }
            None => false,
        });
    }

    fn len(&self) -> usize {
        self.ops.len()
    }

    fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    fn clear(&mut self) {
        self.ops.clear();
        self.order.clear();
    }

    fn predecessors(&self, version: &Version) -> Vec<TaggedOp<O>> {
        self.iter()
            .filter(|tagged_op| tagged_op.id().is_predecessor_of(version))
            .cloned()
            .collect()
    }

    fn parents(&self, _event_id: &EventId) -> Vec<EventId> {
        unimplemented!()
    }

    fn delivery_order(&self, event_id: &EventId) -> usize {
        self.order
            .iter()
            .position(|(id, _)| id == event_id)
            .unwrap()
    }

    fn frontier(&self) -> Vec<TaggedOp<O>> {
        unimplemented!()
    }
}

impl<O> IsUnstableState<O> for Vec<TaggedOp<O>>
where
    O: Debug + Clone,
{
    type Key = EventId;

    fn append(&mut self, event: Event<O>) {
        let tagged_op = TaggedOp::from(&event);
        self.push(tagged_op);
    }

    fn key_of(&self, tagged_op: &TaggedOp<O>) -> Self::Key {
        tagged_op.id().clone()
    }

    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.iter().find(|to| to.id() == event_id)
    }

    fn get_by_key(&self, key: &Self::Key) -> Option<&TaggedOp<O>> {
        self.get(key)
    }

    fn remove(&mut self, event_id: &EventId) {
        let maybe_pos = self.iter().position(|to| to.id() == event_id);
        if let Some(pos) = maybe_pos {
            self.remove(pos);
        }
    }

    fn remove_by_key(&mut self, key: &Self::Key) {
        IsUnstableState::remove(self, key);
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

    fn frontier(&self) -> Vec<TaggedOp<O>> {
        unimplemented!()
    }
}

impl<O> IsUnstableState<O> for HashMap<EventId, TaggedOp<O>>
where
    O: Debug + Clone,
{
    type Key = EventId;

    fn append(&mut self, event: Event<O>) {
        let tagged_op = TaggedOp::from(&event);
        self.insert(tagged_op.id().clone(), tagged_op);
    }

    fn key_of(&self, tagged_op: &TaggedOp<O>) -> Self::Key {
        tagged_op.id().clone()
    }

    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        self.get(event_id)
    }

    fn get_by_key(&self, key: &Self::Key) -> Option<&TaggedOp<O>> {
        self.get(key)
    }

    fn remove(&mut self, event_id: &EventId) {
        self.remove(event_id);
    }

    fn remove_by_key(&mut self, key: &Self::Key) {
        self.remove(key);
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

    fn frontier(&self) -> Vec<TaggedOp<O>> {
        unimplemented!()
    }
}
