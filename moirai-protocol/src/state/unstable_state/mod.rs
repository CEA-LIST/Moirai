pub mod event_graph;
pub mod hashmap;
pub mod vec;

use std::{fmt::Debug, hash::Hash};

use crate::{
    HashMap,
    clock::version_vector::Version,
    event::{Event, id::EventId, tagged_op::TaggedOp},
};

pub trait IsUnstableCore<O>: Debug {
    fn append(&mut self, event: Event<O>);
    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>>;
    fn predecessors(&self, version: &Version) -> Vec<TaggedOp<O>>
    where
        O: Clone;
    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
}

pub trait IsUnstableKeyed<O>: Debug {
    type Key;

    fn key_of(&self, tagged_op: &TaggedOp<O>) -> Self::Key;
    fn get_by_key(&self, key: &Self::Key) -> Option<&TaggedOp<O>>;
}

pub trait IsUnstablePrune<O>: IsUnstableCore<O> {
    fn remove(&mut self, event_id: &EventId);
    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T);
    fn clear(&mut self);
}

pub trait IsUnstableCausal<O>: IsUnstableCore<O> {
    fn parents(&self, event_id: &EventId) -> Vec<EventId>;
    fn frontier(&self) -> Vec<TaggedOp<O>>;
}

pub trait IsUnstableDelivery<O>: IsUnstableCore<O> {
    fn delivery_order(&self, event_id: &EventId) -> Option<usize>;
}

pub trait CausalReplay<O>: IsUnstableCore<O> + IsUnstableCausal<O> + IsUnstableDelivery<O> {}

impl<O, T> CausalReplay<O> for T where
    T: IsUnstableCore<O> + IsUnstableCausal<O> + IsUnstableDelivery<O>
{
}

pub trait IsUnstableState<O>: IsUnstablePrune<O> + CausalReplay<O> {}

impl<O, T> IsUnstableState<O> for T where T: IsUnstablePrune<O> + CausalReplay<O> {}

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

#[cfg(feature = "test_utils")]
impl<O> ::deepsize::DeepSizeOf for DerivedKeyState<O>
where
    O: HasDerivedKey + ::deepsize::DeepSizeOf,
    O::DerivedKey: ::deepsize::DeepSizeOf,
{
    fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> usize {
        // DerivedKeyState stores the same logical key in the map and in the
        // delivery order. Count both owned copies, and delegate operation
        // payload accounting to TaggedOp.
        let ops_size = self
            .ops
            .iter()
            .map(|((event_id, derived_key), tagged_op)| {
                event_id.deep_size_of_children(context)
                    + derived_key.deep_size_of_children(context)
                    + tagged_op.deep_size_of_children(context)
            })
            .sum::<usize>();

        let order_size = self
            .order
            .iter()
            .map(|(event_id, derived_key)| {
                event_id.deep_size_of_children(context) + derived_key.deep_size_of_children(context)
            })
            .sum::<usize>();

        ops_size + order_size
    }
}
