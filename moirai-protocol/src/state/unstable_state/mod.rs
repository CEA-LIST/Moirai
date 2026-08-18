#![allow(clippy::mutable_key_type)]

pub mod event_graph;
pub mod hashmap;
pub mod vec;

use std::fmt::Debug;

use crate::{
    clock::version_vector::Version,
    crdt::policy::Policy,
    event::{Event, id::EventId, tagged_op::TaggedOp},
    utils::hashmap::HashSet,
};

/// Essential services for an unstable state implementation.
/// Core services include appending, retrieving and iterating over tagged operations.
/// As such, it is "grow-only".
pub trait IsUnstableCore<O>: Debug {
    /// Append an event to the unstable state.
    fn append(&mut self, event: Event<O>);
    /// Get a tagged operation from the unstable state by its ID.
    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>>;
    /// Returns a list of references to tagged operations that are predecessors of the given version.
    fn predecessors(&self, version: &Version) -> Vec<&TaggedOp<O>>
    where
        O: Clone;
    /// Returns an iterator over all tagged operations in the unstable state.
    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a;
    /// Returns the number of tagged operations in the unstable state.
    fn len(&self) -> usize;
    /// Checks if the unstable state is empty.
    fn is_empty(&self) -> bool;
}

/// Services for pruning an unstable state.
pub trait IsUnstablePrune<O>: IsUnstableCore<O> {
    /// Remove a tagged operation from the unstable state by its ID.
    fn remove(&mut self, event_id: &EventId);
    /// Retain only the tagged operations that satisfy the given predicate.
    fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T);
    /// Clear all tagged operations from the unstable state.
    fn clear(&mut self);
}

/// Services for advanced causal information retrieval from an unstable state.
pub trait IsUnstableCausal<O>: IsUnstableCore<O> {
    /// Returns a list of event IDs that are the direct parents of the given event ID.
    /// # Note
    /// The direct parents of an event are those events that are immediately causally before it, i.e.,
    /// an event e' is the direct parent of event e if e' < e and there is no event e'' such that e' < e'' < e.
    fn direct_parents(&self, event_id: &EventId) -> Vec<EventId>;
    /// Returns the list of tagged operations that are maximal in the unstable state, i.e.,
    /// those that have no successors in the unstable state.
    fn frontier(&self) -> Vec<TaggedOp<O>>;
    /// Returns the inclusive causal past of `event_id` in deterministic topological order.
    /// The policy only breaks ties between events whose parents have already been emitted.
    fn predecessors_by_id<P: Policy>(&self, event_id: &EventId) -> Vec<EventId>;
    /// Returns the inclusive causal past of `event_id` without imposing a deterministic order.
    fn predecessor_set_by_id(&self, event_id: &EventId) -> HashSet<EventId>;
}

/// Services for retrieving the delivery order of events in an unstable state.
pub trait IsUnstableDelivery<O>: IsUnstableCore<O> {
    /// Returns the index of the given event ID in the delivery order, if it exists.
    fn delivery_order(&self, event_id: &EventId) -> Option<usize>;
}

/// Service for replaying events in an unstable state.
/// Requires the unstable state to support core, causal, and delivery services.
pub trait CausalReplay<O>: IsUnstableCore<O> + IsUnstableCausal<O> + IsUnstableDelivery<O> {}

impl<O, T> CausalReplay<O> for T where
    T: IsUnstableCore<O> + IsUnstableCausal<O> + IsUnstableDelivery<O>
{
}

/// Complete set of services for an unstable state implementation.
pub trait IsUnstableState<O>: IsUnstablePrune<O> + CausalReplay<O> {}

impl<O, T> IsUnstableState<O> for T where T: IsUnstablePrune<O> + CausalReplay<O> {}
