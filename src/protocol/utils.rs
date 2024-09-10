use std::fmt::Display;
use std::hash::Hash;
use std::ops::Add;
use std::ops::AddAssign;

use super::event::Event;
use super::metadata::Metadata;
use super::po_log::POLog;
use super::pure_crdt::PureCRDT;
use super::tcsb::RedundantRelation;

pub trait Incrementable<C> = Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Display;
pub trait Keyable = Ord + PartialOrd + Hash + Eq + Default + Display;

/// Returns the index of the stable and unstable events that are redundant according to the relation
pub(crate) fn prune_redundant_events<O: PureCRDT>(
    event: &Event<O>,
    state: &POLog<O>,
    r_relation: RedundantRelation<O>,
) -> (Vec<usize>, Vec<Metadata>) {
    let mut remove_stable_by_index = Vec::<usize>::new();
    for (i, o) in state.stable.iter().enumerate() {
        let old_event: Event<O> = Event::new(o.as_ref().clone(), Metadata::default());
        if r_relation(&old_event, event) {
            remove_stable_by_index.push(i);
        }
    }
    let mut remove_unstable_by_key = Vec::<Metadata>::new();
    for (m, o) in state.unstable.iter() {
        let old_event: Event<O> = Event::new(o.as_ref().clone(), m.clone());
        if r_relation(&old_event, event) {
            remove_unstable_by_key.push(m.clone());
        }
    }

    (remove_stable_by_index, remove_unstable_by_key)
}

#[deprecated]
pub(crate) fn _prune_redundant_events_mut<O: PureCRDT>(
    event: &Event<O>,
    state: &mut POLog<O>,
    r_relation: RedundantRelation<O>,
) {
    // Keep only the operations that are not made redundant by the new operation
    state.stable.retain(|o| {
        let old_event: Event<O> = Event::new(o.as_ref().clone(), Metadata::default());
        !(r_relation(&old_event, event))
    });
    state.unstable.retain(|m, o| {
        let old_event: Event<O> = Event::new(o.as_ref().clone(), m.clone());
        !(r_relation(&old_event, event))
    });
}
