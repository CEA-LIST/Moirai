use std::fmt::Display;
use std::hash::Hash;
use std::ops::Add;
use std::ops::AddAssign;

use super::event::Event;
use super::metadata::Metadata;
use super::pure_crdt::PureCRDT;
use super::tcsb::POLog;
use super::tcsb::RedundantRelation;

pub trait Incrementable<C> = Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Display;
pub trait Keyable = Ord + PartialOrd + Hash + Eq + Default + Display;

pub(crate) fn prune_redundant_events<O: PureCRDT>(
    event: &Event<O>,
    state: &mut POLog<O>,
    r_relation: RedundantRelation<O>,
) {
    // Keep only the operations that are not made redundant by the new operation
    state.0.retain(|o| {
        let old_event: Event<O> = Event::new(o.clone(), Metadata::default());
        !(r_relation(&old_event, event))
    });
    state.1.retain(|m, o| {
        let old_event: Event<O> = Event::new(o.clone(), m.clone());
        !(r_relation(&old_event, event))
    });
}
