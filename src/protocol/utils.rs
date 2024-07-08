use std::fmt::Debug;
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

pub(crate) fn prune_redundant_events<
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT,
>(
    event: &Event<K, C, O>,
    state: &mut POLog<K, C, O>,
    r_relation: RedundantRelation<K, C, O>,
) {
    // Keep only the operations that are not made redundant by the new operation
    state.0.retain(|o| {
        let old_event: Event<K, C, O> = Event::new(o.clone(), Metadata::default());
        !(r_relation(&old_event, event))
    });
    state.1.retain(|m, o| {
        let old_event: Event<K, C, O> = Event::new(o.clone(), m.clone());
        !(r_relation(&old_event, event))
    });
}
