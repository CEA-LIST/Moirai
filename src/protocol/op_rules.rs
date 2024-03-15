use serde::Serialize;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::{Add, AddAssign};

use super::event::Event;

pub trait OpRules: Clone + Debug {
    type Value: Clone + Debug + Serialize + Default;

    fn obsolete<
        K: PartialOrd + Hash + Eq + Clone + Debug,
        C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    >(
        is_obsolete: &Event<K, C, Self>,
        other: &Event<K, C, Self>,
    ) -> bool; // Checks if the operation is obsolete.
    fn eval<
        K: PartialOrd + Hash + Eq + Clone + Debug,
        C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    >(
        unstable_events: &[Event<K, C, Self>],
        stable_events: &[Self],
    ) -> Self::Value; // Evaluates the state of the CRDT.
}
