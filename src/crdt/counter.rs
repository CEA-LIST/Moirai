use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    ops::{Add, AddAssign, SubAssign},
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use tsify::Tsify;

use crate::{
    clocks::dependency_clock::DependencyClock,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT},
};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
pub enum Counter<V: Add + AddAssign + SubAssign + Default + Copy> {
    Inc(V),
    Dec(V),
}

impl<V: Add + AddAssign + SubAssign + Default + Copy + Debug + PartialEq> PureCRDT for Counter<V> {
    type Value = V;
    const R_ZERO: Option<bool> = Some(false);
    const R_ONE: Option<bool> = Some(false);

    fn r(_new_op: &Self) -> bool {
        false
    }

    fn r_zero(_old_op: &Self, _order: Option<Ordering>, _new_op: &Self) -> bool {
        false
    }

    fn r_one(_old_op: &Self, _order: Option<Ordering>, _new_op: &Self) -> bool {
        false
    }

    fn eval(state: &[Self]) -> Self::Value {
        let mut counter = Self::Value::default();
        for op in state.iter() {
            match op {
                Counter::Inc(v) => counter += *v,
                Counter::Dec(v) => counter -= *v,
            }
        }
        counter
    }

    fn stabilize(_metadata: &DependencyClock, _state: &mut EventGraph<Self>) {}
}

impl<V> Display for Counter<V>
where
    V: Add + AddAssign + SubAssign + Default + Copy + Debug + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Counter::Inc(v) => write!(f, "Inc({})", v),
            Counter::Dec(v) => write!(f, "Dec({})", v),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{counter::Counter, test_util::twins},
        protocol::event_graph::EventGraph,
    };

    #[test_log::test]
    pub fn simple_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<Counter<isize>>>();

        let event = tcsb_a.tc_bcast(Counter::Dec(5));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(Counter::Inc(5));
        tcsb_b.try_deliver(event);

        let result = 0;
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    pub fn simple_counter_2() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<Counter<isize>>>();

        let event = tcsb_a.tc_bcast(Counter::Dec(5));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(Counter::Inc(5));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(Counter::Inc(5));
        tcsb_b.try_deliver(event);

        let result = 5;
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn convergence_checker() {}
}
