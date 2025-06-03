use std::{
    fmt::{Debug, Display},
    ops::{Add, AddAssign, SubAssign},
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::{
    clocks::clock::{Clock, Partial},
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT, stable::Stable},
};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
pub enum Counter<V: Add + AddAssign + SubAssign + Default + Copy> {
    Inc(V),
    Dec(V),
}

impl<V> Stable<Counter<V>> for V
where
    V: Add + AddAssign + SubAssign + Default + Copy + Debug + PartialEq,
{
    fn is_default(&self) -> bool {
        V::default() == *self
    }

    fn apply_redundant(
        &mut self,
        _rdnt: fn(&Counter<V>, bool, &Counter<V>) -> bool,
        _op: &Counter<V>,
    ) {
    }

    fn apply(&mut self, value: Counter<V>) {
        match value {
            Counter::Inc(v) => *self += v,
            Counter::Dec(v) => *self -= v,
        }
    }
}

impl<V: Add + AddAssign + SubAssign + Default + Copy + Debug + PartialEq> PureCRDT for Counter<V> {
    type Value = V;
    type Stable = V;
    const R_ZERO: Option<bool> = Some(false);
    const R_ONE: Option<bool> = Some(false);

    fn redundant_itself(_new_op: &Self) -> bool {
        false
    }

    fn redundant_by_when_redundant(_old_op: &Self, _is_conc: bool, _new_op: &Self) -> bool {
        false
    }

    fn redundant_by_when_not_redundant(_old_op: &Self, _is_conc: bool, _new_op: &Self) -> bool {
        false
    }

    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
        let mut counter = *stable;
        for op in unstable.iter() {
            match op {
                Counter::Inc(v) => counter += *v,
                Counter::Dec(v) => counter -= *v,
            }
        }
        counter
    }

    fn stabilize(_metadata: &Clock<Partial>, _state: &mut EventGraph<Self>) {}
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
