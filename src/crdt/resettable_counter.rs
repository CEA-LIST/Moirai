use std::{
    fmt::{Debug, Display},
    ops::{Add, AddAssign, SubAssign},
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::{
    clocks::dot::Dot,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT, stable::Stable},
};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
pub enum Counter<V: Add + AddAssign + SubAssign + Default + Copy> {
    Inc(V),
    Dec(V),
    Reset,
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
        _rdnt: fn(&Counter<V>, Option<&Dot>, bool, &Counter<V>, &Dot) -> bool,
        op: &Counter<V>,
        _dot: &Dot,
    ) {
        if let Counter::Reset = op {
            <V as Stable<Counter<V>>>::clear(self);
        }
    }

    fn apply(&mut self, value: Counter<V>) {
        match value {
            Counter::Inc(v) => *self += v,
            Counter::Dec(v) => *self -= v,
            _ => {}
        }
    }
}

impl<V: Add<Output = V> + AddAssign + SubAssign + Default + Copy + Debug + PartialEq> PureCRDT
    for Counter<V>
{
    type Stable = Vec<Self>;
    type Value = V;
    const DISABLE_R_WHEN_NOT_R: bool = true;

    fn redundant_itself(new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        matches!(new_op, Counter::Reset)
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_dot: Option<&Dot>,
        is_conc: bool,
        new_op: &Self,
        _new_dot: &Dot,
    ) -> bool {
        !is_conc && matches!(new_op, Counter::Reset)
    }

    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
        let mut counter = Self::Value::default();
        for op in stable.iter().chain(unstable.iter()) {
            match op {
                Counter::Inc(v) => counter += *v,
                Counter::Dec(v) => counter -= *v,
                _ => {}
            }
        }
        counter
    }
}

impl<V> Display for Counter<V>
where
    V: Add + AddAssign + SubAssign + Default + Copy + Debug + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Counter::Inc(v) => write!(f, "Inc({v})"),
            Counter::Dec(v) => write!(f, "Dec({v})"),
            Counter::Reset => write!(f, "Reset"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{
            resettable_counter::Counter,
            test_util::{triplet, twins},
        },
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
    pub fn stable_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<Counter<isize>>>();

        let event = tcsb_a.tc_bcast(Counter::Dec(1));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(Counter::Inc(2));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(Counter::Inc(3));
        tcsb_a.try_deliver(event);

        let event = tcsb_b.tc_bcast(Counter::Inc(4));
        tcsb_a.try_deliver(event);

        let event = tcsb_a.tc_bcast(Counter::Inc(5));
        tcsb_b.try_deliver(event);

        let result = 13;
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    pub fn concurrent_counter() {
        let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<EventGraph<Counter<isize>>>();

        let event_a_1 = tcsb_a.tc_bcast(Counter::Dec(1));
        tcsb_b.try_deliver(event_a_1.clone());

        let event_b_1 = tcsb_b.tc_bcast(Counter::Reset);
        let event_c_1 = tcsb_c.tc_bcast(Counter::Inc(18));

        tcsb_a.try_deliver(event_b_1.clone());
        tcsb_a.try_deliver(event_c_1.clone());

        tcsb_b.try_deliver(event_c_1);

        tcsb_c.try_deliver(event_b_1);
        tcsb_c.try_deliver(event_a_1);

        let result = 18;
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
        assert_eq!(tcsb_a.eval(), tcsb_c.eval());
    }

    #[cfg(feature = "utils")]
    #[test_log::test]
    fn convergence_check() {
        use crate::{
            protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
        };

        convergence_checker::<EventGraph<Counter<isize>>>(
            &[Counter::Inc(7), Counter::Dec(15), Counter::Reset],
            -8,
            |a, b| a == b,
        );
    }
}
