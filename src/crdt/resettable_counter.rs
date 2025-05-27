#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    ops::{Add, AddAssign, SubAssign},
};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::{
    clocks::{dependency_clock::Clock, dot::Dot},
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT, stable::Stable},
};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
pub enum Counter<V: Add + AddAssign + SubAssign + Default + Copy> {
    Inc(V),
    Dec(V),
    Reset,
}

impl<V: Add + AddAssign + SubAssign + Default + Copy> Counter<V> {
    fn to_value(&self) -> V {
        match self {
            Counter::Inc(v) => *v,
            Counter::Dec(v) => *v,
            Counter::Reset => panic!("Cannot convert Reset to value"),
        }
    }
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
        op: &Counter<V>,
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
    const R_ONE: Option<bool> = Some(false);

    fn redundant_itself(new_op: &Self) -> bool {
        matches!(new_op, Counter::Reset)
    }

    fn redundant_by_when_redundant(_old_op: &Self, is_conc: bool, new_op: &Self) -> bool {
        !is_conc && matches!(new_op, Counter::Reset)
    }

    fn redundant_by_when_not_redundant(_old_op: &Self, _is_conc: bool, _new_op: &Self) -> bool {
        false
    }

    fn stabilize(metadata: &Clock, state: &mut EventGraph<Self>) {
        if state.stable.is_empty() {
            state.stable.push(Counter::Inc(V::default()));
        }
        if state.stable.get(1).is_none() {
            state.stable.push(Counter::Dec(V::default()));
        }
        let op = state.remove_dot(&Dot::from(metadata)).unwrap();
        match op {
            Counter::Inc(v) => {
                state.stable[0] = state.stable.first().map_or_else(
                    || Counter::Inc(V::default()),
                    |w| Counter::Inc(w.to_value() + v),
                );
            }
            Counter::Dec(v) => {
                state.stable[1] = state.stable.get(1).map_or_else(
                    || Counter::Dec(V::default()),
                    |w| Counter::Dec(w.to_value() + v),
                );
            }
            _ => {}
        }
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
            Counter::Inc(v) => write!(f, "Inc({})", v),
            Counter::Dec(v) => write!(f, "Dec({})", v),
            Counter::Reset => write!(f, "Reset"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{resettable_counter::Counter, test_util::twins},
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

    #[cfg(feature = "utils")]
    #[test_log::test]
    fn convergence_check() {
        use crate::{
            protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
        };

        convergence_checker::<EventGraph<Counter<isize>>>(
            &[Counter::Inc(7), Counter::Dec(15), Counter::Reset],
            -8,
        );
    }
}
