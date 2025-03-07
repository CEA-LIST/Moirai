use crate::protocol::pure_crdt::PureCRDT;
use crate::protocol::{event::Event, metadata::Metadata, po_log::POLog};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fmt::Display;
use std::ops::{Add, AddAssign, SubAssign};

pub trait Number = Add<Output = Self> + AddAssign + SubAssign + Default + Copy;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Counter<V: Number> {
    Inc(V),
    Dec(V),
    Reset,
}

impl<V: Number> Counter<V> {
    fn to_value(&self) -> V {
        match self {
            Counter::Inc(v) => *v,
            Counter::Dec(v) => *v,
            Counter::Reset => panic!("Cannot convert Reset to value"),
        }
    }
}

impl<V: Number + Debug> PureCRDT for Counter<V> {
    type Value = V;

    fn r(new_event: &Event<Self>, _: &Event<Self>) -> bool {
        matches!(new_event.op, Counter::Reset)
    }

    fn r_zero(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        old_event.metadata.clock < new_event.metadata.clock
            && matches!(new_event.op, Counter::Reset)
    }

    fn r_one(_: &Event<Self>, _: &Event<Self>) -> bool {
        false
    }

    fn eval(state: &[Self]) -> Self::Value {
        let mut counter = Self::Value::default();
        for op in state.iter() {
            match op {
                Counter::Inc(v) => counter += *v,
                Counter::Dec(v) => counter -= *v,
                _ => {}
            }
        }
        counter
    }

    fn stabilize(metadata: &Metadata, state: &mut POLog<Self>) {
        if state.stable.is_empty() {
            state.stable.push(Counter::Inc(V::default()));
        }
        if state.stable.get(1).is_none() {
            state.stable.push(Counter::Dec(V::default()));
        }
        let op = state.unstable.remove(metadata).unwrap();
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
}

impl<V> Display for Counter<V>
where
    V: Number + Debug + Display,
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
        protocol::po_log::POLog,
    };

    #[test_log::test]
    pub fn simple_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<POLog<Counter<isize>>>();

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
        let (mut tcsb_a, mut tcsb_b) = twins::<POLog<Counter<isize>>>();

        let event = tcsb_a.tc_bcast(Counter::Dec(5));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(Counter::Inc(5));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(Counter::Inc(5));
        tcsb_a.try_deliver(event);

        let event = tcsb_b.tc_bcast(Counter::Inc(5));
        tcsb_a.try_deliver(event);

        let event = tcsb_a.tc_bcast(Counter::Inc(5));
        tcsb_b.try_deliver(event);

        let result = 15;
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }
}
