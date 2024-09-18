use std::fmt::Debug;
use std::fmt::Display;
use std::{
    borrow::Borrow,
    ops::{Add, AddAssign, SubAssign},
    path::Path,
};

use crate::protocol::{event::Event, metadata::Metadata, po_log::POLog, pure_crdt::PureCRDT};

pub trait Number = Add + AddAssign + SubAssign + Default + Copy;

#[derive(Clone, Debug)]
pub enum Counter<V: Number> {
    Inc(V),
    Dec(V),
}

impl<V: Number + Debug> PureCRDT for Counter<V> {
    type Value = V;

    fn r(_event: &Event<Self>, _state: &POLog<Self>) -> bool {
        false
    }

    fn r_zero(_old_event: &Event<Self>, _new_event: &Event<Self>) -> bool {
        false
    }

    fn r_one(_old_event: &Event<Self>, _new_event: &Event<Self>) -> bool {
        false
    }

    fn stabilize(_metadata: &Metadata, _state: &mut POLog<Self>) {}

    fn eval(state: &POLog<Self>, _: &Path) -> Self::Value {
        let mut counter = Self::Value::default();
        for op in state.iter() {
            match op.borrow() {
                Counter::Inc(v) => counter += *v,
                Counter::Dec(v) => counter -= *v,
            }
        }
        counter
    }
}

#[cfg(test)]
mod tests {
    use crate::crdt::{counter::Counter, test_util::twins};

    #[test_log::test]
    pub fn simple_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Counter<isize>>();

        let event = tcsb_a.tc_bcast(Counter::Dec(5));
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(Counter::Inc(5));
        tcsb_b.tc_deliver(event);

        let result = 0;
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
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
        }
    }
}
