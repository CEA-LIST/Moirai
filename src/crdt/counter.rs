use std::ops::{Add, AddAssign, SubAssign};

use crate::protocol::{event::Event, metadata::Metadata, pure_crdt::PureCRDT, tcsb::POLog};

pub trait Number = Add + AddAssign + SubAssign + Default + Copy;

#[derive(Clone, Debug)]
pub enum Counter<V: Number> {
    Inc(V),
    Dec(V),
}

impl<V: Number> PureCRDT for Counter<V> {
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

    fn eval(state: &POLog<Self>) -> Self::Value {
        let mut counter = Self::Value::default();
        for op in &state.0 {
            match op {
                Counter::Inc(v) => counter += *v,
                Counter::Dec(v) => counter -= *v,
            }
        }
        for op in state.1.values() {
            match op {
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
