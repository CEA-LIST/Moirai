use crate::protocol::event::Event;
use crate::protocol::metadata::Metadata;
use crate::protocol::pure_crdt::PureCRDT;
use crate::protocol::tcsb::POLog;
use crate::protocol::utils::{Incrementable, Keyable};
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub enum Op {
    Inc,
    Dec,
}

impl PureCRDT for Op {
    type Value = isize;

    fn r<K: Keyable + Clone + std::fmt::Debug, C: Incrementable<C> + Clone + std::fmt::Debug>(
        _event: &Event<K, C, Self>,
        _: &POLog<K, C, Self>,
    ) -> bool {
        false
    }

    fn r_zero<K, C>(_old_event: &Event<K, C, Self>, _new_event: &Event<K, C, Self>) -> bool
    where
        K: Keyable + Clone + std::fmt::Debug,
        C: Incrementable<C> + Clone + std::fmt::Debug,
    {
        false
    }

    fn r_one<
        K: Keyable + Clone + std::fmt::Debug,
        C: Incrementable<C> + Clone + std::fmt::Debug,
    >(
        old_event: &Event<K, C, Self>,
        new_event: &Event<K, C, Self>,
    ) -> bool {
        Self::r_zero(old_event, new_event)
    }

    fn stabilize<
        K: Keyable + Clone + std::fmt::Debug,
        C: Incrementable<C> + Clone + std::fmt::Debug,
    >(
        _: &Metadata<K, C>,
        _: &mut POLog<K, C, Self>,
    ) {
    }

    fn eval<K: Keyable + Clone + std::fmt::Debug, C: Incrementable<C> + Clone + std::fmt::Debug>(
        state: &POLog<K, C, Self>,
    ) -> Self::Value {
        let mut value = Self::Value::default();
        for n in state.0.iter() {
            match &n.op {
                Op::Inc => value += 1,
                Op::Dec => value -= 1,
            }
        }
        for (_, n) in state.1.iter() {
            if let Op::Inc = n.op {
                value += 1;
            }
            if let Op::Dec = n.op {
                value -= 1;
            }
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use crate::crdt::{counter::Op, test_util::twins};

    #[test_log::test]
    pub fn simple_counter() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Op>();

        let event = tcsb_a.tc_bcast(Op::Dec);
        tcsb_b.tc_deliver(event);

        let event = tcsb_a.tc_bcast(Op::Inc);
        tcsb_b.tc_deliver(event);

        let result = 0;
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }
}
