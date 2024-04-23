use crate::clocks::vector_clock::VectorClock;
use crate::protocol::event::{Message, OpEvent};
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
        _event: &OpEvent<K, C, Self>,
        _: &POLog<K, C, Self>,
    ) -> bool {
        false
    }

    fn r_zero<K, C>(_old_event: &OpEvent<K, C, Self>, _new_event: &OpEvent<K, C, Self>) -> bool
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
        old_event: &OpEvent<K, C, Self>,
        new_event: &OpEvent<K, C, Self>,
    ) -> bool {
        Self::r_zero(old_event, new_event)
    }

    fn stabilize<
        K: Keyable + Clone + std::fmt::Debug,
        C: Incrementable<C> + Clone + std::fmt::Debug,
    >(
        _: &VectorClock<K, C>,
        _state: &mut POLog<K, C, Self>,
    ) {
    }

    fn eval<K: Keyable + Clone + std::fmt::Debug, C: Incrementable<C> + Clone + std::fmt::Debug>(
        state: &POLog<K, C, Self>,
    ) -> Self::Value {
        let mut value = Self::Value::default();
        for op in state.0.iter() {
            match op {
                Op::Inc => value += 1,
                Op::Dec => value -= 1,
            }
        }
        for (_, message) in state.1.iter() {
            if let Message::Op(Op::Inc) = message {
                value += 1;
            }
            if let Message::Op(Op::Dec) = message {
                value -= 1;
            }
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::counter::Op,
        protocol::{event::Message, tcsb::Tcsb},
    };

    #[test_log::test]
    pub fn simple_counter() {
        let mut tscb_a = Tcsb::<&str, u64, Op>::new("a");
        let mut tscb_b = Tcsb::<&str, u64, Op>::new("b");

        let event = tscb_a.tc_bcast(Message::Op(Op::Dec));
        tscb_b.tc_deliver(event);

        let event = tscb_a.tc_bcast(Message::Op(Op::Inc));
        tscb_b.tc_deliver(event);

        let result = 0;
        assert_eq!(tscb_a.eval(), result);
        assert_eq!(tscb_a.eval(), tscb_b.eval());
    }
}
