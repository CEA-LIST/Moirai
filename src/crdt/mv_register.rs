use crate::clocks::vector_clock::VectorClock;
use crate::protocol::tcsb::POLog;
use crate::protocol::utils::{Incrementable, Keyable};
use crate::protocol::{event::Message, event::OpEvent, pure_crdt::PureCRDT};
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Debug)]
pub enum Op<V> {
    Clear,
    Write(V),
}

impl<V> PureCRDT for Op<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = Vec<V>;

    fn r<K: Keyable + Clone + std::fmt::Debug, C: Incrementable<C> + Clone + std::fmt::Debug>(
        event: &OpEvent<K, C, Self>,
        _: &POLog<K, C, Self>,
    ) -> bool {
        matches!(event.op, Op::Clear)
    }

    fn r_zero<K, C>(old_event: &OpEvent<K, C, Self>, new_event: &OpEvent<K, C, Self>) -> bool
    where
        K: Keyable + Clone + std::fmt::Debug,
        C: Incrementable<C> + Clone + std::fmt::Debug,
    {
        old_event.metadata.vc < new_event.metadata.vc
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
        let mut vec = Self::Value::new();
        for op in &state.0 {
            if let Op::Write(v) = op {
                vec.push(v.clone());
            }
        }
        for message in state.1.values() {
            if let Message::Op(Op::Write(v)) = message {
                vec.push(v.clone());
            }
        }
        vec
    }
}

#[cfg(test)]
mod tests {
    use crate::{crdt::mv_register::Op, protocol::event::Message, protocol::tcsb::Tcsb};

    #[test_log::test]
    fn simple_mv_register() {
        let mut tscb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
        let mut tscb_b = Tcsb::<&str, u64, Op<&str>>::new("b");

        let event = tscb_a.tc_bcast(Message::Op(Op::Write("a")));
        tscb_b.tc_deliver(event);

        assert_eq!(tscb_b.state.0.len(), 1);

        let event = tscb_b.tc_bcast(Message::Op(Op::Write("b")));
        tscb_a.tc_deliver(event);

        assert_eq!(tscb_a.state.0.len(), 1);

        let result = vec!["b"];
        assert_eq!(tscb_a.eval(), result);
        assert_eq!(tscb_a.eval(), tscb_b.eval());
    }

    #[test_log::test]
    fn concurrent_mv_register() {
        let mut tscb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
        let mut tscb_b = Tcsb::<&str, u64, Op<&str>>::new("b");

        let event = tscb_a.tc_bcast(Message::Op(Op::Write("c")));
        tscb_b.tc_deliver(event);

        assert_eq!(tscb_a.eval(), vec!["c"]);
        assert_eq!(tscb_b.eval(), vec!["c"]);

        let event = tscb_b.tc_bcast(Message::Op(Op::Write("d")));
        tscb_a.tc_deliver(event);

        assert_eq!(tscb_a.eval(), vec!["d"]);
        assert_eq!(tscb_b.eval(), vec!["d"]);

        let event_a = tscb_a.tc_bcast(Message::Op(Op::Write("a")));
        let event_b = tscb_b.tc_bcast(Message::Op(Op::Write("b")));
        tscb_b.tc_deliver(event_a);
        tscb_a.tc_deliver(event_b);

        let result = vec!["b", "a"];
        assert_eq!(tscb_a.eval(), result);
        assert_eq!(tscb_a.eval(), tscb_b.eval());
    }
}
