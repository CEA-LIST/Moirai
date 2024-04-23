use crate::clocks::vector_clock::VectorClock;
use crate::protocol::event::{Message, OpEvent};
use crate::protocol::pure_crdt::PureCRDT;
use crate::protocol::tcsb::POLog;
use crate::protocol::utils::{Incrementable, Keyable};
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Debug)]
pub enum Op<V> {
    Add(V),
    Remove(V),
    Clear,
}

impl<V> PureCRDT for Op<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = HashSet<V>;

    fn r<K: Keyable + Clone + std::fmt::Debug, C: Incrementable<C> + Clone + std::fmt::Debug>(
        event: &OpEvent<K, C, Self>,
        _: &POLog<K, C, Self>,
    ) -> bool {
        matches!(event.op, Op::Clear) || matches!(event.op, Op::Remove(_))
    }

    fn r_zero<K, C>(old_event: &OpEvent<K, C, Self>, new_event: &OpEvent<K, C, Self>) -> bool
    where
        K: Keyable + Clone + std::fmt::Debug,
        C: Incrementable<C> + Clone + std::fmt::Debug,
    {
        old_event.metadata.vc < new_event.metadata.vc
            && (matches!(new_event.op, Op::Clear)
                || match (&old_event.op, &new_event.op) {
                    (Op::Add(v1), Op::Add(v2))
                    | (Op::Remove(v1), Op::Remove(v2))
                    | (Op::Add(v1), Op::Remove(v2))
                    | (Op::Remove(v1), Op::Add(v2)) => v1 == v2,
                    _ => false,
                })
    }

    fn r_one<
        K: Keyable + Clone + std::fmt::Debug,
        C: Incrementable<C> + Clone + std::fmt::Debug,
    >(
        old_event: &OpEvent<K, C, Self>,
        new_event: &OpEvent<K, C, Self>,
    ) -> bool {
        Self::r_zero(new_event, old_event)
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
        let mut set = Self::Value::new();
        for op in &state.0 {
            if let Op::Add(v) = op {
                set.insert(v.clone());
            }
        }
        for message in state.1.values() {
            if let Message::Op(Op::Add(v)) = &message {
                set.insert(v.clone());
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{
        crdt::aw_set::Op,
        protocol::{event::Message, tcsb::Tcsb},
    };

    #[test_log::test]
    fn simple_aw_set() {
        let mut tscb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
        let mut tscb_b = Tcsb::<&str, u64, Op<&str>>::new("b");

        let event = tscb_a.tc_bcast(Message::Op(Op::Add("a")));
        tscb_b.tc_deliver(event);

        assert_eq!(tscb_b.state.0.len(), 1);

        let event = tscb_b.tc_bcast(Message::Op(Op::Add("b")));
        tscb_a.tc_deliver(event);

        assert_eq!(tscb_a.state.0.len(), 2);

        let event = tscb_a.tc_bcast(Message::Op(Op::Remove("a")));
        tscb_b.tc_deliver(event);

        let event = tscb_b.tc_bcast(Message::Op(Op::Add("c")));
        tscb_a.tc_deliver(event);

        assert_eq!(tscb_a.state.0.len(), 2);
        assert_eq!(tscb_b.state.0.len(), 1);

        let result = HashSet::from(["b", "c"]);
        assert_eq!(tscb_a.eval(), result);
        assert_eq!(tscb_a.eval(), tscb_b.eval());
    }

    #[test_log::test]
    fn clear_aw_set() {
        let mut tscb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
        let mut tscb_b = Tcsb::<&str, u64, Op<&str>>::new("b");

        let event = tscb_a.tc_bcast(Message::Op(Op::Add("a")));
        tscb_b.tc_deliver(event);

        assert_eq!(tscb_b.state.0.len(), 1);

        let event = tscb_b.tc_bcast(Message::Op(Op::Add("b")));
        tscb_a.tc_deliver(event);

        assert_eq!(tscb_a.state.0.len(), 2);

        let event = tscb_a.tc_bcast(Message::Op(Op::Clear));
        tscb_b.tc_deliver(event);

        let result = HashSet::new();
        assert_eq!(tscb_a.eval(), result);
        assert_eq!(tscb_a.eval(), tscb_b.eval());
    }

    #[test_log::test]
    fn concurrent_aw_set() {
        let mut tscb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
        let mut tscb_b = Tcsb::<&str, u64, Op<&str>>::new("b");

        let event = tscb_a.tc_bcast(Message::Op(Op::Add("a")));
        tscb_b.tc_deliver(event);

        assert_eq!(tscb_b.state.0.len(), 1);

        let event = tscb_b.tc_bcast(Message::Op(Op::Add("b")));
        tscb_a.tc_deliver(event);

        assert_eq!(tscb_a.state.0.len(), 2);

        let event_a = tscb_a.tc_bcast(Message::Op(Op::Add("a")));
        let event_b = tscb_b.tc_bcast(Message::Op(Op::Remove("a")));
        tscb_a.tc_deliver(event_b);
        tscb_b.tc_deliver(event_a);

        let result = HashSet::from(["a", "b"]);
        assert_eq!(tscb_a.eval(), result);
        assert_eq!(tscb_a.eval(), tscb_b.eval());
    }

    #[test_log::test]
    fn concurrent_add_aw_set() {
        let mut tscb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
        let mut tscb_b = Tcsb::<&str, u64, Op<&str>>::new("b");

        let event = tscb_a.tc_bcast(Message::Op(Op::Add("c")));
        tscb_b.tc_deliver(event);

        assert_eq!(tscb_b.state.0.len(), 1);

        let event = tscb_b.tc_bcast(Message::Op(Op::Add("b")));
        tscb_a.tc_deliver(event);

        assert_eq!(tscb_a.state.0.len(), 2);

        let event_a = tscb_a.tc_bcast(Message::Op(Op::Add("a")));
        let event_b = tscb_b.tc_bcast(Message::Op(Op::Add("a")));
        tscb_a.tc_deliver(event_b);
        tscb_b.tc_deliver(event_a);

        println!("{:?}", tscb_a.state);

        let result = HashSet::from(["a", "c", "b"]);
        assert_eq!(tscb_a.eval(), result);
        assert_eq!(tscb_a.eval(), tscb_b.eval());
    }
}
