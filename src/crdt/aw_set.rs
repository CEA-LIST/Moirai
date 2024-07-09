use crate::protocol::event::Event;
use crate::protocol::metadata::Metadata;
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
        event: &Event<K, C, Self>,
        _: &POLog<K, C, Self>,
    ) -> bool {
        matches!(event.op, Op::Clear) || matches!(event.op, Op::Remove(_))
    }

    fn r_zero<K, C>(old_event: &Event<K, C, Self>, new_event: &Event<K, C, Self>) -> bool
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
        old_event: &Event<K, C, Self>,
        new_event: &Event<K, C, Self>,
    ) -> bool {
        Self::r_zero(new_event, old_event)
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
        let mut set = Self::Value::new();
        for n in &state.0 {
            if let Op::Add(v) = &n.op {
                set.insert(v.clone());
            }
        }
        for n in state.1.values() {
            if let Op::Add(v) = &n.op {
                set.insert(v.clone());
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{crdt::aw_set::Op, crdt::test_util::twins};

    #[test_log::test]
    fn simple_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Op<&str>>();

        let event = tcsb_a.tc_bcast(Op::Add("a"));
        tcsb_b.tc_deliver(event);

        assert_eq!(tcsb_b.state.0.len(), 1);

        let event = tcsb_b.tc_bcast(Op::Add("b"));
        tcsb_a.tc_deliver(event);

        assert_eq!(tcsb_a.state.0.len(), 2);

        let event = tcsb_a.tc_bcast(Op::Remove("a"));
        tcsb_b.tc_deliver(event);

        let event = tcsb_b.tc_bcast(Op::Add("c"));
        tcsb_a.tc_deliver(event);

        assert_eq!(tcsb_a.state.0.len(), 2);
        assert_eq!(tcsb_b.state.0.len(), 1);

        let result = HashSet::from(["b", "c"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn clear_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Op<&str>>();

        let event = tcsb_a.tc_bcast(Op::Add("a"));
        tcsb_b.tc_deliver(event);

        assert_eq!(tcsb_b.state.0.len(), 1);

        let event = tcsb_b.tc_bcast(Op::Add("b"));
        tcsb_a.tc_deliver(event);

        assert_eq!(tcsb_a.state.0.len(), 2);

        let event = tcsb_a.tc_bcast(Op::Clear);
        tcsb_b.tc_deliver(event);

        let result = HashSet::new();
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Op<&str>>();

        let event = tcsb_a.tc_bcast(Op::Add("a"));
        tcsb_b.tc_deliver(event);

        assert_eq!(tcsb_b.state.0.len(), 1);

        let event = tcsb_b.tc_bcast(Op::Add("b"));
        tcsb_a.tc_deliver(event);

        assert_eq!(tcsb_a.state.0.len(), 2);

        let event_a = tcsb_a.tc_bcast(Op::Add("a"));
        let event_b = tcsb_b.tc_bcast(Op::Remove("a"));
        tcsb_a.tc_deliver(event_b);
        tcsb_b.tc_deliver(event_a);

        let result = HashSet::from(["a", "b"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_add_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Op<&str>>();

        let event = tcsb_a.tc_bcast(Op::Add("c"));
        tcsb_b.tc_deliver(event);

        assert_eq!(tcsb_b.state.0.len(), 1);

        let event = tcsb_b.tc_bcast(Op::Add("b"));
        tcsb_a.tc_deliver(event);

        assert_eq!(tcsb_a.state.0.len(), 2);

        let event_a = tcsb_a.tc_bcast(Op::Add("a"));
        let event_b = tcsb_b.tc_bcast(Op::Add("a"));
        tcsb_a.tc_deliver(event_b);
        tcsb_b.tc_deliver(event_a);

        let result = HashSet::from(["a", "c", "b"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn test_concurrent_add_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Op<&str>>();

        let event_a = tcsb_a.tc_bcast(Op::Add("a"));
        let event_b = tcsb_b.tc_bcast(Op::Add("a"));
        tcsb_a.tc_deliver(event_b);
        tcsb_b.tc_deliver(event_a);

        let result = HashSet::from(["a"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }
}
