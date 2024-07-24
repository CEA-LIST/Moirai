use crate::protocol::event::Event;
use crate::protocol::metadata::Metadata;
use crate::protocol::pure_crdt::PureCRDT;
use crate::protocol::tcsb::POLog;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Debug)]
pub enum AWSet<V> {
    Add(V),
    Remove(V),
    Clear,
}

impl<V> PureCRDT for AWSet<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = HashSet<V>;

    fn r(event: &Event<Self>, _state: &POLog<Self>) -> bool {
        matches!(event.op, AWSet::Clear) || matches!(event.op, AWSet::Remove(_))
    }

    fn r_zero(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        old_event.metadata.vc < new_event.metadata.vc
            && (matches!(new_event.op, AWSet::Clear)
                || match (&old_event.op, &new_event.op) {
                    (AWSet::Add(v1), AWSet::Add(v2))
                    | (AWSet::Remove(v1), AWSet::Remove(v2))
                    | (AWSet::Add(v1), AWSet::Remove(v2))
                    | (AWSet::Remove(v1), AWSet::Add(v2)) => v1 == v2,
                    _ => false,
                })
    }

    fn r_one(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        Self::r_zero(new_event, old_event)
    }

    fn stabilize(_metadata: &Metadata, _state: &mut POLog<Self>) {}

    fn eval(state: &POLog<Self>) -> Self::Value {
        let mut set = Self::Value::new();
        for o in &state.0 {
            if let AWSet::Add(v) = o {
                set.insert(v.clone());
            }
        }
        for o in state.1.values() {
            if let AWSet::Add(v) = o {
                set.insert(v.clone());
            }
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{crdt::aw_set::AWSet, crdt::test_util::twins};

    #[test_log::test]
    fn simple_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<AWSet<&str>>();

        let event = tcsb_a.tc_bcast(AWSet::Add("a"));
        tcsb_b.tc_deliver(event);

        assert_eq!(tcsb_b.state.0.len(), 1);

        let event = tcsb_b.tc_bcast(AWSet::Add("b"));
        tcsb_a.tc_deliver(event);

        assert_eq!(tcsb_a.state.0.len(), 2);

        let event = tcsb_a.tc_bcast(AWSet::Remove("a"));
        tcsb_b.tc_deliver(event);

        let event = tcsb_b.tc_bcast(AWSet::Add("c"));
        tcsb_a.tc_deliver(event);

        assert_eq!(tcsb_a.state.0.len(), 2);
        assert_eq!(tcsb_b.state.0.len(), 1);

        let result = HashSet::from(["b", "c"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn clear_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<AWSet<&str>>();

        let event = tcsb_a.tc_bcast(AWSet::Add("a"));
        tcsb_b.tc_deliver(event);

        assert_eq!(tcsb_b.state.0.len(), 1);

        let event = tcsb_b.tc_bcast(AWSet::Add("b"));
        tcsb_a.tc_deliver(event);

        assert_eq!(tcsb_a.state.0.len(), 2);

        let event = tcsb_a.tc_bcast(AWSet::Clear);
        tcsb_b.tc_deliver(event);

        let result = HashSet::new();
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<AWSet<&str>>();

        let event = tcsb_a.tc_bcast(AWSet::Add("a"));
        tcsb_b.tc_deliver(event);

        assert_eq!(tcsb_b.state.0.len(), 1);

        let event = tcsb_b.tc_bcast(AWSet::Add("b"));
        tcsb_a.tc_deliver(event);

        assert_eq!(tcsb_a.state.0.len(), 2);

        let event_a = tcsb_a.tc_bcast(AWSet::Add("a"));
        let event_b = tcsb_b.tc_bcast(AWSet::Remove("a"));
        tcsb_a.tc_deliver(event_b);
        tcsb_b.tc_deliver(event_a);

        let result = HashSet::from(["a", "b"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_add_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<AWSet<&str>>();

        let event = tcsb_a.tc_bcast(AWSet::Add("c"));
        tcsb_b.tc_deliver(event);

        assert_eq!(tcsb_b.state.0.len(), 1);

        let event = tcsb_b.tc_bcast(AWSet::Add("b"));
        tcsb_a.tc_deliver(event);

        assert_eq!(tcsb_a.state.0.len(), 2);

        let event_a = tcsb_a.tc_bcast(AWSet::Add("a"));
        let event_b = tcsb_b.tc_bcast(AWSet::Add("a"));
        tcsb_a.tc_deliver(event_b);
        tcsb_b.tc_deliver(event_a);

        let result = HashSet::from(["a", "c", "b"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn test_concurrent_add_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<AWSet<&str>>();

        let event_a = tcsb_a.tc_bcast(AWSet::Add("a"));
        let event_b = tcsb_b.tc_bcast(AWSet::Add("a"));
        tcsb_a.tc_deliver(event_b);
        tcsb_b.tc_deliver(event_a);

        let result = HashSet::from(["a"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }
}
