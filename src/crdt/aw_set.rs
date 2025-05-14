use std::{cmp::Ordering, collections::HashSet, fmt::Debug, hash::Hash};

use crate::{
    clocks::dependency_clock::DependencyClock,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT},
};

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

    fn redundant_itself(new_op: &Self) -> bool {
        matches!(new_op, AWSet::Clear | AWSet::Remove(_))
    }

    fn redundant_by_when_redundant(old_op: &Self, order: Option<Ordering>, new_op: &Self) -> bool {
        Some(Ordering::Less) == order
            && (matches!(new_op, AWSet::Clear)
                || match (&old_op, &new_op) {
                    (AWSet::Add(v1), AWSet::Add(v2)) | (AWSet::Add(v1), AWSet::Remove(v2)) => {
                        v1 == v2
                    }
                    _ => false,
                })
    }

    fn redundant_by_when_not_redundant(old_op: &Self, order: Option<Ordering>, new_op: &Self) -> bool {
        Self::redundant_by_when_redundant(old_op, order, new_op)
    }

    fn stabilize(_metadata: &DependencyClock, _state: &mut EventGraph<Self>) {}

    fn eval(ops: &[Self]) -> Self::Value {
        let mut set = Self::Value::new();
        for o in ops {
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

    use crate::{
        crdt::{aw_set::AWSet, test_util::twins},
        protocol::event_graph::EventGraph,
    };

    #[test_log::test]
    fn simple_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<AWSet<&str>>>();

        let event = tcsb_a.tc_bcast(AWSet::Add("a"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_b.state.stable.len(), 1);

        let event = tcsb_b.tc_bcast(AWSet::Add("b"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.state.stable.len(), 2);

        let event = tcsb_a.tc_bcast(AWSet::Remove("a"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(AWSet::Add("c"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.state.stable.len(), 2);
        assert_eq!(tcsb_b.state.stable.len(), 1);

        let result = HashSet::from(["b", "c"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn clear_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<AWSet<&str>>>();

        assert_eq!(tcsb_a.view_id(), tcsb_b.view_id());

        let event = tcsb_a.tc_bcast(AWSet::Add("a"));

        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_b.state.stable.len(), 1);

        let event = tcsb_b.tc_bcast(AWSet::Add("b"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.state.stable.len(), 2);

        let event = tcsb_a.tc_bcast(AWSet::Clear);
        tcsb_b.try_deliver(event);

        let result = HashSet::new();
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<AWSet<&str>>>();

        let event = tcsb_a.tc_bcast(AWSet::Add("a"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_b.state.stable.len(), 1);

        let event = tcsb_b.tc_bcast(AWSet::Add("b"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.state.stable.len(), 2);

        let event_a = tcsb_a.tc_bcast(AWSet::Add("a"));
        let event_b = tcsb_b.tc_bcast(AWSet::Remove("a"));
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        let result = HashSet::from(["a", "b"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_add_aw_set() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<AWSet<&str>>>();

        let event = tcsb_a.tc_bcast(AWSet::Add("c"));
        tcsb_b.try_deliver(event);

        assert_eq!(tcsb_b.state.stable.len(), 1);

        let event = tcsb_b.tc_bcast(AWSet::Add("b"));
        tcsb_a.try_deliver(event);

        assert_eq!(tcsb_a.state.stable.len(), 2);

        let event_a = tcsb_a.tc_bcast(AWSet::Add("a"));
        let event_b = tcsb_b.tc_bcast(AWSet::Add("a"));
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        let result = HashSet::from(["a", "c", "b"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn concurrent_add_aw_set_2() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<AWSet<&str>>>();

        let event_a = tcsb_a.tc_bcast(AWSet::Remove("a"));
        let event_b = tcsb_b.tc_bcast(AWSet::Add("a"));

        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        assert_eq!(tcsb_a.eval(), vec!["a"].into_iter().collect());
        assert_eq!(tcsb_b.eval(), tcsb_a.eval());
    }

    #[test_log::test]
    fn convergence_checker() {}
}
