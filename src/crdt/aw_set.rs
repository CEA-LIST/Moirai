use std::{collections::HashSet, fmt::Debug, hash::Hash};

use crate::{
    clocks::dependency_clock::DependencyClock,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT, stable::Stable},
};

#[derive(Clone, Debug)]
pub enum AWSet<V> {
    Add(V),
    Remove(V),
    Clear,
}

impl<V> Stable<AWSet<V>> for HashSet<V>
where
    V: Clone + Eq + Hash + Debug,
{
    fn is_default(&self) -> bool {
        HashSet::default() == *self
    }

    fn apply_redundant(&mut self, _rdnt: fn(&AWSet<V>, bool, &AWSet<V>) -> bool, op: &AWSet<V>) {
        match op {
            AWSet::Add(v) => {
                self.remove(v);
            }
            AWSet::Remove(v) => {
                self.remove(v);
            }
            AWSet::Clear => {
                self.clear();
            }
        }
    }

    fn apply(&mut self, value: AWSet<V>) {
        match value {
            AWSet::Add(v) => {
                self.insert(v);
            }
            _ => {}
        }
    }
}

impl<V> PureCRDT for AWSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Value = HashSet<V>;
    type Stable = HashSet<V>;

    fn redundant_itself(new_op: &Self) -> bool {
        matches!(new_op, AWSet::Clear | AWSet::Remove(_))
    }

    fn redundant_by_when_redundant(old_op: &Self, is_conc: bool, new_op: &Self) -> bool {
        !is_conc
            && (matches!(new_op, AWSet::Clear)
                || match (&old_op, &new_op) {
                    (AWSet::Add(v1), AWSet::Add(v2)) | (AWSet::Add(v1), AWSet::Remove(v2)) => {
                        v1 == v2
                    }
                    _ => false,
                })
    }

    fn redundant_by_when_not_redundant(old_op: &Self, is_conc: bool, new_op: &Self) -> bool {
        Self::redundant_by_when_redundant(old_op, is_conc, new_op)
    }

    fn stabilize(_metadata: &DependencyClock, _state: &mut EventGraph<Self>) {}

    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
        let mut set = stable.clone();
        for o in unstable.iter() {
            match o {
                AWSet::Add(v) => {
                    set.insert(v.clone());
                }
                _ => {}
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

        assert_eq!(HashSet::from(["a"]), tcsb_a.eval());
        assert_eq!(HashSet::from(["a"]), tcsb_b.eval());

        let event = tcsb_b.tc_bcast(AWSet::Add("b"));
        tcsb_a.try_deliver(event);

        let event = tcsb_a.tc_bcast(AWSet::Remove("a"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(AWSet::Add("c"));
        tcsb_a.try_deliver(event);

        let result = HashSet::from(["b", "c"]);
        assert_eq!(tcsb_a.eval(), result);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    }

    #[test_log::test]
    fn complex_aw_set() {
        let (mut tcsb_a, mut tcsb_b, _) =
            crate::crdt::test_util::triplet::<EventGraph<AWSet<&str>>>();

        let event = tcsb_a.tc_bcast(AWSet::Add("b"));
        tcsb_b.try_deliver(event);

        let event = tcsb_a.tc_bcast(AWSet::Add("a"));
        tcsb_b.try_deliver(event);

        let event_a = tcsb_a.tc_bcast(AWSet::Remove("a"));
        let event_b = tcsb_b.tc_bcast(AWSet::Add("c"));

        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

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

        assert_eq!(tcsb_a.eval(), HashSet::from(["a"]));
        assert_eq!(tcsb_b.eval(), tcsb_a.eval());
    }

    #[test_log::test]
    fn convergence_checker() {}
}
