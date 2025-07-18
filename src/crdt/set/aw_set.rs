use std::{collections::HashSet, fmt::Debug, hash::Hash};

use crate::{
    clocks::dot::Dot,
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

    fn apply_redundant(
        &mut self,
        _rdnt: fn(&AWSet<V>, Option<&Dot>, bool, &AWSet<V>, &Dot) -> bool,
        op: &AWSet<V>,
        _dot: &Dot,
    ) {
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
        if let AWSet::Add(v) = value {
            self.insert(v);
        }
    }
}

impl<V> PureCRDT for AWSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Value = HashSet<V>;
    type Stable = HashSet<V>;

    fn redundant_itself(new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        matches!(new_op, AWSet::Clear | AWSet::Remove(_))
    }

    fn redundant_by_when_redundant(
        old_op: &Self,
        _old_dot: Option<&Dot>,
        is_conc: bool,
        new_op: &Self,
        _new_dot: &Dot,
    ) -> bool {
        !is_conc
            && (matches!(new_op, AWSet::Clear)
                || match (&old_op, &new_op) {
                    (AWSet::Add(v1), AWSet::Add(v2)) | (AWSet::Add(v1), AWSet::Remove(v2)) => {
                        v1 == v2
                    }
                    _ => false,
                })
    }

    fn redundant_by_when_not_redundant(
        old_op: &Self,
        old_dot: Option<&Dot>,
        is_conc: bool,
        new_op: &Self,
        new_dot: &Dot,
    ) -> bool {
        Self::redundant_by_when_redundant(old_op, old_dot, is_conc, new_op, new_dot)
    }

    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
        let mut set = stable.clone();
        for o in unstable.iter() {
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
        crdt::{set::aw_set::AWSet, test_util::twins},
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

    #[cfg(feature = "utils")]
    #[test_log::test]
    fn convergence_checker() {
        // TODO: Implement a convergence checker for AWSet
    }

    #[cfg(feature = "op_weaver")]
    #[test_log::test]
    fn op_weaver_aw_set() {
        use crate::utils::op_weaver::{op_weaver, EventGraphConfig};

        let mut ops = Vec::with_capacity(10_000);

        // Add operations from 0 to 4999
        for val in 0..5000 {
            ops.push(AWSet::Add(val));
        }

        // Remove operations from 0 to 4999
        for val in 0..5000 {
            ops.push(AWSet::Remove(val));
        }

        let config = EventGraphConfig {
            name: "aw_set",
            num_replicas: 8,
            num_operations: 100_000,
            operations: &ops,
            final_sync: true,
            churn_rate: 0.4,
            reachability: None,
            compare: |a: &HashSet<i32>, b: &HashSet<i32>| a == b,
            record_results: true,
            seed: None,
            witness_graph: false,
            concurrency_score: false,
        };

        op_weaver::<EventGraph<AWSet<i32>>>(config);
    }
}
