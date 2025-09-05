use std::{collections::HashSet, fmt::Debug, hash::Hash};

use crate::protocol::{
    crdt::pure_crdt::{PureCRDT, RedundancyRelation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::stable_state::IsStableState,
};

#[derive(Clone, Debug)]
pub enum AWSet<V> {
    Add(V),
    Remove(V),
    Clear,
}

impl<V> IsStableState<AWSet<V>> for HashSet<V>
where
    V: Clone + Eq + Hash + Debug,
{
    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        HashSet::is_empty(self)
    }

    fn apply(&mut self, value: AWSet<V>) {
        if let AWSet::Add(v) = value {
            self.insert(v);
        }
    }

    fn clear(&mut self) {
        self.clear();
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<AWSet<V>>,
        tagged_op: &TaggedOp<AWSet<V>>,
    ) {
        match tagged_op.op() {
            AWSet::Add(v) | AWSet::Remove(v) => {
                self.remove(v);
            }
            AWSet::Clear => {
                self.clear();
            }
        }
    }
}

impl<V> PureCRDT for AWSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Value = HashSet<V>;
    type StableState = HashSet<V>;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(new_tagged_op.op(), AWSet::Clear | AWSet::Remove(_))
    }

    fn redundant_by_when_redundant(
        old_op: &Self,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        tracing::info!(
            "Checking redundancy: old_op = {:?}, new_op = {:?}",
            old_op,
            new_tagged_op.op()
        );
        tracing::info!("Is concurrent: {}", is_conc);
        !is_conc
            && match (old_op, new_tagged_op.op()) {
                (AWSet::Add(v1), AWSet::Add(v2)) | (AWSet::Add(v1), AWSet::Remove(v2)) => v1 == v2,
                (_, AWSet::Clear) => true,
                (AWSet::Remove(_), _) => unreachable!(),
                (AWSet::Clear, _) => unreachable!(),
            }
    }

    fn redundant_by_when_not_redundant(
        old_op: &Self,
        old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        Self::redundant_by_when_redundant(old_op, old_tag, is_conc, new_tagged_op)
    }

    fn eval<'a>(
        stable: &Self::StableState,
        unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> Self::Value
    where
        V: 'a,
    {
        let mut set = stable.clone();
        for o in unstable {
            if let AWSet::Add(v) = o.op() {
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
        crdt::{
            set::aw_set::AWSet,
            test_util::{triplet, twins},
        },
        protocol::{
            replica::IsReplica,
            state::{log::IsLogTest, stable_state::IsStableState},
        },
    };

    #[test]
    fn simple_aw_set() {
        let (mut replica_a, mut replica_b) = twins::<AWSet<&str>>();

        let event = replica_a.send(AWSet::Add("a"));
        replica_b.receive(event);

        assert_eq!(HashSet::from(["a"]), replica_a.query());
        assert_eq!(HashSet::from(["a"]), replica_b.query());

        let event = replica_b.send(AWSet::Add("b"));
        replica_a.receive(event);

        let result = HashSet::from(["b", "a"]);
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_b.query(), result);

        let event = replica_a.send(AWSet::Remove("a"));
        replica_b.receive(event);

        let event = replica_b.send(AWSet::Add("c"));
        replica_a.receive(event);

        let result = HashSet::from(["b", "c"]);
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_b.query(), result);
    }

    #[test]
    fn complex_aw_set() {
        let (mut replica_a, mut replica_b, _) = triplet::<AWSet<&str>>();

        let event = replica_a.send(AWSet::Add("b"));
        replica_b.receive(event);

        let event = replica_a.send(AWSet::Add("a"));
        replica_b.receive(event);

        let event_a = replica_a.send(AWSet::Remove("a"));
        let event_b = replica_b.send(AWSet::Add("c"));

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = HashSet::from(["b", "c"]);
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn clear_aw_set() {
        let (mut replica_a, mut replica_b) = twins::<AWSet<&str>>();

        let event = replica_a.send(AWSet::Add("a"));
        replica_b.receive(event);

        println!("replica_b state: {:#?}", replica_b.state().stable());
        assert_eq!(replica_b.state().stable().len(), 1);

        let event = replica_b.send(AWSet::Add("b"));
        replica_a.receive(event);

        assert_eq!(replica_a.state().stable().len(), 2);

        let event = replica_a.send(AWSet::Clear);
        replica_b.receive(event);

        let result = HashSet::new();
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn concurrent_aw_set() {
        let (mut replica_a, mut replica_b) = twins::<AWSet<&str>>();

        let event = replica_a.send(AWSet::Add("a"));
        replica_b.receive(event);

        assert_eq!(replica_b.state().stable().len(), 1);

        let event = replica_b.send(AWSet::Add("b"));
        replica_a.receive(event);

        assert_eq!(replica_a.state().stable().len(), 2);

        let event_a = replica_a.send(AWSet::Add("a"));
        let event_b = replica_b.send(AWSet::Remove("a"));
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = HashSet::from(["a", "b"]);
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn concurrent_add_aw_set() {
        let (mut replica_a, mut replica_b) = twins::<AWSet<&str>>();

        let event = replica_a.send(AWSet::Add("c"));
        replica_b.receive(event);

        assert_eq!(replica_b.state().stable().len(), 1);

        let event = replica_b.send(AWSet::Add("b"));
        replica_a.receive(event);

        assert_eq!(replica_a.state().stable().len(), 2);

        let event_a = replica_a.send(AWSet::Add("a"));
        let event_b = replica_b.send(AWSet::Add("a"));
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = HashSet::from(["a", "c", "b"]);
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn concurrent_add_aw_set_2() {
        let (mut replica_a, mut replica_b) = twins::<AWSet<&str>>();

        let event_a = replica_a.send(AWSet::Remove("a"));
        let event_b = replica_b.send(AWSet::Add("a"));

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(), HashSet::from(["a"]));
        assert_eq!(replica_b.query(), replica_a.query());
    }

    //     #[cfg(feature = "utils")]
    //     #[test]
    //     fn convergence_checker() {
    //         // TODO: Implement a convergence checker for AWSet
    //     }

    //     #[cfg(feature = "op_weaver")]
    //     #[test]
    //     fn op_weaver_aw_set() {
    //         use crate::utils::op_weaver::{op_weaver, EventGraphConfig};

    //         let mut ops = Vec::with_capacity(10_000);

    //         // Add operations from 0 to 4999
    //         for val in 0..5000 {
    //             ops.push(AWSet::Add(val));
    //         }

    //         // Remove operations from 0 to 4999
    //         for val in 0..5000 {
    //             ops.push(AWSet::Remove(val));
    //         }

    //         let config = EventGraphConfig {
    //             name: "aw_set",
    //             num_replicas: 8,
    //             num_operations: 100_000,
    //             operations: &ops,
    //             final_sync: true,
    //             churn_rate: 0.8,
    //             reachability: None,
    //             compare: |a: &HashSet<i32>, b: &HashSet<i32>| a == b,
    //             record_results: true,
    //             seed: None,
    //             witness_graph: false,
    //             concurrency_score: false,
    //         };

    //         op_weaver::<EventGraph<AWSet<i32>>>(config);
    //     }
}
