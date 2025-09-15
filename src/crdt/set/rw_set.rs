use std::{collections::HashSet, fmt::Debug, hash::Hash};

use crate::protocol::{
    crdt::pure_crdt::{PureCRDT, RedundancyRelation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::{stable_state::IsStableState, unstable_state::IsUnstableState},
};

#[derive(Clone, Debug, PartialEq)]
pub enum RWSet<V> {
    Add(V),
    Remove(V),
    Clear,
}

// TODO: maybe two hashsets is better?
impl<V> IsStableState<RWSet<V>> for (HashSet<V>, Vec<RWSet<V>>)
where
    V: Clone + Eq + Hash + Debug,
{
    fn len(&self) -> usize {
        self.0.len() + self.1.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty() && self.1.is_empty()
    }

    fn apply(&mut self, value: RWSet<V>) {
        match value {
            RWSet::Add(v) => {
                self.0.insert(v);
            }
            RWSet::Remove(_) => {
                self.1.push(value);
            }
            RWSet::Clear => unreachable!(),
        }
    }

    fn clear(&mut self) {
        self.0.clear();
        self.1.clear();
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<RWSet<V>>,
        tagged_op: &TaggedOp<RWSet<V>>,
    ) {
        // TODO: reuse the rdnt
        match tagged_op.op() {
            RWSet::Add(v) => {
                self.0.remove(v);
                self.1.retain(|o| matches!(o, RWSet::Remove(v2) if v != v2));
            }
            RWSet::Remove(v) => {
                self.0.remove(v);
                self.1.retain(|o| matches!(o, RWSet::Remove(v2) if v != v2));
            }
            RWSet::Clear => {
                self.0.clear();
            }
        }
    }
}

impl<V> PureCRDT for RWSet<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = HashSet<V>;
    type StableState = (HashSet<V>, Vec<RWSet<V>>);

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(new_tagged_op.op(), RWSet::Clear)
    }

    fn redundant_by_when_redundant(
        old_op: &Self,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        !is_conc
            && match (old_op, new_tagged_op.op()) {
                (RWSet::Add(v1), RWSet::Add(v2))
                | (RWSet::Add(v1), RWSet::Remove(v2))
                | (RWSet::Remove(v1), RWSet::Add(v2))
                | (RWSet::Remove(v1), RWSet::Remove(v2)) => v1 == v2,
                (_, RWSet::Clear) => true,
                (RWSet::Clear, _) => unreachable!(),
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

    fn stabilize<'a>(
        tagged_op: &TaggedOp<Self>,
        stable: &mut Self::StableState,
        unstable: &mut impl IsUnstableState<Self>,
    ) {
        // Two cases:
        // 1. The tagged_op is a 'add'
        // ...in this case: remove this 'add' if there exists another op with the same arg in unstable
        // ...and remove any stable 'remove' with the same argument.
        // 2. The tagged_op is a 'remove'
        // ...in this case: remove this 'remove' unless there exists a 'add' with the same arg in unstable
        match tagged_op.op() {
            RWSet::Add(v) => {
                if unstable.iter().any(|t| {
                    matches!(t.op(), RWSet::Add(v2) | RWSet::Remove(v2) if v == v2)
                        && t.id() != tagged_op.id()
                }) {
                    unstable.remove(tagged_op.id());
                }
                stable
                    .1
                    .retain(|o| !matches!(o, RWSet::Remove(v2) if v == v2));
            }
            RWSet::Remove(v) => {
                if unstable.iter().all(|t| {
                    matches!(t.op(), RWSet::Remove(v2) | RWSet::Add(v2) if v != v2)
                        || t.id() == tagged_op.id()
                }) {
                    unstable.remove(tagged_op.id());
                }
            }
            RWSet::Clear => unreachable!(),
        }
    }

    fn eval(stable: &Self::StableState, unstable: &impl IsUnstableState<Self>) -> Self::Value {
        let mut set = stable.0.clone();
        let mut removed = HashSet::new();

        for o in unstable.iter().map(|t| t.op()) {
            match o {
                RWSet::Add(v) => {
                    if !stable
                        .1
                        .iter()
                        .any(|o| matches!(o, RWSet::Remove(v2) if v == v2))
                        && !removed.contains(v)
                    {
                        set.insert(v.clone());
                    }
                }
                RWSet::Remove(v) => {
                    set.remove(v);
                    removed.insert(v);
                }
                RWSet::Clear => unreachable!(),
            }
        }

        set
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{
        crdt::{set::rw_set::RWSet, test_util::twins},
        protocol::{
            replica::IsReplica,
            state::{log::IsLogTest, unstable_state::IsUnstableState},
        },
    };

    #[test]
    fn clear_rw_set() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event = replica_a.send(RWSet::Add("a"));
        replica_b.receive(event);

        let event = replica_b.send(RWSet::Add("b"));
        replica_a.receive(event);

        let event = replica_a.send(RWSet::Clear);
        replica_b.receive(event);

        let result = HashSet::new();
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    // Note: Following tests are reproduction of same simulation in Figure 18 of the “Pure Operation-Based CRDTs” paper.

    #[test]
    fn case_one() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();
        let event = replica_a.send(RWSet::Add("a"));
        replica_b.receive(event);

        let result = HashSet::from(["a"]);
        assert_eq!(replica_b.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn case_two() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event_a = replica_a.send(RWSet::Add("a"));
        let event_b = replica_b.send(RWSet::Add("a"));

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        assert_eq!(replica_a.state().unstable().len(), 1);
        assert_eq!(replica_b.state().unstable().len(), 1);

        let result = HashSet::from(["a"]);
        assert_eq!(replica_b.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn case_three() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event_a = replica_a.send(RWSet::Add("a"));
        let event_b = replica_b.send(RWSet::Remove("a"));
        let event_a_2 = replica_a.send(RWSet::Remove("a"));

        replica_b.receive(event_a);
        replica_a.receive(event_b);
        replica_b.receive(event_a_2);

        let result = HashSet::from([]);
        assert_eq!(replica_b.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn case_five() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();
        let event = replica_a.send(RWSet::Remove("a"));
        replica_b.receive(event);

        assert_eq!(replica_a.state().unstable().len(), 1);
        assert_eq!(replica_b.state().unstable().len(), 0);

        let result = HashSet::from([]);
        assert_eq!(replica_b.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn concurrent_add_remove() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event_b = replica_b.send(RWSet::Remove("a"));
        let event_a = replica_a.send(RWSet::Add("a"));
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = HashSet::from([]);
        assert_eq!(replica_b.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn concurrent_add_remove_add() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();
        let event_a = replica_a.send(RWSet::Add("a"));
        replica_b.receive(event_a);

        assert_eq!(replica_b.query(), HashSet::from(["a"]));
        assert_eq!(replica_a.query(), replica_b.query());

        let event_b = replica_b.send(RWSet::Remove("a"));
        let event_a = replica_a.send(RWSet::Add("a"));
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        assert_eq!(replica_b.query(), HashSet::from([]));
        assert_eq!(replica_a.query(), replica_b.query());

        let event_a = replica_a.send(RWSet::Add("a"));
        replica_b.receive(event_a);

        assert_eq!(replica_b.query(), HashSet::from(["a"]));
        assert_eq!(replica_a.query(), HashSet::from(["a"]));
    }

    //     #[cfg(feature = "utils")]
    //     #[test]
    //     fn convergence_check() {
    //         use crate::{
    //             protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
    //         };

    //         convergence_checker::<EventGraph<RWSet<&str>>>(
    //             &[RWSet::Add("a"), RWSet::Remove("a"), RWSet::Clear],
    //             HashSet::new(),
    //             HashSet::eq,
    //         );
    //     }

    //     #[cfg(feature = "op_weaver")]
    //     #[test]
    //     fn op_weaver_rw_set() {
    //         use crate::{
    //             protocol::event_graph::EventGraph,
    //             utils::op_weaver::{op_weaver, EventGraphConfig},
    //         };

    //         let ops = vec![
    //             RWSet::Add("a"),
    //             RWSet::Add("b"),
    //             RWSet::Add("c"),
    //             RWSet::Remove("a"),
    //             RWSet::Remove("b"),
    //             RWSet::Remove("c"),
    //             RWSet::Clear,
    //         ];

    //         let config = EventGraphConfig {
    //             name: "rw_set",
    //             num_replicas: 8,
    //             num_operations: 10_000,
    //             operations: &ops,
    //             final_sync: true,
    //             churn_rate: 0.3,
    //             reachability: None,
    //             compare: |a: &HashSet<&str>, b: &HashSet<&str>| a == b,
    //             record_results: true,
    //             seed: None,
    //             witness_graph: false,
    //             concurrency_score: false,
    //         };

    //         op_weaver::<EventGraph<RWSet<&str>>>(config);
    //     }
}
