use std::{collections::HashSet, fmt::Debug, hash::Hash};

use crate::protocol::{
    crdt::pure_crdt::{PureCRDT, RedundancyRelation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::{stable_state::IsStableState, unstable_state::IsUnstableState},
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

    fn eval(stable: &Self::StableState, unstable: &impl IsUnstableState<Self>) -> Self::Value {
        let mut set = stable.clone();
        for o in unstable.iter() {
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
        crdt::{set::aw_set::AWSet, test_util::bootstrap_n},
        protocol::{
            broadcast::tcsb::Tcsb,
            replica::IsReplica,
            state::{log::IsLogTest, po_log::VecLog, stable_state::IsStableState},
        },
    };

    #[test]
    fn simple_aw_set() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event = replica_a.send(AWSet::Add("a")).unwrap();
        replica_b.receive(event);

        assert_eq!(HashSet::from(["a"]), replica_a.query());
        assert_eq!(HashSet::from(["a"]), replica_b.query());

        let event = replica_b.send(AWSet::Add("b")).unwrap();
        replica_a.receive(event);

        let result = HashSet::from(["b", "a"]);
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_b.query(), result);

        let event = replica_a.send(AWSet::Remove("a")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(AWSet::Add("c")).unwrap();
        replica_a.receive(event);

        let result = HashSet::from(["b", "c"]);
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_b.query(), result);
    }

    #[test]
    fn complex_aw_set() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event = replica_a.send(AWSet::Add("b")).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(AWSet::Add("a")).unwrap();
        replica_b.receive(event);

        let event_a = replica_a.send(AWSet::Remove("a")).unwrap();
        let event_b = replica_b.send(AWSet::Add("c")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = HashSet::from(["b", "c"]);
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn clear_aw_set() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event = replica_a.send(AWSet::Add("a")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_b.state().stable().len(), 1);

        let event = replica_b.send(AWSet::Add("b")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.state().stable().len(), 2);

        let event = replica_a.send(AWSet::Clear).unwrap();
        replica_b.receive(event);

        let result = HashSet::new();
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn concurrent_aw_set() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event = replica_a.send(AWSet::Add("a")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_b.state().stable().len(), 1);

        let event = replica_b.send(AWSet::Add("b")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.state().stable().len(), 2);

        let event_a = replica_a.send(AWSet::Add("a")).unwrap();
        let event_b = replica_b.send(AWSet::Remove("a")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = HashSet::from(["a", "b"]);
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn concurrent_add_aw_set() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event = replica_a.send(AWSet::Add("c")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_b.state().stable().len(), 1);

        let event = replica_b.send(AWSet::Add("b")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.state().stable().len(), 2);

        let event_a = replica_a.send(AWSet::Add("a")).unwrap();
        let event_b = replica_b.send(AWSet::Add("a")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = HashSet::from(["a", "c", "b"]);
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn concurrent_add_aw_set_2() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event_a = replica_a.send(AWSet::Remove("a")).unwrap();
        let event_b = replica_b.send(AWSet::Add("a")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(), HashSet::from(["a"]));
        assert_eq!(replica_b.query(), replica_a.query());
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_aw_set() {
        use crate::{
            // crdt::test_util::init_tracing,
            fuzz::{
                config::{FuzzerConfig, OpConfig, RunConfig},
                fuzzer,
            },
            protocol::state::po_log::VecLog,
        };

        // init_tracing();

        let ops = OpConfig::Uniform(&[
            AWSet::Add(1),
            AWSet::Add(2),
            AWSet::Add(3),
            AWSet::Add(4),
            AWSet::Remove(1),
            AWSet::Remove(2),
            AWSet::Remove(3),
            AWSet::Remove(4),
            AWSet::Clear,
        ]);

        let run = RunConfig::new(0.4, 8, 1_000, None, None);
        let runs = vec![run.clone(); 1];

        let config =
            FuzzerConfig::<VecLog<AWSet<i32>>>::new("aw_set", runs, ops, true, |a, b| a == b, None);

        fuzzer::<VecLog<AWSet<i32>>>(config);
    }
}
