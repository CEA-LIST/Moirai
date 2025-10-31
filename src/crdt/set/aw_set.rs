use std::{fmt::Debug, hash::Hash};

use crate::{
    protocol::{
        crdt::{
            eval::Eval,
            pure_crdt::PureCRDT,
            query::{Contains, QueryOperation, Read},
            redundancy::RedundancyRelation,
        },
        event::{tag::Tag, tagged_op::TaggedOp},
        state::{stable_state::IsStableState, unstable_state::IsUnstableState},
    },
    HashSet,
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
    fn is_default(&self) -> bool {
        self.is_empty()
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
}

impl<V> Eval<Read<<Self as PureCRDT>::Value>> for AWSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &<AWSet<V> as PureCRDT>::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Read<<Self as PureCRDT>::Value> as QueryOperation>::Response {
        let mut set = stable.clone();
        for o in unstable.iter() {
            if let AWSet::Add(v) = o.op() {
                set.insert(v.clone());
            }
        }
        set
    }
}

impl<V> Eval<Contains<V>> for AWSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    fn execute_query(
        q: Contains<V>,
        stable: &<AWSet<V> as PureCRDT>::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Contains<V> as QueryOperation>::Response {
        stable.contains(&q.0)
            || unstable.iter().any(|o| {
                if let AWSet::Add(v) = o.op() {
                    v == &q.0
                } else {
                    false
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{set::aw_set::AWSet, test_util::bootstrap_n},
        protocol::{
            broadcast::tcsb::Tcsb,
            crdt::query::{Contains, Read},
            replica::IsReplica,
            state::po_log::VecLog,
        },
        set_from_slice, HashSet,
    };

    #[test]
    fn simple_aw_set() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event = replica_a.send(AWSet::Add("a")).unwrap();
        replica_b.receive(event);

        assert_eq!(set_from_slice(&["a"]), replica_a.query(Read::new()));
        assert_eq!(set_from_slice(&["a"]), replica_b.query(Read::new()));

        let event = replica_b.send(AWSet::Add("b")).unwrap();
        replica_a.receive(event);

        let result = set_from_slice(&["b", "a"]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);

        let event = replica_a.send(AWSet::Remove("a")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(AWSet::Add("c")).unwrap();
        replica_a.receive(event);

        let result = set_from_slice(&["b", "c"]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
        assert_eq!(true, replica_a.query(Contains("b")));
        assert_eq!(false, replica_a.query(Contains("a")));
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

        let result = set_from_slice(&["b", "c"]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    #[test]
    fn clear_aw_set() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event = replica_a.send(AWSet::Add("a")).unwrap();
        replica_b.receive(event);

        // assert_eq!(replica_b.state().stable().len(), 1);

        let event = replica_b.send(AWSet::Add("b")).unwrap();
        replica_a.receive(event);

        // assert_eq!(replica_a.state().stable().len(), 2);

        let event = replica_a.send(AWSet::Clear).unwrap();
        replica_b.receive(event);

        let result = HashSet::default();
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    #[test]
    fn concurrent_aw_set() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event = replica_a.send(AWSet::Add("a")).unwrap();
        replica_b.receive(event);

        // assert_eq!(replica_b.state().stable().len(), 1);

        let event = replica_b.send(AWSet::Add("b")).unwrap();
        replica_a.receive(event);

        // assert_eq!(replica_a.state().stable().len(), 2);

        let event_a = replica_a.send(AWSet::Add("a")).unwrap();
        let event_b = replica_b.send(AWSet::Remove("a")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = set_from_slice(&["a", "b"]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    #[test]
    fn concurrent_add_aw_set() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event = replica_a.send(AWSet::Add("c")).unwrap();
        replica_b.receive(event);

        // assert_eq!(replica_b.state().stable().len(), 1);

        let event = replica_b.send(AWSet::Add("b")).unwrap();
        replica_a.receive(event);

        // assert_eq!(replica_a.state().stable().len(), 2);

        let event_a = replica_a.send(AWSet::Add("a")).unwrap();
        let event_b = replica_b.send(AWSet::Add("a")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = set_from_slice(&["a", "c", "b"]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
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

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["a"]));
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["a"]));
    }

    #[test]
    fn concurrent_add_aw_set_3() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event_a_1 = replica_a.send(AWSet::Add("a")).unwrap();
        let event_a_2 = replica_a.send(AWSet::Remove("a")).unwrap();
        let event_b_1 = replica_b.send(AWSet::Add("a")).unwrap();
        let event_b_2 = replica_b.send(AWSet::Remove("a")).unwrap();

        replica_a.receive(event_b_1);
        replica_b.receive(event_a_1);
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_2);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&[]));
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&[]));
    }

    #[test]
    fn concurrent_add_aw_set_4() {
        let mut replicas = bootstrap_n::<VecLog<AWSet<&str>>, Tcsb<AWSet<&str>>>(2);
        let (replica_a, replica_b) = replicas.split_at_mut(1);
        let replica_a = &mut replica_a[0];
        let replica_b = &mut replica_b[0];

        let event_a_1 = replica_a.send(AWSet::Add("a")).unwrap();
        let event_a_2 = replica_a.send(AWSet::Remove("b")).unwrap();
        let event_b_1 = replica_b.send(AWSet::Add("b")).unwrap();
        let event_b_2 = replica_b.send(AWSet::Remove("a")).unwrap();

        replica_a.receive(event_b_1);
        replica_b.receive(event_a_1);
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_2);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["a", "b"]));
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["a", "b"]));
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

        // Génération de 20 000 opérations : 10 000 Add et 10 000 Remove
        let mut ops_vec = Vec::with_capacity(20_000);

        // 10 000 opérations Add (valeurs de 1 à 10 000)
        for i in 1..=10_000 {
            ops_vec.push(AWSet::Add(i));
        }

        // 10 000 opérations Remove (valeurs de 1 à 10 000)
        for i in 1..=10_000 {
            ops_vec.push(AWSet::Remove(i));
        }

        let ops = OpConfig::Uniform(&ops_vec);

        // One replica is inaccessible to every other replica
        // let reachability = Some(vec![
        //     vec![true, true, true, true, true, true, true, false],
        //     vec![true, true, true, true, true, true, true, false],
        //     vec![true, true, true, true, true, true, true, false],
        //     vec![true, true, true, true, true, true, true, false],
        //     vec![true, true, true, true, true, true, true, false],
        //     vec![true, true, true, true, true, true, true, false],
        //     vec![true, true, true, true, true, true, true, false],
        //     vec![false, false, false, false, false, false, false, false],
        // ]);

        let run = RunConfig::new(0.4, 16, 1_000_000, None, None);
        let runs = vec![run.clone(); 1];

        let config =
            FuzzerConfig::<VecLog<AWSet<i32>>>::new("aw_set", runs, ops, true, |a, b| a == b, None);

        fuzzer::<VecLog<AWSet<i32>>>(config);
    }
}
