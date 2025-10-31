use std::{fmt::Debug, hash::Hash};

#[cfg(feature = "fuzz")]
use rand::RngCore;

#[cfg(feature = "fuzz")]
use crate::fuzz::config::{OpConfig, OpGenerator};
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
    fn is_default(&self) -> bool {
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
}

impl<V> Eval<Read<<Self as PureCRDT>::Value>> for RWSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &<RWSet<V> as PureCRDT>::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Read<<Self as PureCRDT>::Value> as QueryOperation>::Response {
        let mut set = stable.0.clone();
        let mut removed = HashSet::default();

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

impl<V> Eval<Contains<V>> for RWSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    fn execute_query(
        q: Contains<V>,
        stable: &<RWSet<V> as PureCRDT>::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Contains<V> as QueryOperation>::Response {
        let exist = stable.0.contains(&q.0)
            && !stable
                .1
                .iter()
                .any(|o| matches!(o, RWSet::Remove(v2) if v2 == &q.0))
            || unstable.iter().any(|o| {
                if let RWSet::Add(v) = o.op() {
                    v == &q.0
                } else {
                    false
                }
            });
        exist
    }
}

#[cfg(feature = "fuzz")]
impl OpGenerator for RWSet<String> {
    fn generate(
        rng: &mut impl RngCore,
        config: &OpConfig,
        _stable: &<Self as PureCRDT>::StableState,
        _unstable: &impl IsUnstableState<Self>,
    ) -> Self {
        let letters: Vec<String> = (0..config.max_elements).map(|i| format!("{}", i)).collect();
        let choice = rand::seq::IteratorRandom::choose(letters.iter(), rng)
            .unwrap()
            .clone();
        if rng.next_u32() % 2 == 0 {
            RWSet::Add(choice)
        } else {
            RWSet::Remove(choice)
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        crdt::{set::rw_set::RWSet, test_util::twins},
        protocol::{
            crdt::query::{Contains, Read},
            replica::IsReplica,
            state::{log::IsLogTest, unstable_state::IsUnstableState},
        },
        set_from_slice, HashSet,
    };

    #[test]
    fn clear_rw_set() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event = replica_a.send(RWSet::Add("a")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(RWSet::Add("b")).unwrap();
        replica_a.receive(event);

        let event = replica_a.send(RWSet::Clear).unwrap();
        replica_b.receive(event);

        let result = HashSet::default();
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
        assert_eq!(replica_a.query(Contains("a")), false);
        assert_eq!(replica_b.query(Contains("b")), false);
    }

    // Note: Following tests are reproduction of same simulation in Figure 18 of the “Pure Operation-Based CRDTs” paper.

    #[test]
    fn case_one() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();
        let event = replica_a.send(RWSet::Add("a")).unwrap();
        replica_b.receive(event);

        let result = set_from_slice(&["a"]);
        assert_eq!(replica_b.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), result);
    }

    #[test]
    fn case_two() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event_a = replica_a.send(RWSet::Add("a")).unwrap();
        let event_b = replica_b.send(RWSet::Add("a")).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        assert_eq!(replica_a.state().unstable().len(), 1);
        assert_eq!(replica_b.state().unstable().len(), 1);

        let result = set_from_slice(&["a"]);
        assert_eq!(replica_b.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), result);
    }

    #[test]
    fn case_three() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event_a = replica_a.send(RWSet::Add("a")).unwrap();
        let event_b = replica_b.send(RWSet::Remove("a")).unwrap();
        let event_a_2 = replica_a.send(RWSet::Remove("a")).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);
        replica_b.receive(event_a_2);

        let result = set_from_slice(&[]);
        assert_eq!(replica_b.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Contains("a")), false);
    }

    #[test]
    fn case_five() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();
        let event = replica_a.send(RWSet::Remove("a")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.state().unstable().len(), 1);
        assert_eq!(replica_b.state().unstable().len(), 0);

        let result = set_from_slice(&[]);
        assert_eq!(replica_b.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), result);
    }

    #[test]
    fn concurrent_add_remove() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event_b = replica_b.send(RWSet::Remove("a")).unwrap();
        let event_a = replica_a.send(RWSet::Add("a")).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = set_from_slice(&[]);
        assert_eq!(replica_b.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), result);
    }

    #[test]
    fn concurrent_add_remove_add() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();
        let event_a = replica_a.send(RWSet::Add("a")).unwrap();
        replica_b.receive(event_a);

        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["a"]));
        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["a"]));

        let event_b = replica_b.send(RWSet::Remove("a")).unwrap();
        let event_a = replica_a.send(RWSet::Add("a")).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        assert_eq!(replica_b.query(Read::new()), set_from_slice(&[]));
        assert_eq!(replica_a.query(Read::new()), set_from_slice(&[]));

        let event_a = replica_a.send(RWSet::Add("a")).unwrap();
        replica_b.receive(event_a);

        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["a"]));
        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["a"]));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_rw_set() {
        // init_tracing();

        use crate::{
            fuzz::{
                config::{FuzzerConfig, OpConfig, RunConfig},
                fuzzer,
            },
            protocol::state::po_log::VecLog,
        };

        let run = RunConfig::new(0.4, 8, 10_000, None, None);
        let runs = vec![run.clone(); 1];

        let op_config = OpConfig {
            max_elements: 10_000,
        };

        let config = FuzzerConfig::<VecLog<RWSet<String>>>::new(
            "rw_set",
            runs,
            op_config,
            true,
            |a, b| a == b,
            None,
        );

        fuzzer::<VecLog<RWSet<String>>>(config);
    }
}
