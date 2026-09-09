use std::{fmt::Debug, hash::Hash};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::op_generator::OpGenerator;
use moirai_protocol::{
    crdt::{
        eval::Eval,
        pure_crdt::PureCRDT,
        query::{Contains, QueryOperation, Read},
        redundancy::RedundancyRelation,
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    state::{stable_state::IsStableState, unstable_state::IsUnstableState},
    utils::intern_str::{InternalizeOp, Interner},
};
#[cfg(feature = "fuzz")]
use rand::Rng;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::HashSet;
#[cfg(feature = "fuzz")]
use crate::set::SetConfig;

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
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
                self.1.clear();
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
        // ...in this case: remove this 'add' if there exists another op with the same arg in the
        // ...po-log, whether it is still unstable or already among the stable removes. A stable
        // ...'remove' is only ever retired by an operation that is causally after it, and such an
        // ...operation retires this 'add' too, so an 'add' that finds one has lost for good.
        // 2. The tagged_op is a 'remove'
        // ...in this case: remove this 'remove' unless there exists a 'add' with the same arg in unstable
        match tagged_op.op() {
            RWSet::Add(v) => {
                let removed_while_stable = stable
                    .1
                    .iter()
                    .any(|o| matches!(o, RWSet::Remove(v2) if v == v2));
                if removed_while_stable
                    || unstable.iter().any(|t| {
                        matches!(t.op(), RWSet::Add(v2) | RWSet::Remove(v2) if v == v2)
                            && t.id() != tagged_op.id()
                    })
                {
                    let key = unstable.key_of(tagged_op);
                    unstable.remove_by_key(&key);
                }
            }
            RWSet::Remove(v) => {
                if unstable.iter().all(|t| {
                    matches!(t.op(), RWSet::Remove(v2) | RWSet::Add(v2) if v != v2)
                        || t.id() == tagged_op.id()
                }) {
                    let key = unstable.key_of(tagged_op);
                    unstable.remove_by_key(&key);
                }
            }
            RWSet::Clear => unreachable!(),
        }
    }
}

impl<V> InternalizeOp for RWSet<V> {
    fn internalize(self, _interner: &Interner) -> Self {
        self
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

/// `Contains` answers exactly whether the value is in the set `Read` renders,
/// which is the only specification it has.
///
/// `Read` drops a value that any unstable `Remove` mentions, keeps a value that
/// is among the stable adds, and admits an unstable `Add` only when no stable
/// `Remove` masks it. Written with `&&` binding tighter than `||`, this used to
/// read `stable add and no stable remove, or any unstable add`, so an unstable
/// `Add(v)` answered `true` on its own. That splits two replicas holding the
/// same operations: on one of them a concurrent `Remove(v)` has already
/// stabilized and masks the still unstable `Add(v)`, on the other the `Add(v)`
/// stabilized first and was dropped for having lost to that `Remove(v)`. Both
/// read the empty set and they used to answer `Contains(v)` differently.
impl<V> Eval<Contains<V>> for RWSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    fn execute_query(
        q: Contains<V>,
        stable: &<RWSet<V> as PureCRDT>::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Contains<V> as QueryOperation>::Response {
        !unstable
            .iter()
            .any(|o| matches!(o.op(), RWSet::Remove(v) if v == &q.0))
            && (stable.0.contains(&q.0)
                || (!stable
                    .1
                    .iter()
                    .any(|o| matches!(o, RWSet::Remove(v2) if v2 == &q.0))
                    && unstable.iter().any(|o| {
                        if let RWSet::Add(v) = o.op() {
                            v == &q.0
                        } else {
                            false
                        }
                    })))
    }
}

#[cfg(feature = "fuzz")]
impl OpGenerator for RWSet<String> {
    type Config = SetConfig;

    fn generate(
        rng: &mut impl Rng,
        config: &Self::Config,
        _stable: &<Self as PureCRDT>::StableState,
        _unstable: &impl IsUnstableState<Self>,
    ) -> Self {
        let letters: Vec<String> = (0..config.max_elements).map(|i| format!("{i}")).collect();
        let choice = rand::seq::IteratorRandom::choose(letters.iter(), rng)
            .unwrap()
            .clone();
        if rng.next_u32().is_multiple_of(2) {
            RWSet::Add(choice)
        } else {
            RWSet::Remove(choice)
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{
        crdt::query::{Contains, Get, Read},
        replica::IsReplica,
        state::po_log::VecLog,
    };

    use crate::{
        HashSet,
        map::uw_map::{UWMap, UWMapLog},
        set::rw_set::RWSet,
        utils::{
            membership::{twins, twins_log},
            set_from_slice,
        },
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

        // assert_eq!(replica_a.state().unstable().len(), 1);
        // assert_eq!(replica_b.state().unstable().len(), 1);

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

        // assert_eq!(replica_a.state().unstable().len(), 1);
        // assert_eq!(replica_b.state().unstable().len(), 0);

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

    /// The shortest pair of concurrent edit lists on which two replicas used to
    /// hold different sets for good.
    ///
    /// `a` adds `alpha`; concurrently `b` removes `alpha` and then clears. On
    /// `b` the `Clear` retires the `Remove` that precedes it and the `Add`
    /// arrives afterwards, so `b` reads `["alpha"]`. On `a` the `Add` arrives
    /// first, the `Remove` stabilizes while that `Add` is still unstable and so
    /// is kept in the stable state, and the `Clear` that follows used to clear
    /// only the stable adds: the kept `Remove` went on masking the `Add` and
    /// `a` read `[]`. A `Clear` makes every operation causally below it
    /// redundant, and every stable operation is causally below any operation
    /// that arrives after it, so the stable removes go with the stable adds.
    #[test]
    fn an_add_concurrent_with_a_remove_and_a_clear() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event_a = replica_a.send(RWSet::Add("alpha")).unwrap();
        let remove = replica_b.send(RWSet::Remove("alpha")).unwrap();
        let clear = replica_b.send(RWSet::Clear).unwrap();

        replica_a.receive(remove);
        replica_a.receive(clear);
        replica_b.receive(event_a);

        let result = set_from_slice(&["alpha"]);
        assert_eq!(replica_b.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), result);
    }

    /// The shortest pair of concurrent edits on which two replicas that read
    /// the same set used to answer `Contains` differently.
    ///
    /// `a` adds `alpha`, concurrently `b` removes it, and each delivers the
    /// other's operation. On `a` the `Remove` is causally stable the moment it
    /// arrives, because `b` authored it and `a` now holds it, so it goes to the
    /// stable removes and masks `a`'s own `Add`, which is still unstable
    /// because `b` has not acknowledged it. On `b` it is the `Add` that is
    /// causally stable on arrival, and `stabilize` drops it for having lost to
    /// the `Remove` beside it, so `b` keeps no `Add` at all. Both read the
    /// empty set; `a` used to answer `Contains("alpha")` with `true` on the
    /// strength of that masked unstable `Add` and `b` with `false`.
    #[test]
    fn an_add_concurrent_with_a_remove_answers_contains_the_same_on_both() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let add = replica_a.send(RWSet::Add("alpha")).unwrap();
        let remove = replica_b.send(RWSet::Remove("alpha")).unwrap();
        replica_a.receive(remove);
        replica_b.receive(add);

        let result = set_from_slice(&[]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
        assert_eq!(replica_a.query(Contains("alpha")), false);
        assert_eq!(replica_b.query(Contains("alpha")), false);
    }

    /// Every ordered pair of concurrent operation lists of length at most two
    /// over `{Add, Remove}` on two values plus `Clear`, delivered both ways:
    /// nine hundred and sixty-one pairs, and the two replicas have to read the
    /// same set on every one of them. Thirty-two did not before the `Clear`
    /// above cleared the stable removes.
    #[test]
    fn every_concurrent_pair_of_at_most_two_operations_converges() {
        let mut split = Vec::new();
        let mut disagree = Vec::new();
        for (left, right) in pairs() {
            let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();
            let from_a: Vec<_> = left
                .iter()
                .filter_map(|op| replica_a.send(op.clone()))
                .collect();
            let from_b: Vec<_> = right
                .iter()
                .filter_map(|op| replica_b.send(op.clone()))
                .collect();
            for event in from_b {
                replica_a.receive(event);
            }
            for event in from_a {
                replica_b.receive(event);
            }
            let read_a: HashSet<&str> = replica_a.query(Read::new());
            let read_b: HashSet<&str> = replica_b.query(Read::new());
            if read_a != read_b {
                split.push(format!(
                    "{left:?} against {right:?}: {read_a:?} and {read_b:?}"
                ));
            }
            for v in ["alpha", "beta"] {
                let contains_a: bool = replica_a.query(Contains(v));
                let contains_b: bool = replica_b.query(Contains(v));
                if contains_a != read_a.contains(v) || contains_b != read_b.contains(v) {
                    disagree.push(format!(
                        "{left:?} against {right:?}: Contains({v:?}) is {contains_a} on `a` and {contains_b} on `b`, reads are {read_a:?} and {read_b:?}"
                    ));
                }
            }
        }
        assert!(
            split.is_empty(),
            "{} concurrent pairs do not converge:\n{}",
            split.len(),
            split.join("\n")
        );
        assert!(
            disagree.is_empty(),
            "{} concurrent pairs answer `Contains` against their own `Read`:\n{}",
            disagree.len(),
            disagree.join("\n")
        );
    }

    /// The same enumeration, with a second round of writes on another key so
    /// that the first round becomes causally stable on both replicas at once.
    ///
    /// This is the shape the two-replica enumeration above cannot reach: a
    /// replica only learns that its peer holds an operation from a later
    /// message, and on a bare log every later message on the same value retires
    /// what came before it. Under a map the second round lands on `spacer` and
    /// retires nothing on `k`, so both replicas stabilize the *same* set of
    /// operations on `k`, each in its own delivery order — which is where a
    /// stabilizing `Add` used to drop a stable `Remove` it was merely
    /// concurrent with. Two hundred and forty of these pairs used to split.
    #[test]
    fn every_concurrent_pair_converges_when_both_replicas_stabilize_it() {
        let mut split = Vec::new();
        let mut disagree = Vec::new();
        for (left, right) in pairs() {
            let (mut replica_a, mut replica_b) = twins_log::<UWMapLog<&str, VecLog<RWSet<&str>>>>();
            let from_a: Vec<_> = left
                .iter()
                .filter_map(|op| replica_a.send(UWMap::Update("k", op.clone())))
                .collect();
            let from_b: Vec<_> = right
                .iter()
                .filter_map(|op| replica_b.send(UWMap::Update("k", op.clone())))
                .collect();
            for event in from_b {
                replica_a.receive(event);
            }
            for event in from_a {
                replica_b.receive(event);
            }
            let spacer_a = replica_a
                .send(UWMap::Update("spacer", RWSet::Add("s")))
                .unwrap();
            let spacer_b = replica_b
                .send(UWMap::Update("spacer", RWSet::Add("s")))
                .unwrap();
            replica_a.receive(spacer_b);
            replica_b.receive(spacer_a);
            let read_a: HashSet<&str> = replica_a
                .query(Get::new(&"k", Read::new()))
                .unwrap_or_default();
            let read_b: HashSet<&str> = replica_b
                .query(Get::new(&"k", Read::new()))
                .unwrap_or_default();
            if read_a != read_b {
                split.push(format!(
                    "{left:?} against {right:?}: {read_a:?} and {read_b:?}"
                ));
            }
            for v in ["alpha", "beta"] {
                let contains_a = replica_a
                    .query(Get::new(&"k", Contains(v)))
                    .unwrap_or_default();
                let contains_b = replica_b
                    .query(Get::new(&"k", Contains(v)))
                    .unwrap_or_default();
                if contains_a != read_a.contains(v) || contains_b != read_b.contains(v) {
                    disagree.push(format!(
                        "{left:?} against {right:?}: Contains({v:?}) is {contains_a} on `a` and {contains_b} on `b`, reads are {read_a:?} and {read_b:?}"
                    ));
                }
            }
        }
        assert!(
            split.is_empty(),
            "{} concurrent pairs do not converge once both replicas stabilize them:\n{}",
            split.len(),
            split.join("\n")
        );
        assert!(
            disagree.is_empty(),
            "{} concurrent pairs answer `Contains` against their own `Read` once both replicas stabilize them:\n{}",
            disagree.len(),
            disagree.join("\n")
        );
    }

    /// Every ordered pair of operation lists of length at most two over the
    /// five operations the two enumerations above run.
    fn pairs() -> Vec<(Vec<RWSet<&'static str>>, Vec<RWSet<&'static str>>)> {
        let alphabet = [
            RWSet::Add("alpha"),
            RWSet::Remove("alpha"),
            RWSet::Add("beta"),
            RWSet::Remove("beta"),
            RWSet::Clear,
        ];
        let mut lists: Vec<Vec<RWSet<&str>>> = vec![Vec::new()];
        for one in &alphabet {
            lists.push(vec![one.clone()]);
            for two in &alphabet {
                lists.push(vec![one.clone(), two.clone()]);
            }
        }
        let mut out = Vec::with_capacity(lists.len() * lists.len());
        for left in &lists {
            for right in &lists {
                out.push((left.clone(), right.clone()));
            }
        }
        out
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_rw_set() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run = RunConfig::new(0.4, 8, 1_000, None, None, false, false);
        let runs = vec![run.clone(); 1];

        let config = FuzzerConfig::<VecLog<RWSet<String>>>::new(
            "rw_set",
            runs,
            true,
            |a, b| a == b,
            false,
            None,
        );

        fuzzer::<VecLog<RWSet<String>>>(config);
    }
}
