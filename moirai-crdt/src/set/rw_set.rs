/////////////////////////////////////////////////////////////////////////////
//  Copyright (c) 2025, 2026 CEA LIST.
//  
//  This program and the accompanying materials
//  are made available under the terms of the Apache License, Version 2.0
//  which accompanies this distribution, and is available at
//  https://www.apache.org/licenses/LICENSE-2.0
// 
//  SPDX-License-Identifier: Apache-2.0
//  
//  Contributors:
// Léo Olivier -  (CEA LIST) - Initial implementation
//
/////////////////////////////////////////////////////////////////////////////

use std::{convert::Infallible, fmt::Debug, hash::Hash, marker::PhantomData};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    generator::op_generator::OpGenerator, generator::value_generator::ValueGenerator,
};
use moirai_protocol::{
    data_type::{
        query::{Contains, QueryOperation, Read},
        replicated::{
            CausalCompaction, CausalReset, CausalStabilize, Eval, Precondition, RedundancyRelation,
            ReplicatedDataType,
        },
        sequential::SequentialDataType,
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::{IsUnstableCore, IsUnstablePrune},
};
#[cfg(feature = "fuzz")]
use rand::Rng;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::{FxHashSet, set::update::SetUpdate};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RWSet<V>(PhantomData<V>);

impl<V, U> CausalReset<U> for RWSet<V> where V: Debug + Clone + Eq + Hash {}

impl<V, U> CausalStabilize<U> for RWSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    fn on_stabilize(
        tagged_op: &TaggedOp<Self::Update>,
        stable: &mut Self::Stable,
        unstable: &mut (impl IsUnstablePrune<Self::Update> + IsUnstableCore<Self::Update>),
    ) {
        // Two cases:
        // 1. The tagged_op is a 'add'
        // ...in this case: remove this 'add' if there exists another op with the same arg in unstable,
        // ...or a stable 'remove' with the same arg.
        // 2. The tagged_op is a 'remove'
        // ...in this case: remove this 'remove' unless there exists a 'add' with the same arg in unstable
        match tagged_op.op() {
            SetUpdate::Add(v) => {
                // Replicas may stabilize the same concurrent ops in different orders, so the result
                // must not depend on that order. A stable 'remove' still present here was not
                // retired by this 'add' when it was delivered, so the two are concurrent and the
                // 'remove' wins: keep it, and drop the 'add'.
                let removed_while_stable = stable
                    .1
                    .iter()
                    .any(|o| matches!(o, SetUpdate::Remove(v2) if v == v2));
                if removed_while_stable
                    || unstable.iter().any(|t| {
                        matches!(t.op(), SetUpdate::Add(v2) | SetUpdate::Remove(v2) if v == v2)
                            && t.id() != tagged_op.id()
                    })
                {
                    unstable.remove(tagged_op.id());
                }
            }
            SetUpdate::Remove(v) => {
                if unstable.iter().all(|t| {
                    matches!(t.op(), SetUpdate::Remove(v2) | SetUpdate::Add(v2) if v != v2)
                        || t.id() == tagged_op.id()
                }) {
                    unstable.remove(tagged_op.id());
                }
            }
            SetUpdate::Clear => unreachable!(),
        }
    }
}

impl<U, V> CausalCompaction<U> for RWSet<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<SetUpdate<V>>,
{
    fn compact_stable(
        stable: &mut Self::Stable,
        _: RedundancyRelation<Self::Update>,
        new: &TaggedOp<Self::Update>,
    ) {
        match new.op() {
            SetUpdate::Add(v) | SetUpdate::Remove(v) => {
                stable.0.remove(v);
                stable
                    .1
                    .retain(|op| matches!(op, SetUpdate::Remove(other) if other != v));
            }
            SetUpdate::Clear => stable.0.clear(),
        }
    }

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self::Update>,
        _stable: &Self::Stable,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self::Update>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(new_tagged_op.op(), SetUpdate::Clear)
    }

    fn redundant_by_when_redundant(
        old_op: &Self::Update,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        !is_conc
            && match (old_op, new_tagged_op.op()) {
                (SetUpdate::Add(v1), SetUpdate::Add(v2))
                | (SetUpdate::Add(v1), SetUpdate::Remove(v2))
                | (SetUpdate::Remove(v1), SetUpdate::Add(v2))
                | (SetUpdate::Remove(v1), SetUpdate::Remove(v2)) => v1 == v2,
                // There was a mistake in the original paper. 'clear' should not make prior 'remove' redundant,
                // otherwise it would fail to preserve the remove against concurrent add.
                (SetUpdate::Add(_), SetUpdate::Clear) => true,
                (SetUpdate::Remove(_), SetUpdate::Clear) => false,
                (SetUpdate::Clear, _) => unreachable!(),
            }
    }

    fn redundant_by_when_not_redundant(
        old_op: &Self::Update,
        old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        <Self as CausalCompaction<U>>::redundant_by_when_redundant(
            old_op,
            old_tag,
            is_conc,
            new_tagged_op,
        )
    }
}

#[derive(Clone, Debug)]
pub struct MixedSet<V>(FxHashSet<V>, Vec<SetUpdate<V>>);

// TODO: maybe two hashsets is better?
impl<V> SequentialDataType for MixedSet<V>
where
    V: Clone + Eq + Hash + Debug,
{
    type Update = SetUpdate<V>;
    type Rejection = Infallible;

    fn is_default(&self) -> bool {
        self.0.is_empty() && self.1.is_empty()
    }

    fn apply(&mut self, value: &Self::Update) {
        match value {
            SetUpdate::Add(v) => {
                self.0.insert(v.clone());
            }
            SetUpdate::Remove(_) => {
                self.1.push(value.clone());
            }
            SetUpdate::Clear => unreachable!(),
        }
    }

    fn is_enabled(&self, _update: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }
}

#[cfg(feature = "test_utils")]
impl<V> DeepSizeOf for MixedSet<V>
where
    V: DeepSizeOf + Eq + Hash,
{
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.0.deep_size_of_children(context) + self.1.deep_size_of_children(context)
    }
}

impl<V> Default for MixedSet<V> {
    fn default() -> Self {
        Self(FxHashSet::default(), vec![])
    }
}

impl<V> ReplicatedDataType for RWSet<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Stable = MixedSet<V>;
    type Update = SetUpdate<V>;
    type Rejection = Infallible;
}

impl<V, U> Eval<U, Read<FxHashSet<V>>> for RWSet<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<SetUpdate<V>>,
{
    fn execute_query(
        stable: &<Self as ReplicatedDataType>::Stable,
        unstable: &U,
        _q: &Read<FxHashSet<V>>,
    ) -> <Read<FxHashSet<V>> as QueryOperation>::Response {
        let mut set = stable.0.clone();
        let mut removed = FxHashSet::default();

        for o in unstable.iter().map(|t| t.op()) {
            match o {
                SetUpdate::Add(v) => {
                    if !stable
                        .1
                        .iter()
                        .any(|o| matches!(o, SetUpdate::Remove(v2) if v == v2))
                        && !removed.contains(v)
                    {
                        set.insert(v.clone());
                    }
                }
                SetUpdate::Remove(v) => {
                    set.remove(v);
                    removed.insert(v);
                }
                SetUpdate::Clear => unreachable!(),
            }
        }

        set
    }
}

impl<V, U> Eval<U, Contains<V>> for RWSet<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<SetUpdate<V>>,
{
    /// Semantics: there exists an 'add' for the element, and there is no 'remove' for the element.
    fn execute_query(
        stable: &<Self as ReplicatedDataType>::Stable,
        unstable: &U,
        q: &Contains<V>,
    ) -> <Contains<V> as QueryOperation>::Response {
        let stable_removed = stable
            .1
            .iter()
            .any(|o| matches!(o, SetUpdate::Remove(v) if v == &q.0));
        let mut present = stable.0.contains(&q.0) && !stable_removed;
        let mut removed = stable_removed;

        for op in unstable.iter().map(|t| t.op()) {
            match op {
                SetUpdate::Add(v) if v == &q.0 && !removed => {
                    present = true;
                }
                SetUpdate::Remove(v) if v == &q.0 => {
                    present = false;
                    removed = true;
                }
                _ => {}
            }
        }

        present
    }
}

#[cfg(feature = "fuzz")]
impl<V, U> OpGenerator<U> for RWSet<V>
where
    U: IsUnstableCore<SetUpdate<V>>,
    V: ValueGenerator + Clone + Hash + Debug + Eq,
{
    type Config = ();

    fn generate(
        rng: &mut impl Rng,
        _config: &Self::Config,
        _stable: &<Self as ReplicatedDataType>::Stable,
        _unstable: &U,
    ) -> Self::Update {
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            Add,
            Remove,
            Clear,
        }
        let dist = WeightedIndex::new([5, 2, 1]).unwrap();

        let choice = &[Choice::Add, Choice::Remove, Choice::Clear][dist.sample(rng)];
        let value = V::generate(rng, &<V as ValueGenerator>::Config::default());
        match choice {
            Choice::Add => SetUpdate::Add(value),
            Choice::Remove => SetUpdate::Remove(value),
            Choice::Clear => SetUpdate::Clear,
        }
    }
}

impl<V: Debug + Clone + Eq + Hash, U> Precondition<U> for RWSet<V> {
    fn is_enabled(_: &Self::Stable, _: &U, _: &Self::Update) -> Result<(), Infallible> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{
        data_type::query::{Contains, Get, Read},
        replica::IsReplica,
        state::po_log::VecLog,
    };

    use crate::{
        FxHashSet,
        map::{update::MapUpdate, uw_map::UWMapLog},
        set::{rw_set::RWSet, update::SetUpdate},
        utils::{
            bootstrap::{twins, twins_log},
            set_from_slice,
        },
    };

    #[test]
    fn clear_rw_set() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event = replica_a.send(SetUpdate::Add("a")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(SetUpdate::Add("b")).unwrap();
        replica_a.receive(event);

        let event = replica_a.send(SetUpdate::Clear).unwrap();
        replica_b.receive(event);

        let result = FxHashSet::default();
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
        assert!(!replica_a.query(&Contains("a")));
        assert!(!replica_b.query(&Contains("b")));
    }

    #[test]
    fn clear_preserves_prior_remove_against_concurrent_add() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let remove = replica_a.send(SetUpdate::Remove("a")).unwrap();
        let clear = replica_a.send(SetUpdate::Clear).unwrap();
        let add = replica_b.send(SetUpdate::Add("a")).unwrap();

        replica_a.receive(add);
        replica_b.receive(remove);
        replica_b.receive(clear);

        let result = set_from_slice(&[]);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
        assert!(!replica_a.query(&Contains("a")));
        assert!(!replica_b.query(&Contains("a")));
    }

    // Note: Following tests are reproduction of same simulation in Figure 18 of the “Pure Operation-Based CRDTs” paper.

    #[test]
    fn case_one() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();
        let event = replica_a.send(SetUpdate::Add("a")).unwrap();
        replica_b.receive(event);

        let result = set_from_slice(&["a"]);
        assert_eq!(replica_b.query(&Read::new()), result);
        assert_eq!(replica_a.query(&Read::new()), result);
    }

    #[test]
    fn case_two() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event_a = replica_a.send(SetUpdate::Add("a")).unwrap();
        let event_b = replica_b.send(SetUpdate::Add("a")).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);

        // assert_eq!(replica_a.state().unstable().len(), 1);
        // assert_eq!(replica_b.state().unstable().len(), 1);

        let result = set_from_slice(&["a"]);
        assert_eq!(replica_b.query(&Read::new()), result);
        assert_eq!(replica_a.query(&Read::new()), result);
    }

    #[test]
    fn case_three() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event_a = replica_a.send(SetUpdate::Add("a")).unwrap();
        let event_b = replica_b.send(SetUpdate::Remove("a")).unwrap();
        let event_a_2 = replica_a.send(SetUpdate::Remove("a")).unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);
        replica_b.receive(event_a_2);

        let result = set_from_slice(&[]);
        assert_eq!(replica_b.query(&Read::new()), result);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert!(!replica_a.query(&Contains("a")));
    }

    #[test]
    fn case_five() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();
        let event = replica_a.send(SetUpdate::Remove("a")).unwrap();
        replica_b.receive(event);

        // assert_eq!(replica_a.state().unstable().len(), 1);
        // assert_eq!(replica_b.state().unstable().len(), 0);

        let result = set_from_slice(&[]);
        assert_eq!(replica_b.query(&Read::new()), result);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert!(!replica_b.query(&Contains("a")));
        assert!(!replica_a.query(&Contains("a")));
    }

    #[test]
    fn concurrent_add_remove() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event_b = replica_b.send(SetUpdate::Remove("a")).unwrap();
        let event_a = replica_a.send(SetUpdate::Add("a")).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = set_from_slice(&[]);
        assert_eq!(replica_b.query(&Read::new()), result);
        assert_eq!(replica_a.query(&Read::new()), result);
    }

    #[test]
    fn concurrent_add_remove_add() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();
        let event_a = replica_a.send(SetUpdate::Add("a")).unwrap();
        replica_b.receive(event_a);

        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&["a"]));
        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&["a"]));

        let event_b = replica_b.send(SetUpdate::Remove("a")).unwrap();
        let event_a = replica_a.send(SetUpdate::Add("a")).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&[]));
        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&[]));

        let event_a = replica_a.send(SetUpdate::Add("a")).unwrap();
        replica_b.receive(event_a);

        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&["a"]));
        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&["a"]));
    }

    #[test]
    fn concurrent_add_remove_same_element() {
        let (mut replica_a, mut replica_b) = twins::<RWSet<&str>>();

        let event_a_1 = replica_a.send(SetUpdate::Add("a")).unwrap();
        let event_a_2 = replica_a.send(SetUpdate::Remove("a")).unwrap();
        let event_b_1 = replica_b.send(SetUpdate::Add("a")).unwrap();
        let event_b_2 = replica_b.send(SetUpdate::Remove("a")).unwrap();

        replica_a.receive(event_b_1);
        replica_b.receive(event_a_1);
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_2);

        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&[]));
        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&[]));
    }

    /// `a` adds `alpha` while `b` concurrently removes it, under a map key; a
    /// second round of writes on another key makes both operations causally
    /// stable on both replicas, each replica stabilizing them in its own
    /// delivery order. Remove wins over a concurrent add, so both read `{}`.
    /// On the replica that stabilizes the `Add` after the `Remove`, the `Add`
    /// arm of `RWSet::stabilize` drops the stable `Remove` unconditionally,
    /// although the two are concurrent, and the `Add` comes back.
    #[test]
    fn rw_set_concurrent_add_and_remove_converge_after_stabilization() {
        let (mut replica_a, mut replica_b) = twins_log::<UWMapLog<&str, VecLog<RWSet<&str>>>>();

        let add = replica_a
            .send(MapUpdate::Update("k", SetUpdate::Add("alpha")))
            .unwrap();
        let remove = replica_b
            .send(MapUpdate::Update("k", SetUpdate::Remove("alpha")))
            .unwrap();
        replica_a.receive(remove);
        replica_b.receive(add);

        let spacer_a = replica_a
            .send(MapUpdate::Update("spacer", SetUpdate::Add("s")))
            .unwrap();
        let spacer_b = replica_b
            .send(MapUpdate::Update("spacer", SetUpdate::Add("s")))
            .unwrap();
        replica_a.receive(spacer_b);
        replica_b.receive(spacer_a);

        let read_a: FxHashSet<&str> = replica_a
            .query(&Get::new(&"k", Read::new()))
            .unwrap_or_default();
        let read_b: FxHashSet<&str> = replica_b
            .query(&Get::new(&"k", Read::new()))
            .unwrap_or_default();
        let result = set_from_slice(&[]);
        assert_eq!(
            (read_a, read_b),
            (result.clone(), result),
            "(replica a, replica b)"
        );
    }

    /// Every ordered pair of concurrent operation lists of length at most two
    /// over `{Add, Remove}` on two values plus `Clear`, placed under
    /// a map key, with a second round of writes on another key so that both
    /// replicas stabilize the same operations, each in its own delivery order.
    /// The replicas must read the same set on every pair.
    #[test]
    fn rw_set_converges_when_replicas_stabilize_in_different_orders() {
        let mut split = Vec::new();
        for (left, right) in concurrent_pairs() {
            let (mut replica_a, mut replica_b) = twins_log::<UWMapLog<&str, VecLog<RWSet<&str>>>>();
            let from_a: Vec<_> = left
                .iter()
                .filter_map(|op| replica_a.send(MapUpdate::Update("k", op.clone())).ok())
                .collect();
            let from_b: Vec<_> = right
                .iter()
                .filter_map(|op| replica_b.send(MapUpdate::Update("k", op.clone())).ok())
                .collect();
            for event in from_b {
                replica_a.receive(event);
            }
            for event in from_a {
                replica_b.receive(event);
            }
            let spacer_a = replica_a
                .send(MapUpdate::Update("spacer", SetUpdate::Add("s")))
                .unwrap();
            let spacer_b = replica_b
                .send(MapUpdate::Update("spacer", SetUpdate::Add("s")))
                .unwrap();
            replica_a.receive(spacer_b);
            replica_b.receive(spacer_a);
            let read_a: FxHashSet<&str> = replica_a
                .query(&Get::new(&"k", Read::new()))
                .unwrap_or_default();
            let read_b: FxHashSet<&str> = replica_b
                .query(&Get::new(&"k", Read::new()))
                .unwrap_or_default();
            if read_a != read_b {
                split.push(format!(
                    "{left:?} against {right:?}: {read_a:?} and {read_b:?}"
                ));
            }
        }
        assert!(
            split.is_empty(),
            "{} of 961 concurrent pairs do not converge once both replicas stabilize them, first: {}",
            split.len(),
            split.first().map(String::as_str).unwrap_or("")
        );
    }

    type UpdateList = Vec<SetUpdate<&'static str>>;

    fn concurrent_pairs() -> Vec<(UpdateList, UpdateList)> {
        let alphabet = [
            SetUpdate::Add("alpha"),
            SetUpdate::Remove("alpha"),
            SetUpdate::Add("beta"),
            SetUpdate::Remove("beta"),
            SetUpdate::Clear,
        ];
        let mut lists: Vec<Vec<SetUpdate<&str>>> = vec![Vec::new()];
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
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run = RunConfig::new(ChurnConfig::new(0.4, 0.6), 4, 10, None, None);
        let runs = vec![run; 10_000];

        let config = FuzzerConfig::<VecLog<RWSet<String>>, Read<FxHashSet<String>>>::new(
            "rw_set",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<VecLog<RWSet<String>>, Read<FxHashSet<String>>>(config);
    }
}
