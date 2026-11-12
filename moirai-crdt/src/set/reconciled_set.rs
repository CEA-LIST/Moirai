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
use moirai_protocol::{
    data_type::{
        policy::{LwwPolicy, Policy},
        query::{Contains, QueryOperation, Read},
        reconciliation::{Reconciled, Reconciliation},
        replicated::{
            CausalCompaction, CausalReset, CausalStabilize, Eval, Precondition, ReplicatedDataType,
        },
        sequential::{SequentialDataType, SequentialEval},
    },
    event::tagged_op::TaggedOp,
    state::unstable_state::IsUnstableCore,
};

use crate::{FxHashSet, set::update::SetUpdate};

/// Reconciles a log by sorting borrowed events with `P`.
///
/// `P` must provide a deterministic order compatible with causality. For example,
/// `Sorted<LwwPolicy>` orders by increasing timestamp, then replica ID.
pub struct Sorted<P>(PhantomData<fn() -> P>);

impl<O, U, P> Reconciliation<O, U> for Sorted<P>
where
    U: IsUnstableCore<O>,
    P: Policy,
{
    fn linearize<'a>(log: &'a U) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        let mut events: Vec<_> = log.iter().collect();
        events.sort_unstable_by(|a, b| {
            P::compare(a.tag(), b.tag())
                // Derived events can share a timestamp. Break ties independently
                // of delivery order and each replica's local index assignment.
                .then_with(|| a.id().origin_id().cmp(b.id().origin_id()))
                .then_with(|| a.id().seq().cmp(&b.id().seq()))
                .then_with(|| a.id().disambiguator().cmp(&b.id().disambiguator()))
        });
        events.into_iter()
    }
}

/// An update-consistent set
pub struct ReconciledSet<V>(PhantomData<V>);

impl<V: Debug + Clone + Eq + Hash> ReplicatedDataType for ReconciledSet<V> {
    type Update = SetUpdate<V>;
    type Stable = SequentialSet<V>;
    type Rejection = Infallible;
}

impl<V: Debug + Clone + Eq + Hash, U> Precondition<U> for ReconciledSet<V> {
    fn is_enabled(_: &SequentialSet<V>, _: &U, _: &SetUpdate<V>) -> Result<(), Infallible> {
        Ok(())
    }
}

impl<V, U> CausalCompaction<U> for ReconciledSet<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<SetUpdate<V>>,
{
    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_R_WHEN_NOT_R: bool = true;
}

impl<V: Debug + Clone + Eq + Hash, U> CausalStabilize<U> for ReconciledSet<V> {
    const DISABLE_STABILIZE: bool = true;
}

impl<V: Debug + Clone + Eq + Hash, U> CausalReset<U> for ReconciledSet<V> {}

impl<V, U, Q> Eval<U, Q> for ReconciledSet<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<SetUpdate<V>>,
    Q: QueryOperation,
    SequentialSet<V>: SequentialEval<Q, Update = SetUpdate<V>>,
{
    fn execute_query(stable: &SequentialSet<V>, unstable: &U, query: &Q) -> Q::Response {
        let state = Reconciled::<SequentialSet<V>, Sorted<LwwPolicy>>::fold(stable, unstable);
        SequentialSet::eval(&state, query)
    }
}

/// Ordinary sequential set semantics, independent of the replication order.
#[derive(Clone, Debug)]
pub struct SequentialSet<V>(FxHashSet<V>);

impl<V> Default for SequentialSet<V> {
    fn default() -> Self {
        Self(FxHashSet::default())
    }
}

impl<V> SequentialDataType for SequentialSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Rejection = Infallible;
    type Update = SetUpdate<V>;

    fn apply(&mut self, update: &Self::Update) {
        match update {
            SetUpdate::Add(value) => {
                self.0.insert(value.clone());
            }
            SetUpdate::Remove(value) => {
                self.0.remove(value);
            }
            SetUpdate::Clear => self.0.clear(),
        }
    }

    fn is_default(&self) -> bool {
        self.0.is_empty()
    }

    fn is_enabled(&self, _update: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }
}

impl<V> SequentialEval<Read<FxHashSet<V>>> for SequentialSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    fn eval(state: &Self, _: &Read<FxHashSet<V>>) -> FxHashSet<V> {
        state.0.clone()
    }
}

impl<V> SequentialEval<Contains<V>> for SequentialSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    fn eval(state: &Self, query: &Contains<V>) -> bool {
        state.0.contains(&query.0)
    }
}

#[cfg(feature = "test_utils")]
impl<V> DeepSizeOf for SequentialSet<V>
where
    V: DeepSizeOf + Eq + Hash,
{
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.0.deep_size_of_children(context)
    }
}

#[cfg(feature = "fuzz")]
impl<V, U> moirai_fuzz::generator::op_generator::OpGenerator<U> for ReconciledSet<V>
where
    V: moirai_fuzz::generator::value_generator::ValueGenerator + Debug + Clone + Eq + Hash,
    U: IsUnstableCore<SetUpdate<V>>,
{
    type Config = V::Config;

    fn generate(
        rng: &mut impl rand::Rng,
        config: &Self::Config,
        _: &SequentialSet<V>,
        _: &U,
    ) -> SetUpdate<V> {
        use rand::RngExt;

        match rng.random_range(0..8) {
            0..=4 => SetUpdate::Add(V::generate(rng, config)),
            5..=6 => SetUpdate::Remove(V::generate(rng, config)),
            _ => SetUpdate::Clear,
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{
        data_type::query::{Contains, Read},
        replica::IsReplica,
        state::po_log::GraphLog,
    };

    use crate::{
        FxHashSet,
        set::{reconciled_set::ReconciledSet, update::SetUpdate},
        utils::{bootstrap::twins_log, set_from_slice},
    };

    #[test]
    fn reconciled_set_converges_with_polog() {
        let (mut a, mut b) = twins_log::<GraphLog<ReconciledSet<&str>>>();

        // Both updates have timestamp 1. B's removal wins, even though it is
        // initially ineffective and B receives A's insertion afterwards.
        let add = a.send(SetUpdate::Add("x")).unwrap();
        assert!(a.query(&Contains("x")));

        let remove = b.send(SetUpdate::Remove("x")).unwrap();

        a.receive(remove);
        b.receive(add);

        for replica in [&a, &b] {
            assert_eq!(replica.query(&Read::new()), FxHashSet::<&str>::default());
            assert!(!replica.query(&Contains("x")));
        }

        // Reverse the operations: B's concurrent insertion now wins.
        let remove = a.send(SetUpdate::Remove("x")).unwrap();
        let add = b.send(SetUpdate::Add("x")).unwrap();
        a.receive(add);
        b.receive(remove);
        for replica in [&a, &b] {
            assert_eq!(replica.query(&Read::new()), set_from_slice(&["x"]));
        }

        // A later timestamp takes precedence over the replica-ID tie-break.
        let remove = a.send(SetUpdate::Remove("x")).unwrap();
        b.receive(remove);
        assert!(!a.query(&Contains("x")));
        assert!(!b.query(&Contains("x")));
    }

    #[cfg(feature = "fuzz")]
    #[ignore]
    #[test]
    fn fuzz_reconciled_set() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::{data_type::query::Read, state::po_log::VecLog};

        use crate::{FxHashSet, set::reconciled_set::ReconciledSet};

        type Log = VecLog<ReconciledSet<u8>>;

        let runs = vec![RunConfig::new(
            ChurnConfig::new(0.4, 0.6),
            8,
            1_000,
            None,
            None,
        )];
        let config = FuzzerConfig::<Log, Read<FxHashSet<u8>>>::new(
            "reconciled_set",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        // The fuzzer reconnects replicas, delivers every update, and checks convergence.
        fuzzer(config);
    }
}
