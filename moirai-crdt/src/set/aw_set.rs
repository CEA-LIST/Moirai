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
    state::unstable_state::IsUnstableCore,
};
#[cfg(feature = "fuzz")]
use rand::Rng;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::{FxHashSet, set::update::SetUpdate};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AWSet<V>(PhantomData<V>);

impl<V> ReplicatedDataType for AWSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Update = SetUpdate<V>;
    type Stable = Set<V>;
    type Rejection = Infallible;
}

impl<U, V> CausalReset<U> for AWSet<V> where V: Debug + Clone + Eq + Hash {}

impl<U, V> CausalStabilize<U> for AWSet<V> where V: Debug + Clone + Eq + Hash {}

impl<U, V> Precondition<U> for AWSet<V>
where
    V: Debug + Clone + Eq + Hash,
{
    fn is_enabled(_: &Set<V>, _: &U, _: &SetUpdate<V>) -> Result<(), Infallible> {
        Ok(())
    }
}

impl<U, V> CausalCompaction<U> for AWSet<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<SetUpdate<V>>,
{
    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self::Update>,
        _stable: &Self::Stable,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self::Update>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(
            new_tagged_op.op(),
            Self::Update::Clear | Self::Update::Remove(_)
        )
    }

    fn redundant_by_when_redundant(
        old_op: &Self::Update,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        !is_conc
            && match (old_op, new_tagged_op.op()) {
                (Self::Update::Add(v1), Self::Update::Add(v2))
                | (Self::Update::Add(v1), Self::Update::Remove(v2)) => v1 == v2,
                (_, Self::Update::Clear) => true,
                (Self::Update::Remove(_), _) => unreachable!(),
                (Self::Update::Clear, _) => unreachable!(),
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

    fn compact_stable(
        stable: &mut Set<V>,
        _rdnt: RedundancyRelation<SetUpdate<V>>,
        new: &TaggedOp<SetUpdate<V>>,
    ) {
        match new.op() {
            SetUpdate::Add(value) | SetUpdate::Remove(value) => {
                stable.0.remove(value);
            }
            SetUpdate::Clear => stable.0.clear(),
        }
    }
}

impl<V, U> Eval<U, Read<FxHashSet<V>>> for AWSet<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<SetUpdate<V>>,
{
    fn execute_query(
        stable: &Set<V>,
        unstable: &U,
        _q: &Read<FxHashSet<V>>,
    ) -> <Read<FxHashSet<V>> as QueryOperation>::Response {
        let mut set = stable.0.clone();
        for o in unstable.iter() {
            if let SetUpdate::Add(v) = o.op() {
                set.insert(v.clone());
            }
        }
        set
    }
}

impl<V, U> Eval<U, Contains<V>> for AWSet<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<SetUpdate<V>>,
{
    fn execute_query(
        stable: &Set<V>,
        unstable: &U,
        q: &Contains<V>,
    ) -> <Contains<V> as QueryOperation>::Response {
        stable.0.contains(&q.0)
            || unstable
                .iter()
                .any(|o| matches!(o.op(), SetUpdate::Add(v) if v == &q.0))
    }
}

#[derive(Clone, Debug)]
pub struct Set<V>(FxHashSet<V>);

#[cfg(feature = "test_utils")]
impl<V: DeepSizeOf + Eq + Hash> DeepSizeOf for Set<V> {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.0.deep_size_of_children(context)
    }
}

impl<V> Default for Set<V> {
    fn default() -> Self {
        Self(FxHashSet::default())
    }
}

impl<V> SequentialDataType for Set<V>
where
    V: Clone + Eq + Hash + Debug,
{
    type Update = SetUpdate<V>;
    type Rejection = Infallible;

    fn is_default(&self) -> bool {
        self.0.is_empty()
    }

    fn apply(&mut self, update: &Self::Update) {
        match update {
            SetUpdate::Add(value) => {
                self.0.insert(value.clone());
            }
            SetUpdate::Remove(value) => {
                self.0.remove(value);
            }
            SetUpdate::Clear => {
                self.0.clear();
            }
        }
    }

    fn is_enabled(&self, _update: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }
}

#[cfg(feature = "fuzz")]
impl<V, U> OpGenerator<U> for AWSet<V>
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
            Choice::Add => Self::Update::Add(value),
            Choice::Remove => Self::Update::Remove(value),
            Choice::Clear => Self::Update::Clear,
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{
        data_type::query::{Contains, Read},
        replica::IsReplica,
        state::po_log::VecLog,
    };

    use crate::{
        FxHashSet,
        set::{aw_set::AWSet, update::SetUpdate},
        utils::{bootstrap::twins_log, set_from_slice},
    };

    #[test]
    fn simple_aw_set() {
        let (mut replica_a, mut replica_b) = twins_log::<VecLog<AWSet<&str>>>();

        let event = replica_a.send(SetUpdate::Add("a")).unwrap();
        replica_b.receive(event);

        assert_eq!(set_from_slice(&["a"]), replica_a.query(&Read::new()));
        assert_eq!(set_from_slice(&["a"]), replica_b.query(&Read::new()));

        let event = replica_b.send(SetUpdate::Add("b")).unwrap();
        replica_a.receive(event);

        let result = set_from_slice(&["b", "a"]);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);

        let event = replica_a.send(SetUpdate::Remove("a")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(SetUpdate::Add("c")).unwrap();
        replica_a.receive(event);

        let result = set_from_slice(&["b", "c"]);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
        assert!(replica_a.query(&Contains("b")));
        assert!(!replica_a.query(&Contains("a")));
    }

    #[test]
    fn complex_aw_set() {
        let (mut replica_a, mut replica_b) = twins_log::<VecLog<AWSet<&str>>>();

        let event = replica_a.send(SetUpdate::Add("b")).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(SetUpdate::Add("a")).unwrap();
        replica_b.receive(event);

        let event_a = replica_a.send(SetUpdate::Remove("a")).unwrap();
        let event_b = replica_b.send(SetUpdate::Add("c")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = set_from_slice(&["b", "c"]);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    #[test]
    fn clear_aw_set() {
        let (mut replica_a, mut replica_b) = twins_log::<VecLog<AWSet<&str>>>();

        let event = replica_a.send(SetUpdate::Add("a")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(SetUpdate::Add("b")).unwrap();
        replica_a.receive(event);

        let event = replica_a.send(SetUpdate::Clear).unwrap();
        replica_b.receive(event);

        let result = FxHashSet::default();
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    #[test]
    fn concurrent_aw_set() {
        let (mut replica_a, mut replica_b) = twins_log::<VecLog<AWSet<&str>>>();

        let event = replica_a.send(SetUpdate::Add("a")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(SetUpdate::Add("b")).unwrap();
        replica_a.receive(event);

        let event_a = replica_a.send(SetUpdate::Add("a")).unwrap();
        let event_b = replica_b.send(SetUpdate::Remove("a")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = set_from_slice(&["a", "b"]);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    #[test]
    fn concurrent_add_aw_set() {
        let (mut replica_a, mut replica_b) = twins_log::<VecLog<AWSet<&str>>>();

        let event = replica_a.send(SetUpdate::Add("c")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(SetUpdate::Add("b")).unwrap();
        replica_a.receive(event);

        let event_a = replica_a.send(SetUpdate::Add("a")).unwrap();
        let event_b = replica_b.send(SetUpdate::Add("a")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = set_from_slice(&["a", "c", "b"]);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    #[test]
    fn concurrent_add_aw_set_2() {
        let (mut replica_a, mut replica_b) = twins_log::<VecLog<AWSet<&str>>>();

        let event_a = replica_a.send(SetUpdate::Remove("a")).unwrap();
        let event_b = replica_b.send(SetUpdate::Add("a")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&["a"]));
        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&["a"]));
    }

    #[test]
    fn concurrent_add_aw_set_3() {
        let (mut replica_a, mut replica_b) = twins_log::<VecLog<AWSet<&str>>>();

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

    #[test]
    fn concurrent_add_aw_set_4() {
        let (mut replica_a, mut replica_b) = twins_log::<VecLog<AWSet<&str>>>();

        let event_a_1 = replica_a.send(SetUpdate::Add("a")).unwrap();
        let event_a_2 = replica_a.send(SetUpdate::Remove("b")).unwrap();
        let event_b_1 = replica_b.send(SetUpdate::Add("b")).unwrap();
        let event_b_2 = replica_b.send(SetUpdate::Remove("a")).unwrap();

        replica_a.receive(event_b_1);
        replica_b.receive(event_a_1);
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_2);

        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&["a", "b"]));
        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&["a", "b"]));
    }

    #[test]
    fn concurrent_add_remove_same_element() {
        let (mut replica_a, mut replica_b) = twins_log::<VecLog<AWSet<&str>>>();

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

    #[test]
    #[cfg(feature = "test_utils")]
    pub fn commitment() {
        use moirai_protocol::{
            broadcast::tcsb::Tcsb,
            commitment::{commit_log::CommitmentLog, commit_op::CommitOp},
            replica::Replica,
        };

        type Log<'a> = CommitmentLog<VecLog<AWSet<&'a str>>>;
        type Tcb<'a> = Tcsb<CommitOp<SetUpdate<&'a str>>>;

        let members = ["a", "b", "c"];

        let mut replica_a = Replica::<Log, Tcb>::bootstrap("a".into(), &members);
        let mut replica_b = Replica::<Log, Tcb>::bootstrap("b".into(), &members);
        // Let replica c be faulty
        let mut _replica_c = Replica::<Log, Tcb>::bootstrap("c".into(), &members);

        // Replica a and b are well-connected
        replica_a.state_mut().oracle_mut().set_leader("a");
        replica_b.state_mut().oracle_mut().set_leader("a");

        let event_a_1 = replica_a.send(SetUpdate::Add("a")).unwrap();
        replica_b.receive(event_a_1);

        let event_a_2 = replica_a.send(SetUpdate::Add("b")).unwrap();

        let event_b_1 = replica_b.send(SetUpdate::Remove("a")).unwrap();
        let event_b_2 = replica_b.send(SetUpdate::Add("c")).unwrap();

        replica_a.receive(event_b_1);
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_2);

        let value = set_from_slice(&["b", "c"]);
        assert_eq!(replica_a.query(&Read::new()), value);
        assert_eq!(replica_b.query(&Read::new()), value);

        assert_eq!(replica_a.state().child().unstable().len(), 2);
        assert_eq!(replica_b.state().child().unstable().len(), 2);
        assert_eq!(replica_a.tcsb().causally_stable_events_count(), 0);
        assert_eq!(replica_b.tcsb().causally_stable_events_count(), 0);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_aw_set() {
        use moirai_fuzz::{
            config::{
                CallSampling, ChurnConfig, FuzzerConfig, MetricsSampling, Predicate,
                ReplicaSampling, RunConfig,
            },
            fuzzer::fuzzer,
            utils::seed_hex::parse_hex_seed,
        };

        let replica_zero =
            MetricsSampling::new(CallSampling::All, ReplicaSampling::Indices(vec![0]));

        let run = RunConfig::new(
            ChurnConfig::new(0.4, 0.6),
            8,
            1_000_000,
            None,
            Some(
                parse_hex_seed(
                    "0x665A465C8960A890E4EDA68EAE17BF7558EFA299E7279960653A2C7298CDFBBE",
                )
                .unwrap(),
            ),
        )
        .with_response_time(replica_zero.clone())
        .with_effect_time(replica_zero.clone())
        .with_log_size(replica_zero)
        .without_log_stabilization();

        let config = FuzzerConfig::<VecLog<AWSet<usize>>, Read<FxHashSet<usize>>>::new(
            "aw_set",
            vec![run],
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<VecLog<AWSet<usize>>, Read<FxHashSet<usize>>>(config);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_commitment_aw_set() {
        use moirai_fuzz::{
            config::{
                CallSampling, ChurnConfig, FuzzerConfig, MetricsSampling, Predicate,
                ReplicaSampling, RunConfig,
            },
            fuzzer::fuzzer,
            utils::seed_hex::parse_hex_seed,
        };
        use moirai_protocol::commitment::commit_log::CommitmentLog;

        type Log = CommitmentLog<VecLog<AWSet<usize>>>;

        let replica_zero =
            MetricsSampling::new(CallSampling::All, ReplicaSampling::Indices(vec![0]));
        let run = RunConfig::new(
            ChurnConfig::new(0.4, 0.6),
            8,
            10_000,
            None,
            Some(
                parse_hex_seed(
                    "0x665A465C8960A890E4EDA68EAE17BF7558EFA299E7279960653A2C7298CDFBBE",
                )
                .unwrap(),
            ),
        )
        .with_response_time(replica_zero.clone())
        .with_effect_time(replica_zero.clone())
        .with_log_size(replica_zero.clone())
        .with_replica_size(replica_zero)
        .without_log_stabilization();

        let config = FuzzerConfig::<Log, Read<FxHashSet<usize>>>::new(
            "aw_set_commitment",
            vec![run],
            Predicate::new(Read::new(), |a, b| a == b),
        )
        .with_omega_oracle();

        fuzzer::<Log, Read<FxHashSet<usize>>>(config);
    }
}
