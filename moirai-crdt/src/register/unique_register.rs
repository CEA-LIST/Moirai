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

use std::{cmp::Ordering, convert::Infallible, fmt::Debug, marker::PhantomData};

#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    generator::op_generator::OpGenerator, generator::value_generator::ValueGenerator,
};
use moirai_protocol::{
    data_type::{
        policy::{FairPolicy, LwwPolicy, Policy},
        query::{QueryOperation, Read},
        replicated::{
            CausalCompaction, CausalReset, CausalStabilize, Eval, Precondition, RedundancyRelation,
            ReplicatedDataType,
        },
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableCore,
};
#[cfg(feature = "fuzz")]
use rand::{Rng, RngExt};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::register::update::RegisterUpdate;

pub type LwwRegister<V> = Register<V, LwwPolicy>;
pub type FairRegister<V> = Register<V, FairPolicy>;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Register<V, P>(PhantomData<(V, P)>);

impl<V, P> ReplicatedDataType for Register<V, P>
where
    V: Debug + Clone,
    P: Policy,
{
    type Update = RegisterUpdate<V>;
    type Stable = Vec<Self::Update>;
    type Rejection = Infallible;
}

impl<V, P, U> CausalCompaction<U> for Register<V, P>
where
    V: Debug + Clone,
    P: Policy,
    U: IsUnstableCore<Self::Update>,
{
    const DISABLE_R_WHEN_R: bool = true;

    fn compact_stable(
        stable: &mut Self::Stable,
        rdnt: RedundancyRelation<Self::Update>,
        new: &TaggedOp<Self::Update>,
    ) {
        stable.retain(|old| !rdnt(old, None, false, new));
    }

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self::Update>,
        _stable: &Self::Stable,
        mut unstable: impl Iterator<Item = &'a TaggedOp<Self::Update>>,
    ) -> bool
    where
        Self: 'a,
    {
        match new_tagged_op.op() {
            RegisterUpdate::Clear => true,
            RegisterUpdate::Write(_) => unstable.any(|old_tagged_op| {
                P::compare(new_tagged_op.tag(), old_tagged_op.tag()) == Ordering::Less
            }),
        }
    }

    fn redundant_by_when_redundant(
        _old_op: &Self::Update,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        _new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        !is_conc
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self::Update,
        old_tag: Option<&Tag>,
        _is_conc: bool,
        new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        old_tag.is_none_or(|old_tag| P::compare(new_tagged_op.tag(), old_tag) == Ordering::Greater)
    }
}

impl<V, P, U> CausalStabilize<U> for Register<V, P>
where
    V: Debug + Clone,
    P: Policy,
{
}

impl<V, P, U> CausalReset<U> for Register<V, P>
where
    V: Debug + Clone,
    P: Policy,
{
}

impl<V, P, U> Eval<U, Read<Option<V>>> for Register<V, P>
where
    V: Debug + Clone,
    P: Policy,
    U: IsUnstableCore<<Self as ReplicatedDataType>::Update>,
{
    fn execute_query(
        stable: &<Self as ReplicatedDataType>::Stable,
        unstable: &U,
        _q: &Read<Option<V>>,
    ) -> <Read<Option<V>> as QueryOperation>::Response {
        let mut value = None;
        for op in stable.iter().chain(unstable.iter().map(|t| t.op())) {
            match op {
                RegisterUpdate::Write(v) => value = Some(v.clone()),
                _ => unreachable!(),
            }
        }
        value
    }
}

#[cfg(feature = "fuzz")]
impl<V, P, U> OpGenerator<U> for Register<V, P>
where
    U: IsUnstableCore<Self::Update>,
    P: Policy,
    V: ValueGenerator + Debug + Clone,
{
    type Config = ();

    fn generate(
        rng: &mut impl Rng,
        _config: &Self::Config,
        _stable: &Self::Stable,
        _unstable: &U,
    ) -> Self::Update {
        if rng.random_ratio(1, 5) {
            RegisterUpdate::Clear
        } else {
            RegisterUpdate::Write(V::generate(rng, &<V as ValueGenerator>::Config::default()))
        }
    }
}

impl<V, P, U> Precondition<U> for Register<V, P>
where
    V: Debug + Clone,
    P: Policy,
{
    fn is_enabled(_: &Self::Stable, _: &U, _: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{
        data_type::{
            policy::{FairPolicy, LwwPolicy},
            query::Read,
        },
        replica::IsReplica,
    };

    use crate::{
        register::{unique_register::Register, update::RegisterUpdate},
        utils::bootstrap::{triplet, twins},
    };

    #[test]
    pub fn lww_register_with_write() {
        let (mut replica_a, mut replica_b) = twins::<Register<String, LwwPolicy>>();

        let event = replica_a
            .send(RegisterUpdate::Write("Hello".to_string()))
            .unwrap();
        replica_b.receive(event);

        let event = replica_a
            .send(RegisterUpdate::Write("World".to_string()))
            .unwrap();
        replica_b.receive(event);

        let result = "World".to_string();
        assert_eq!(replica_a.query(&Read::new()), Some(result));
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }

    #[test]
    pub fn lww_register_concurrent_writes() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet::<Register<String, LwwPolicy>>();

        let event_a = replica_a
            .send(RegisterUpdate::Write("Hello".to_string()))
            .unwrap();
        assert!(replica_a.query(&Read::new()) == Some("Hello".to_string()));
        let event_b = replica_b
            .send(RegisterUpdate::Write("World".to_string()))
            .unwrap();
        assert!(replica_b.query(&Read::new()) == Some("World".to_string()));

        replica_a.receive(event_b.clone());
        assert_eq!(replica_a.query(&Read::new()), Some("World".to_string()));
        replica_b.receive(event_a.clone());
        assert_eq!(replica_b.query(&Read::new()), Some("World".to_string()));
        replica_c.receive(event_a);
        assert_eq!(replica_c.query(&Read::new()), Some("Hello".to_string()));
        replica_c.receive(event_b);
        assert_eq!(replica_c.query(&Read::new()), Some("World".to_string()));
    }

    #[test]
    pub fn lww_register_more_concurrent() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet::<Register<String, LwwPolicy>>();

        let event_c_1 = replica_c
            .send(RegisterUpdate::Write("x".to_string()))
            .unwrap();
        replica_a.receive(event_c_1.clone());

        let event_a_1 = replica_a
            .send(RegisterUpdate::Write("y".to_string()))
            .unwrap();

        let event_b_1 = replica_b
            .send(RegisterUpdate::Write("z".to_string()))
            .unwrap();
        replica_c.receive(event_b_1.clone());

        replica_b.receive(event_c_1);
        replica_b.receive(event_a_1.clone());

        replica_c.receive(event_a_1);
        replica_a.receive(event_b_1);

        assert_eq!(replica_a.query(&Read::new()), Some("y".to_string()));
        assert_eq!(replica_b.query(&Read::new()), Some("y".to_string()));
        assert_eq!(replica_c.query(&Read::new()), Some("y".to_string()));
    }

    #[test]
    pub fn fair_register_concurrent() {
        let (mut replica_a, mut replica_b) = twins::<Register<String, FairPolicy>>();

        let event_a = replica_a
            .send(RegisterUpdate::Write("Public".to_string()))
            .unwrap();

        let event_b = replica_b
            .send(RegisterUpdate::Write("Protected".to_string()))
            .unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(&Read::new()), Some("Public".to_string()));
        assert_eq!(replica_b.query(&Read::new()), Some("Public".to_string()));

        let event_a_2 = replica_a
            .send(RegisterUpdate::Write("Private".to_string()))
            .unwrap();

        replica_b.receive(event_a_2);

        assert_eq!(replica_a.query(&Read::new()), Some("Private".to_string()));
        assert_eq!(replica_b.query(&Read::new()), Some("Private".to_string()));

        let event_b_2 = replica_b
            .send(RegisterUpdate::Write("Protected".to_string()))
            .unwrap();

        let event_a_3 = replica_a
            .send(RegisterUpdate::Write("Public".to_string()))
            .unwrap();

        replica_a.receive(event_b_2);
        replica_b.receive(event_a_3);

        assert_eq!(replica_a.query(&Read::new()), Some("Protected".to_string()));
        assert_eq!(replica_b.query(&Read::new()), Some("Protected".to_string()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_lww_register() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        type Log = VecLog<Register<i32, LwwPolicy>>;
        let runs = vec![RunConfig::new(
            ChurnConfig::new(0.4, 0.6),
            8,
            1_000,
            None,
            None,
        )];
        let config = FuzzerConfig::<Log, Read<Option<i32>>>::new(
            "lww_register",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );
        fuzzer::<Log, Read<Option<i32>>>(config);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_fair_register() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        type Log = VecLog<Register<i32, FairPolicy>>;
        let runs = vec![RunConfig::new(
            ChurnConfig::new(0.4, 0.6),
            8,
            1_000,
            None,
            None,
        )];
        let config = FuzzerConfig::<Log, Read<Option<i32>>>::new(
            "fair_register",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );
        fuzzer::<Log, Read<Option<i32>>>(config);
    }
}
