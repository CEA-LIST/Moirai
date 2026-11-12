use std::{convert::Infallible, fmt::Debug, marker::PhantomData};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    generator::op_generator::OpGenerator, generator::value_generator::ValueGenerator,
};
use moirai_protocol::{
    data_type::{
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

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TORegister<V>(PhantomData<V>);

impl<V> ReplicatedDataType for TORegister<V>
where
    V: Debug + PartialOrd + Ord + Clone,
{
    type Update = RegisterUpdate<V>;
    type Stable = Vec<Self::Update>;
    type Rejection = Infallible;
}

impl<V, U> CausalCompaction<U> for TORegister<V>
where
    V: Debug + PartialOrd + Ord + Clone,
    U: IsUnstableCore<Self::Update>,
{
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
        _unstable: impl Iterator<Item = &'a TaggedOp<Self::Update>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(new_tagged_op.op(), RegisterUpdate::Clear)
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
        _old_tag: Option<&Tag>,
        is_conc: bool,
        _new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        !is_conc
    }
}

impl<V, U> CausalStabilize<U> for TORegister<V> where V: Debug + PartialOrd + Ord + Clone {}

impl<V, U> CausalReset<U> for TORegister<V> where V: Debug + PartialOrd + Ord + Clone {}

impl<V, U> Eval<U, Read<Option<V>>> for TORegister<V>
where
    V: Debug + PartialOrd + Ord + Clone,
    U: IsUnstableCore<<Self as ReplicatedDataType>::Update>,
{
    fn execute_query(
        stable: &<Self as ReplicatedDataType>::Stable,
        unstable: &U,
        _q: &Read<Option<V>>,
    ) -> <Read<Option<V>> as QueryOperation>::Response {
        let mut val = None;
        for o in stable.iter().chain(unstable.iter().map(|t| t.op())) {
            if let RegisterUpdate::Write(v) = o
                && (val.is_none() || v > val.as_ref().unwrap())
            {
                val = Some(v.clone());
            }
        }
        val
    }
}

#[cfg(feature = "fuzz")]
impl<V, U> OpGenerator<U> for TORegister<V>
where
    U: IsUnstableCore<Self::Update>,
    V: Clone + ValueGenerator + PartialEq + PartialOrd + Debug + Ord,
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

impl<V, U> Precondition<U> for TORegister<V>
where
    V: Debug + PartialOrd + Ord + Clone,
{
    fn is_enabled(_: &Self::Stable, _: &U, _: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{data_type::query::Read, replica::IsReplica};

    use crate::{
        register::{to_register::TORegister, update::RegisterUpdate},
        utils::bootstrap::{triplet, twins},
    };

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_to_register() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        type Log = VecLog<TORegister<i32>>;
        let runs = vec![RunConfig::new(
            ChurnConfig::new(0.4, 0.6),
            8,
            1_000,
            None,
            None,
        )];
        let config = FuzzerConfig::<Log, Read<Option<i32>>>::new(
            "to_register",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );
        fuzzer::<Log, Read<Option<i32>>>(config);
    }

    #[test]
    fn simple_to_register() {
        let (mut replica_a, mut replica_b) = twins::<TORegister<&str>>();

        let event = replica_a.send(RegisterUpdate::Write("a")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(&Read::new()), Some("a"));
        assert_eq!(replica_b.query(&Read::new()), Some("a"));

        let event = replica_b.send(RegisterUpdate::Write("b")).unwrap();
        replica_a.receive(event);

        let result = Some("b");
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }

    #[test]
    fn concurrent_to_register() {
        let (mut replica_a, mut replica_b) = twins::<TORegister<&str>>();

        let event = replica_a.send(RegisterUpdate::Write("c")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(&Read::new()), Some("c"));
        assert_eq!(replica_b.query(&Read::new()), Some("c"));

        let event = replica_b.send(RegisterUpdate::Write("d")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(&Read::new()), Some("d"));
        assert_eq!(replica_b.query(&Read::new()), Some("d"));

        let event_a = replica_a.send(RegisterUpdate::Write("a")).unwrap();
        let event_b = replica_b.send(RegisterUpdate::Write("b")).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = Some("b");
        let eval_a = replica_a.query(&Read::new());
        let eval_b = replica_b.query(&Read::new());
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn multiple_concurrent_to_register() {
        let (mut replica_a, mut replica_b, _replica_c) = triplet::<TORegister<&str>>();

        let event = replica_a.send(RegisterUpdate::Write("c")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(&Read::new()), Some("c"));
        assert_eq!(replica_b.query(&Read::new()), Some("c"));

        let event = replica_b.send(RegisterUpdate::Write("d")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(&Read::new()), Some("d"));
        assert_eq!(replica_b.query(&Read::new()), Some("d"));

        let event_a = replica_a.send(RegisterUpdate::Write("a")).unwrap();
        let event_aa = replica_a.send(RegisterUpdate::Write("aa")).unwrap();

        let event_b = replica_b.send(RegisterUpdate::Write("b")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);
        replica_b.receive(event_aa);

        let result = Some("b");
        let eval_a = replica_a.query(&Read::new());
        let eval_b = replica_b.query(&Read::new());
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn to_register_instability() {
        let (mut replica_a, mut replica_b) = twins::<TORegister<u32>>();

        let event_a_1 = replica_a.send(RegisterUpdate::Write(4)).unwrap();
        assert_eq!(replica_a.query(&Read::new()), Some(4));
        let event_b_1 = replica_b.send(RegisterUpdate::Write(5)).unwrap();
        assert_eq!(replica_b.query(&Read::new()), Some(5));
        replica_a.receive(event_b_1);
        assert_eq!(replica_a.query(&Read::new()), Some(5));

        let event_b_2 = replica_b.send(RegisterUpdate::Write(2)).unwrap();
        assert_eq!(replica_b.query(&Read::new()), Some(2));
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);

        assert_eq!(replica_a.query(&Read::new()), Some(4));
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }
}
