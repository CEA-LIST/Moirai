use std::{convert::Infallible, fmt::Debug, hash::Hash, marker::PhantomData};

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

use crate::{FxHashSet, register::update::RegisterUpdate};

pub struct MVRegister<V>(PhantomData<V>);

impl<V> ReplicatedDataType for MVRegister<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Update = RegisterUpdate<V>;
    type Stable = Vec<Self::Update>;
    type Rejection = Infallible;
}

impl<V, U> CausalCompaction<U> for MVRegister<V>
where
    V: Debug + Clone + Eq + Hash,
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

impl<V, U> CausalStabilize<U> for MVRegister<V> where V: Debug + Clone + Eq + Hash {}

impl<V, U> CausalReset<U> for MVRegister<V> where V: Debug + Clone + Eq + Hash {}

impl<V, U> Eval<U, Read<FxHashSet<V>>> for MVRegister<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<<Self as ReplicatedDataType>::Update>,
{
    fn execute_query(
        stable: &Vec<<Self as ReplicatedDataType>::Update>,
        unstable: &U,
        _q: &Read<FxHashSet<V>>,
    ) -> <Read<FxHashSet<V>> as QueryOperation>::Response {
        let mut set = FxHashSet::<V>::default();
        for o in stable.iter().chain(unstable.iter().map(|t| t.op())) {
            if let RegisterUpdate::Write(v) = o {
                set.insert(v.clone());
            }
        }
        set
    }
}

#[cfg(feature = "fuzz")]
impl<V, U> OpGenerator<U> for MVRegister<V>
where
    U: IsUnstableCore<Self::Update>,
    V: Clone + Debug + Eq + Hash + ValueGenerator,
{
    type Config = ();

    fn generate(
        rng: &mut impl Rng,
        _config: &Self::Config,
        _stable: &Self::Stable,
        _unstable: &U,
    ) -> Self::Update {
        if rng.random_ratio(1, 5) {
            Self::Update::Clear
        } else {
            Self::Update::Write(V::generate(rng, &<V as ValueGenerator>::Config::default()))
        }
    }
}

impl<V, U> Precondition<U> for MVRegister<V>
where
    V: Debug + Clone + Eq + Hash,
{
    fn is_enabled(_: &Self::Stable, _: &U, _: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{data_type::query::Read, replica::IsReplica};

    use crate::{
        FxHashSet,
        register::mv_register::{MVRegister, RegisterUpdate},
        utils::{
            bootstrap::{triplet, twins},
            set_from_slice,
        },
    };

    #[test]
    fn simple_mv_register() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<&str>>();

        let event = replica_a.send(RegisterUpdate::Write("a")).unwrap();
        replica_b.receive(event);

        assert_eq!(
            replica_a.query(&Read::new()),
            FxHashSet::from_iter(std::iter::once(&"a").cloned())
        );
        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&["a"]));

        let event = replica_b.send(RegisterUpdate::Write("b")).unwrap();
        replica_a.receive(event);

        let result = set_from_slice(&["b"]);
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }

    #[test]
    fn concurrent_mv_register() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<&str>>();

        let event = replica_a.send(RegisterUpdate::Write("c")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&["c"]));
        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&["c"]));

        let event = replica_b.send(RegisterUpdate::Write("d")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&["d"]));
        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&["d"]));

        let event_a = replica_a.send(RegisterUpdate::Write("a")).unwrap();
        let event_b = replica_b.send(RegisterUpdate::Write("b")).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = set_from_slice(&["b", "a"]);
        let eval_a = replica_a.query(&Read::new());
        let eval_b = replica_b.query(&Read::new());
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn multiple_concurrent_mv_register() {
        let (mut replica_a, mut replica_b, _replica_c) = triplet::<MVRegister<&str>>();

        let event = replica_a.send(RegisterUpdate::Write("c")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&["c"]));
        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&["c"]));

        let event = replica_b.send(RegisterUpdate::Write("d")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&["d"]));
        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&["d"]));

        let event_a = replica_a.send(RegisterUpdate::Write("a")).unwrap();
        let event_aa = replica_a.send(RegisterUpdate::Write("aa")).unwrap();

        let event_b = replica_b.send(RegisterUpdate::Write("b")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);
        replica_b.receive(event_aa);

        let result = set_from_slice(&["aa", "b"]);
        let eval_a = replica_a.query(&Read::new());
        let eval_b = replica_b.query(&Read::new());
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn mv_register_instability() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<u32>>();

        let event_a_1 = replica_a.send(RegisterUpdate::Write(4)).unwrap();
        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&[4]));
        let event_b_1 = replica_b.send(RegisterUpdate::Write(5)).unwrap();
        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&[5]));
        replica_a.receive(event_b_1);
        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&[4, 5]));

        let event_b_2 = replica_b.send(RegisterUpdate::Write(2)).unwrap();
        assert_eq!(replica_b.query(&Read::new()), set_from_slice(&[2]));
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);

        assert_eq!(replica_a.query(&Read::new()), set_from_slice(&[4, 2]));
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_mv_register() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        use crate::register::mv_register::MVRegister;

        type Log = VecLog<MVRegister<i32>>;
        let runs = vec![RunConfig::new(
            ChurnConfig::new(0.4, 0.6),
            8,
            1_000,
            None,
            None,
        )];
        let config = FuzzerConfig::<Log, Read<FxHashSet<i32>>>::new(
            "mv_register",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );
        fuzzer::<Log, Read<FxHashSet<i32>>>(config);
    }
}
