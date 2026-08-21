use std::{convert::Infallible, fmt::Debug, hash::Hash};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{op_generator::OpGenerator, value_generator::ValueGenerator};
use moirai_protocol::{
    crdt::{
        eval::Eval,
        query::{QueryOperation, Read},
        replicated_data_type::{ReplicatedDataType, UsesUnstableService},
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableCore,
};
#[cfg(feature = "fuzz")]
use rand::{Rng, RngExt};

use crate::HashSet;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum MVRegister<V> {
    Clear,
    Write(V),
}

impl<V> ReplicatedDataType for MVRegister<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Value = HashSet<V>;
    type StableState = Vec<Self>;
    type Rejection = Infallible;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(new_tagged_op.op(), MVRegister::Clear)
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        !is_conc
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        !is_conc
    }
}

impl<V, U> UsesUnstableService<U> for MVRegister<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<Self>,
{
}

impl<V, U> Eval<Read<<Self as ReplicatedDataType>::Value>, U> for MVRegister<V>
where
    V: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<Self>,
{
    fn execute_query(
        _q: Read<<Self as ReplicatedDataType>::Value>,
        stable: &<MVRegister<V> as ReplicatedDataType>::StableState,
        unstable: &U,
    ) -> <Read<<Self as ReplicatedDataType>::Value> as QueryOperation>::Response {
        let mut set = HashSet::<V>::default();
        for o in stable.iter().chain(unstable.iter().map(|t| t.op())) {
            if let MVRegister::Write(v) = o {
                set.insert(v.clone());
            }
        }
        set
    }
}

#[cfg(feature = "fuzz")]
impl<V> OpGenerator for MVRegister<V>
where
    V: Clone + Debug + Eq + Hash + ValueGenerator,
{
    type Config = ();

    fn generate(
        rng: &mut impl Rng,
        _config: &Self::Config,
        _stable: &Self::StableState,
        _unstable: &impl IsUnstableCore<Self>,
    ) -> Self {
        if rng.random_ratio(1, 5) {
            Self::Clear
        } else {
            Self::Write(V::generate(rng, &<V as ValueGenerator>::Config::default()))
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{crdt::query::Read, replica::IsReplica};

    use crate::{
        HashSet,
        register::mv_register::MVRegister,
        utils::{
            membership::{triplet, twins},
            set_from_slice,
        },
    };

    #[test]
    fn simple_mv_register() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<&str>>();

        let event = replica_a.send(MVRegister::Write("a")).unwrap();
        replica_b.receive(event);

        assert_eq!(
            replica_a.query(Read::new()),
            HashSet::from_iter(["a"].iter().cloned())
        );
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["a"]));

        let event = replica_b.send(MVRegister::Write("b")).unwrap();
        replica_a.receive(event);

        let result = set_from_slice(&["b"]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn concurrent_mv_register() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<&str>>();

        let event = replica_a.send(MVRegister::Write("c")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["c"]));
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["c"]));

        let event = replica_b.send(MVRegister::Write("d")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["d"]));
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["d"]));

        let event_a = replica_a.send(MVRegister::Write("a")).unwrap();
        let event_b = replica_b.send(MVRegister::Write("b")).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = set_from_slice(&["b", "a"]);
        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn multiple_concurrent_mv_register() {
        let (mut replica_a, mut replica_b, _replica_c) = triplet::<MVRegister<&str>>();

        let event = replica_a.send(MVRegister::Write("c")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["c"]));
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["c"]));

        let event = replica_b.send(MVRegister::Write("d")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&["d"]));
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&["d"]));

        let event_a = replica_a.send(MVRegister::Write("a")).unwrap();
        let event_aa = replica_a.send(MVRegister::Write("aa")).unwrap();

        let event_b = replica_b.send(MVRegister::Write("b")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);
        replica_b.receive(event_aa);

        let result = set_from_slice(&["aa", "b"]);
        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn mv_register_instability() {
        let (mut replica_a, mut replica_b) = twins::<MVRegister<u32>>();

        let event_a_1 = replica_a.send(MVRegister::Write(4)).unwrap();
        assert_eq!(replica_a.query(Read::new()), set_from_slice(&[4]));
        let event_b_1 = replica_b.send(MVRegister::Write(5)).unwrap();
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&[5]));
        replica_a.receive(event_b_1);
        assert_eq!(replica_a.query(Read::new()), set_from_slice(&[4, 5]));

        let event_b_2 = replica_b.send(MVRegister::Write(2)).unwrap();
        assert_eq!(replica_b.query(Read::new()), set_from_slice(&[2]));
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);

        assert_eq!(replica_a.query(Read::new()), set_from_slice(&[4, 2]));
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_mv_register() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        type Log = VecLog<MVRegister<i32>>;
        let runs = vec![RunConfig::new(0.4, 8, 1_000, None, None, false, false)];
        let config = FuzzerConfig::<Log>::new("mv_register", runs, true, |a, b| a == b, false);
        fuzzer::<Log>(config);
    }
}
