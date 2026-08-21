use std::{convert::Infallible, fmt::Debug};

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

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum TORegister<V> {
    Clear,
    Write(V),
}

impl<V> ReplicatedDataType for TORegister<V>
where
    V: Debug + PartialOrd + Ord + Clone,
{
    type Value = Option<V>;
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
        matches!(new_tagged_op.op(), TORegister::Clear)
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

impl<V, U> UsesUnstableService<U> for TORegister<V>
where
    V: Debug + PartialOrd + Ord + Clone,
    U: IsUnstableCore<Self>,
{
}

impl<V, U> Eval<Read<<Self as ReplicatedDataType>::Value>, U> for TORegister<V>
where
    V: Debug + PartialOrd + Ord + Clone,
    U: IsUnstableCore<Self>,
{
    fn execute_query(
        _q: Read<<Self as ReplicatedDataType>::Value>,
        stable: &<TORegister<V> as ReplicatedDataType>::StableState,
        unstable: &U,
    ) -> <Read<<Self as ReplicatedDataType>::Value> as QueryOperation>::Response {
        let mut val = None;
        for o in stable.iter().chain(unstable.iter().map(|t| t.op())) {
            if let TORegister::Write(v) = o
                && (val.is_none() || v > val.as_ref().unwrap())
            {
                val = Some(v.clone());
            }
        }
        val
    }
}

#[cfg(feature = "fuzz")]
impl<V> OpGenerator for TORegister<V>
where
    V: Clone + ValueGenerator + PartialEq + PartialOrd + Debug + Ord,
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
        register::to_register::TORegister,
        utils::membership::{triplet, twins},
    };

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_to_register() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        type Log = VecLog<TORegister<i32>>;
        let runs = vec![RunConfig::new(0.4, 8, 1_000, None, None, false, false)];
        let config = FuzzerConfig::<Log>::new("to_register", runs, true, |a, b| a == b, false);
        fuzzer::<Log>(config);
    }

    #[test]
    fn simple_to_register() {
        let (mut replica_a, mut replica_b) = twins::<TORegister<&str>>();

        let event = replica_a.send(TORegister::Write("a")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), Some("a"));
        assert_eq!(replica_b.query(Read::new()), Some("a"));

        let event = replica_b.send(TORegister::Write("b")).unwrap();
        replica_a.receive(event);

        let result = Some("b");
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn concurrent_to_register() {
        let (mut replica_a, mut replica_b) = twins::<TORegister<&str>>();

        let event = replica_a.send(TORegister::Write("c")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), Some("c"));
        assert_eq!(replica_b.query(Read::new()), Some("c"));

        let event = replica_b.send(TORegister::Write("d")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(Read::new()), Some("d"));
        assert_eq!(replica_b.query(Read::new()), Some("d"));

        let event_a = replica_a.send(TORegister::Write("a")).unwrap();
        let event_b = replica_b.send(TORegister::Write("b")).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        let result = Some("b");
        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn multiple_concurrent_to_register() {
        let (mut replica_a, mut replica_b, _replica_c) = triplet::<TORegister<&str>>();

        let event = replica_a.send(TORegister::Write("c")).unwrap();
        replica_b.receive(event);

        assert_eq!(replica_a.query(Read::new()), Some("c"));
        assert_eq!(replica_b.query(Read::new()), Some("c"));

        let event = replica_b.send(TORegister::Write("d")).unwrap();
        replica_a.receive(event);

        assert_eq!(replica_a.query(Read::new()), Some("d"));
        assert_eq!(replica_b.query(Read::new()), Some("d"));

        let event_a = replica_a.send(TORegister::Write("a")).unwrap();
        let event_aa = replica_a.send(TORegister::Write("aa")).unwrap();

        let event_b = replica_b.send(TORegister::Write("b")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);
        replica_b.receive(event_aa);

        let result = Some("b");
        let eval_a = replica_a.query(Read::new());
        let eval_b = replica_b.query(Read::new());
        assert_eq!(eval_a, result);
        assert_eq!(eval_a, eval_b);
    }

    #[test]
    fn to_register_instability() {
        let (mut replica_a, mut replica_b) = twins::<TORegister<u32>>();

        let event_a_1 = replica_a.send(TORegister::Write(4)).unwrap();
        assert_eq!(replica_a.query(Read::new()), Some(4));
        let event_b_1 = replica_b.send(TORegister::Write(5)).unwrap();
        assert_eq!(replica_b.query(Read::new()), Some(5));
        replica_a.receive(event_b_1);
        assert_eq!(replica_a.query(Read::new()), Some(5));

        let event_b_2 = replica_b.send(TORegister::Write(2)).unwrap();
        assert_eq!(replica_b.query(Read::new()), Some(2));
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);

        assert_eq!(replica_a.query(Read::new()), Some(4));
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }
}
