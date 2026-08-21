use std::{cmp::Ordering, convert::Infallible, fmt::Debug, marker::PhantomData};

#[cfg(feature = "fuzz")]
use moirai_fuzz::{op_generator::OpGenerator, value_generator::ValueGenerator};
use moirai_protocol::{
    crdt::{
        eval::Eval,
        policy::{FairPolicy, LwwPolicy, Policy},
        query::{QueryOperation, Read},
        replicated_data_type::{ReplicatedDataType, UsesUnstableService},
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableCore,
};
#[cfg(feature = "fuzz")]
use rand::{Rng, RngExt};

pub type LwwRegister<V> = Register<V, LwwPolicy>;
pub type FairRegister<V> = Register<V, FairPolicy>;

#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize, tsify::Tsify)
)]
pub enum Register<V, P> {
    Write(V),
    Clear,
    // TODO: find a better design pattern
    __Marker(std::convert::Infallible, PhantomData<P>),
}

impl<V, P> ReplicatedDataType for Register<V, P>
where
    V: Debug + Clone,
    P: Policy,
{
    type Value = Option<V>;
    type StableState = Vec<Self>;
    type Rejection = Infallible;

    const DISABLE_R_WHEN_R: bool = true;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        mut unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        match new_tagged_op.op() {
            Register::Clear => true,
            Register::Write(_) => unstable.any(|old_tagged_op| {
                P::compare(new_tagged_op.tag(), old_tagged_op.tag()) == Ordering::Less
            }),
            _ => unreachable!(),
        }
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
        old_tag: Option<&Tag>,
        _is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        if let Some(old_tag) = old_tag {
            P::compare(new_tagged_op.tag(), old_tag) == Ordering::Greater
        } else {
            true
        }
    }
}

impl<V, P, U> UsesUnstableService<U> for Register<V, P>
where
    V: Debug + Clone,
    P: Policy,
    U: IsUnstableCore<Self>,
{
}

impl<V, P, U> Eval<Read<<Self as ReplicatedDataType>::Value>, U> for Register<V, P>
where
    V: Debug + Clone,
    P: Policy,
    U: IsUnstableCore<Self>,
{
    fn execute_query(
        _q: Read<<Self as ReplicatedDataType>::Value>,
        stable: &<Register<V, P> as ReplicatedDataType>::StableState,
        unstable: &U,
    ) -> <Read<<Self as ReplicatedDataType>::Value> as QueryOperation>::Response {
        let mut value = None;
        for op in stable.iter().chain(unstable.iter().map(|t| t.op())) {
            match op {
                Register::Write(v) => value = Some(v.clone()),
                _ => unreachable!(),
            }
        }
        value
    }
}

#[cfg(feature = "fuzz")]
impl<V, P> OpGenerator for Register<V, P>
where
    P: Policy,
    V: ValueGenerator + Debug + Clone,
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
    use moirai_protocol::{
        crdt::{
            policy::{FairPolicy, LwwPolicy},
            query::Read,
        },
        replica::IsReplica,
    };

    use crate::{
        register::unique_register::Register,
        utils::membership::{triplet, twins},
    };

    #[test]
    pub fn lww_register_with_write() {
        let (mut replica_a, mut replica_b) = twins::<Register<String, LwwPolicy>>();

        let event = replica_a
            .send(Register::Write("Hello".to_string()))
            .unwrap();
        replica_b.receive(event);

        let event = replica_a
            .send(Register::Write("World".to_string()))
            .unwrap();
        replica_b.receive(event);

        let result = "World".to_string();
        assert_eq!(replica_a.query(Read::new()), Some(result));
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    pub fn lww_register_concurrent_writes() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet::<Register<String, LwwPolicy>>();

        let event_a = replica_a
            .send(Register::Write("Hello".to_string()))
            .unwrap();
        assert!(replica_a.query(Read::new()) == Some("Hello".to_string()));
        let event_b = replica_b
            .send(Register::Write("World".to_string()))
            .unwrap();
        assert!(replica_b.query(Read::new()) == Some("World".to_string()));

        replica_a.receive(event_b.clone());
        assert_eq!(replica_a.query(Read::new()), Some("World".to_string()));
        replica_b.receive(event_a.clone());
        assert_eq!(replica_b.query(Read::new()), Some("World".to_string()));
        replica_c.receive(event_a);
        assert_eq!(replica_c.query(Read::new()), Some("Hello".to_string()));
        replica_c.receive(event_b);
        assert_eq!(replica_c.query(Read::new()), Some("World".to_string()));
    }

    #[test]
    pub fn lww_register_more_concurrent() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet::<Register<String, LwwPolicy>>();

        let event_c_1 = replica_c.send(Register::Write("x".to_string())).unwrap();
        replica_a.receive(event_c_1.clone());

        let event_a_1 = replica_a.send(Register::Write("y".to_string())).unwrap();

        let event_b_1 = replica_b.send(Register::Write("z".to_string())).unwrap();
        replica_c.receive(event_b_1.clone());

        replica_b.receive(event_c_1.clone());
        replica_b.receive(event_a_1.clone());

        replica_c.receive(event_a_1.clone());
        replica_a.receive(event_b_1);

        assert_eq!(replica_a.query(Read::new()), Some("y".to_string()));
        assert_eq!(replica_b.query(Read::new()), Some("y".to_string()));
        assert_eq!(replica_c.query(Read::new()), Some("y".to_string()));
    }

    #[test]
    pub fn fair_register_concurrent() {
        let (mut replica_a, mut replica_b) = twins::<Register<String, FairPolicy>>();

        let event_a = replica_a
            .send(Register::Write("Public".to_string()))
            .unwrap();

        let event_b = replica_b
            .send(Register::Write("Protected".to_string()))
            .unwrap();

        replica_a.receive(event_b.clone());
        replica_b.receive(event_a.clone());

        assert_eq!(replica_a.query(Read::new()), Some("Public".to_string()));
        assert_eq!(replica_b.query(Read::new()), Some("Public".to_string()));

        let event_a_2 = replica_a
            .send(Register::Write("Private".to_string()))
            .unwrap();

        replica_b.receive(event_a_2.clone());

        assert_eq!(replica_a.query(Read::new()), Some("Private".to_string()));
        assert_eq!(replica_b.query(Read::new()), Some("Private".to_string()));

        let event_b_2 = replica_b
            .send(Register::Write("Protected".to_string()))
            .unwrap();

        let event_a_3 = replica_a
            .send(Register::Write("Public".to_string()))
            .unwrap();

        replica_a.receive(event_b_2.clone());
        replica_b.receive(event_a_3.clone());

        assert_eq!(replica_a.query(Read::new()), Some("Protected".to_string()));
        assert_eq!(replica_b.query(Read::new()), Some("Protected".to_string()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_lww_register() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        type Log = VecLog<Register<i32, LwwPolicy>>;
        let runs = vec![RunConfig::new(0.4, 8, 1_000, None, None, false, false)];
        let config = FuzzerConfig::<Log>::new("lww_register", runs, true, |a, b| a == b, false);
        fuzzer::<Log>(config);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_fair_register() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        type Log = VecLog<Register<i32, FairPolicy>>;
        let runs = vec![RunConfig::new(0.4, 8, 1_000, None, None, false, false)];
        let config = FuzzerConfig::<Log>::new("fair_register", runs, true, |a, b| a == b, false);
        fuzzer::<Log>(config);
    }
}
