use std::{cmp::Ordering, convert::Infallible, fmt::Debug, hash::Hash};

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
pub enum PORegister<V> {
    Clear,
    Write(V),
}

impl<V> ReplicatedDataType for PORegister<V>
where
    V: Debug + PartialOrd + Clone + Eq + PartialEq + Hash,
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
        matches!(new_tagged_op.op(), PORegister::Clear)
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

impl<V, U> UsesUnstableService<U> for PORegister<V>
where
    V: Debug + PartialOrd + Clone + Eq + PartialEq + Hash,
    U: IsUnstableCore<Self>,
{
}

impl<V, U> Eval<Read<<Self as ReplicatedDataType>::Value>, U> for PORegister<V>
where
    V: Debug + PartialOrd + Clone + Eq + PartialEq + Hash,
    U: IsUnstableCore<Self>,
{
    fn execute_query(
        _q: Read<<Self as ReplicatedDataType>::Value>,
        stable: &<PORegister<V> as ReplicatedDataType>::StableState,
        unstable: &U,
    ) -> <Read<<Self as ReplicatedDataType>::Value> as QueryOperation>::Response {
        // The set can contain only incomparable values
        let mut set = HashSet::<V>::default();
        for o in stable.iter().chain(unstable.iter().map(|to| to.op())) {
            if let PORegister::Write(v) = o {
                // We add the value if there is no v' in the set that is superior to v
                // We remove any v' in the set that is inferior to v
                if !set.iter().any(|v2| v2 > v) {
                    set.retain(|v2| !matches!(v2.partial_cmp(v), Some(Ordering::Less)));
                    set.insert(v.clone());
                }
            }
        }
        set
    }
}

#[cfg(feature = "fuzz")]
impl<V> OpGenerator for PORegister<V>
where
    V: Clone + Debug + Eq + Hash + ValueGenerator + PartialEq + PartialOrd,
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
    use std::cmp::Ordering;

    use moirai_protocol::{crdt::query::Read, replica::IsReplica};

    use crate::{
        register::po_register::PORegister,
        utils::{membership::twins, set_from_slice},
    };

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_po_register() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        type Log = VecLog<PORegister<i32>>;
        let runs = vec![RunConfig::new(0.4, 8, 1_000, None, None, false, false)];
        let config = FuzzerConfig::<Log>::new("po_register", runs, true, |a, b| a == b, false);
        fuzzer::<Log>(config);
    }

    #[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
    pub enum Family {
        Parent(u32), // Age
        #[default]
        Child,
    }

    impl PartialOrd for Family {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            match (self, other) {
                (Family::Parent(age1), Family::Parent(age2)) => {
                    if age1 == age2 {
                        Some(Ordering::Equal)
                    } else {
                        None
                    }
                }
                (Family::Parent(_), Family::Child) => Some(Ordering::Greater),
                (Family::Child, Family::Parent(_)) => Some(Ordering::Less),
                (Family::Child, Family::Child) => None,
            }
        }
    }

    #[test]
    fn simple_po_register() {
        let (mut replica_a, mut replica_b) = twins::<PORegister<Family>>();

        let event = replica_a.send(PORegister::Write(Family::Child)).unwrap();
        replica_b.receive(event);

        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Child])
        );
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Child])
        );

        let event = replica_b
            .send(PORegister::Write(Family::Parent(20)))
            .unwrap();
        replica_a.receive(event);

        let result = set_from_slice(&[Family::Parent(20)]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn simple_po_register_2() {
        let (mut replica_a, mut replica_b) = twins::<PORegister<Family>>();

        let event = replica_a
            .send(PORegister::Write(Family::Parent(20)))
            .unwrap();
        replica_b.receive(event);

        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(20)])
        );
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Parent(20)])
        );

        let event = replica_b.send(PORegister::Write(Family::Child)).unwrap();
        replica_a.receive(event);

        let result = set_from_slice(&[Family::Child]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn concurrent_po_register() {
        let (mut replica_a, mut replica_b) = twins::<PORegister<Family>>();

        let event_a = replica_a
            .send(PORegister::Write(Family::Parent(20)))
            .unwrap();
        let event_b = replica_b
            .send(PORegister::Write(Family::Parent(21)))
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let result = set_from_slice(&[Family::Parent(20), Family::Parent(21)]);
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn po_register_instability() {
        let (mut replica_a, mut replica_b) = twins::<PORegister<Family>>();

        let event_a_1 = replica_a.send(PORegister::Write(Family::Child)).unwrap();
        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Child])
        );
        let event_b_1 = replica_b
            .send(PORegister::Write(Family::Parent(42)))
            .unwrap();
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Parent(42)])
        );
        replica_a.receive(event_b_1);
        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(42)])
        );

        let event_b_2 = replica_b
            .send(PORegister::Write(Family::Parent(21)))
            .unwrap();
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Parent(21)])
        );
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);

        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(21)])
        );
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn po_register_instability_2() {
        let (mut replica_a, mut replica_b) = twins::<PORegister<Family>>();

        let event_a_1 = replica_a
            .send(PORegister::Write(Family::Parent(20)))
            .unwrap();
        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(20)])
        );
        let event_b_1 = replica_b
            .send(PORegister::Write(Family::Parent(42)))
            .unwrap();
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Parent(42)])
        );
        replica_a.receive(event_b_1);
        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(42), Family::Parent(20)])
        );

        let event_b_2 = replica_b.send(PORegister::Write(Family::Child)).unwrap();
        assert_eq!(
            replica_b.query(Read::new()),
            set_from_slice(&[Family::Child])
        );
        replica_a.receive(event_b_2);
        replica_b.receive(event_a_1);

        assert_eq!(
            replica_a.query(Read::new()),
            set_from_slice(&[Family::Parent(20)])
        );
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }
}
