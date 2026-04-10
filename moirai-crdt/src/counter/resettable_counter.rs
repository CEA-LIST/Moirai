use std::{
    fmt::{Debug, Display},
    ops::{Add, AddAssign, SubAssign},
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{op_generator::OpGenerator, value_generator::ValueGenerator};
use moirai_protocol::{
    crdt::{
        eval::Eval,
        pure_crdt::PureCRDT,
        query::{QueryOperation, Read},
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableState,
    utils::intern_str::{InternalizeOp, Interner},
};
#[cfg(feature = "fuzz")]
use rand::Rng;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::counter::stable::CounterStable;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum Counter<V: Add + AddAssign + SubAssign + Default + Copy> {
    Inc(V),
    Dec(V),
    Reset,
}

impl<V> PureCRDT for Counter<V>
where
    V: Add<Output = V> + AddAssign + SubAssign + Default + Copy + Debug + PartialEq,
{
    type Value = V;
    type StableState = CounterStable<V>;

    const DISABLE_R_WHEN_NOT_R: bool = true;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(new_tagged_op.op(), Counter::Reset)
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_event_id: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        !is_conc && matches!(new_tagged_op.op(), Counter::Reset)
    }
}

impl<V> Eval<Read<<Self as PureCRDT>::Value>> for Counter<V>
where
    V: Add<Output = V> + AddAssign + SubAssign + Default + Copy + Debug + PartialEq,
{
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Read<<Self as PureCRDT>::Value> as QueryOperation>::Response {
        let mut counter = *stable;
        for op in unstable.iter().map(|t| t.op()) {
            match op {
                Counter::Inc(v) => *counter.as_inner_mut() += *v,
                Counter::Dec(v) => *counter.as_inner_mut() -= *v,
                Counter::Reset => unreachable!(),
            }
        }
        *counter.as_inner()
    }
}

impl<V> Display for Counter<V>
where
    V: Add + AddAssign + SubAssign + Default + Copy + Debug + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Counter::Inc(v) => write!(f, "Inc({v})"),
            Counter::Dec(v) => write!(f, "Dec({v})"),
            Counter::Reset => write!(f, "Reset"),
        }
    }
}

#[cfg(feature = "fuzz")]
impl<V> OpGenerator for Counter<V>
where
    V: Add<Output = V>
        + AddAssign
        + SubAssign
        + Default
        + Copy
        + Debug
        + PartialEq
        + ValueGenerator,
{
    type Config = ();

    fn generate(
        rng: &mut impl Rng,
        _config: &Self::Config,
        _stable: &<Self as PureCRDT>::StableState,
        _unstable: &impl IsUnstableState<Self>,
    ) -> Self {
        enum Choice {
            Inc,
            Dec,
            Reset,
        }
        let choice = rand::seq::IteratorRandom::choose(
            [Choice::Inc, Choice::Dec, Choice::Reset].iter(),
            rng,
        )
        .unwrap();
        let value = V::generate(rng, &<V as ValueGenerator>::Config::default());
        match choice {
            Choice::Inc => Counter::Inc(value),
            Choice::Dec => Counter::Dec(value),
            Choice::Reset => Counter::Reset,
        }
    }
}

impl<V> InternalizeOp for Counter<V>
where
    V: Add<Output = V> + AddAssign + SubAssign + Default + Copy + Debug + PartialEq,
{
    fn internalize(self, _interner: &Interner) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{crdt::query::Read, replica::IsReplica};

    use crate::{
        counter::resettable_counter::Counter,
        utils::membership::{triplet, twins},
    };

    #[test]
    pub fn simple_counter() {
        let (mut replica_a, mut replica_b) = twins::<Counter<isize>>();

        let event = replica_a.send(Counter::Dec(5)).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(Counter::Inc(5)).unwrap();
        replica_b.receive(event);

        let result = 0;
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    pub fn stable_counter() {
        let (mut replica_a, mut replica_b) = twins::<Counter<isize>>();

        let event = replica_a.send(Counter::Dec(1)).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(Counter::Inc(2)).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(Counter::Inc(3)).unwrap();
        replica_a.receive(event);

        let event = replica_b.send(Counter::Inc(4)).unwrap();
        replica_a.receive(event);

        let result = 8;
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));

        let event = replica_a.send(Counter::Inc(5)).unwrap();
        replica_b.receive(event);

        let result = 13;
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_b.query(Read::new()), result);
    }

    #[test]
    pub fn concurrent_counter() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet::<Counter<f64>>();

        let event_a_1 = replica_a.send(Counter::Dec(1.0)).unwrap();
        replica_b.receive(event_a_1.clone());

        let event_b_1 = replica_b.send(Counter::Reset).unwrap();
        let event_c_1 = replica_c.send(Counter::Inc(18.0)).unwrap();

        replica_a.receive(event_b_1.clone());
        replica_a.receive(event_c_1.clone());

        replica_b.receive(event_c_1);

        replica_c.receive(event_b_1);
        replica_c.receive(event_a_1);

        let result = 18.0;
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
        assert_eq!(replica_a.query(Read::new()), replica_c.query(Read::new()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_resettable_counter() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run = RunConfig::new(0.4, 8, 1_000, None, None, false, false);
        let runs = vec![run.clone(); 1];

        let config = FuzzerConfig::<VecLog<Counter<i32>>>::new(
            "resettable_counter",
            runs,
            true,
            |a, b| a == b,
            false,
        );

        fuzzer::<VecLog<Counter<i32>>>(config);
    }
}
