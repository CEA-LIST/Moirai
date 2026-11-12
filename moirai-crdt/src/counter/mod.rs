mod stable;
pub mod update;

use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::{Add, AddAssign, SubAssign},
};

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
use rand::Rng;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::counter::{stable::CounterStable, update::CounterUpdate};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Counter<V>(PhantomData<V>);

impl<V> ReplicatedDataType for Counter<V>
where
    V: CounterValue,
{
    type Update = CounterUpdate<V>;
    type Stable = CounterStable<V>;
    type Rejection = Infallible;
}

impl<U, V> CausalReset<U> for Counter<V> where V: CounterValue {}

impl<U, V> CausalStabilize<U> for Counter<V> where V: CounterValue {}

impl<U, V> CausalCompaction<U> for Counter<V>
where
    V: CounterValue,
    U: IsUnstableCore<CounterUpdate<V>>,
{
    fn compact_stable(
        stable: &mut Self::Stable,
        _: RedundancyRelation<Self::Update>,
        new: &TaggedOp<Self::Update>,
    ) {
        if matches!(new.op(), CounterUpdate::Reset) {
            *stable = Default::default();
        }
    }

    const DISABLE_R_WHEN_NOT_R: bool = true;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self::Update>,
        _stable: &Self::Stable,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self::Update>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(new_tagged_op.op(), CounterUpdate::Reset)
    }

    fn redundant_by_when_redundant(
        _old_op: &Self::Update,
        _old_event_id: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        !is_conc && matches!(new_tagged_op.op(), CounterUpdate::Reset)
    }
}

impl<V, U> Eval<U, Read<V>> for Counter<V>
where
    V: CounterValue,
    U: IsUnstableCore<CounterUpdate<V>>,
{
    fn execute_query(
        stable: &CounterStable<V>,
        unstable: &U,
        _q: &Read<V>,
    ) -> <Read<V> as QueryOperation>::Response {
        let mut counter = stable.clone();
        for op in unstable.iter().map(|t| t.op()) {
            match op {
                CounterUpdate::Inc(v) => V::increment(counter.as_inner_mut(), *v),
                CounterUpdate::Dec(v) => V::decrement(counter.as_inner_mut(), *v),
                CounterUpdate::Reset => unreachable!(),
            }
        }
        V::value(counter.as_inner())
    }
}

impl<V> Display for CounterUpdate<V>
where
    V: Add + AddAssign + SubAssign + Default + Copy + Debug + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inc(v) => write!(f, "Inc({v})"),
            Self::Dec(v) => write!(f, "Dec({v})"),
            Self::Reset => write!(f, "Reset"),
        }
    }
}

/// Numeric values supported by the resettable counter.
///
/// Integers accumulate directly. Floating-point values use exact `bitrep`
/// accumulators and round only when the counter is read.
/// Implement this trait to select an accumulator for a custom numeric type.
pub trait CounterValue:
    Add<Output = Self> + AddAssign + SubAssign + Default + Copy + Debug + PartialEq
{
    /// Stored sum
    type Accumulator: Clone + Debug + Default;

    fn increment(accumulator: &mut Self::Accumulator, value: Self);

    fn decrement(accumulator: &mut Self::Accumulator, value: Self);

    /// Read the sum, rounding to the numeric type if needed.
    fn value(accumulator: &Self::Accumulator) -> Self;
}

macro_rules! integer_counter_values {
    ($($integer:ty),+ $(,)?) => {
        $(
            impl CounterValue for $integer {
                type Accumulator = Self;

                fn increment(accumulator: &mut Self, value: Self) {
                    *accumulator += value;
                }

                fn decrement(accumulator: &mut Self, value: Self) {
                    *accumulator -= value;
                }

                fn value(accumulator: &Self) -> Self {
                    *accumulator
                }
            }
        )+
    };
}

integer_counter_values!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
);

macro_rules! float_counter_value {
    ($float:ty, $accumulator:ident, $sum:ty) => {
        #[doc(hidden)]
        #[derive(Clone, Debug, Default, PartialEq, Eq)]
        pub struct $accumulator($sum);

        #[cfg(feature = "test_utils")]
        impl DeepSizeOf for $accumulator {
            fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
                // bitrep's fixed-size accumulator has no heap allocations.
                0
            }
        }

        impl CounterValue for $float {
            type Accumulator = $accumulator;

            fn increment(accumulator: &mut Self::Accumulator, value: Self) {
                accumulator.0.add(value);
            }

            fn decrement(accumulator: &mut Self::Accumulator, value: Self) {
                accumulator.0.add(-value);
            }

            fn value(accumulator: &Self::Accumulator) -> Self {
                accumulator.0.value()
            }
        }
    };
}

float_counter_value!(f32, F32Accumulator, bitrep::SumF32);
float_counter_value!(f64, F64Accumulator, bitrep::SumF64);

#[cfg(feature = "fuzz")]
impl<V, U> OpGenerator<U> for Counter<V>
where
    U: IsUnstableCore<CounterUpdate<V>>,
    V: CounterValue + ValueGenerator,
{
    type Config = ();

    fn generate(
        rng: &mut impl Rng,
        _config: &Self::Config,
        _stable: &<Self as ReplicatedDataType>::Stable,
        _unstable: &U,
    ) -> Self::Update {
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
            Choice::Inc => CounterUpdate::Inc(value),
            Choice::Dec => CounterUpdate::Dec(value),
            Choice::Reset => CounterUpdate::Reset,
        }
    }
}

impl<V: CounterValue, U> Precondition<U> for Counter<V> {
    fn is_enabled(_: &Self::Stable, _: &U, _: &Self::Update) -> Result<(), Infallible> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{data_type::query::Read, replica::IsReplica};

    use crate::{
        counter::{Counter, update::CounterUpdate},
        utils::bootstrap::{triplet, twins},
    };

    #[test]
    pub fn simple_counter() {
        let (mut replica_a, mut replica_b) = twins::<Counter<isize>>();

        let event = replica_a.send(CounterUpdate::Dec(5)).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(CounterUpdate::Inc(5)).unwrap();
        replica_b.receive(event);

        let result = 0;
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }

    #[test]
    pub fn stable_counter() {
        let (mut replica_a, mut replica_b) = twins::<Counter<isize>>();

        let event = replica_a.send(CounterUpdate::Dec(1)).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(CounterUpdate::Inc(2)).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(CounterUpdate::Inc(3)).unwrap();
        replica_a.receive(event);

        let event = replica_b.send(CounterUpdate::Inc(4)).unwrap();
        replica_a.receive(event);

        let result = 8;
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));

        let event = replica_a.send(CounterUpdate::Inc(5)).unwrap();
        replica_b.receive(event);

        let result = 13;
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_b.query(&Read::new()), result);
    }

    #[test]
    pub fn concurrent_counter() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet::<Counter<f64>>();

        let event_a_1 = replica_a.send(CounterUpdate::Dec(1.0)).unwrap();
        replica_b.receive(event_a_1.clone());

        let event_b_1 = replica_b.send(CounterUpdate::Reset).unwrap();
        let event_c_1 = replica_c.send(CounterUpdate::Inc(18.0)).unwrap();

        replica_a.receive(event_b_1.clone());
        replica_a.receive(event_c_1.clone());

        replica_b.receive(event_c_1);

        replica_c.receive(event_b_1);
        replica_c.receive(event_a_1);

        let result = 18.0;
        assert_eq!(replica_a.query(&Read::new()), result);
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
        assert_eq!(replica_a.query(&Read::new()), replica_c.query(&Read::new()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_resettable_counter_int() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run = RunConfig::new(ChurnConfig::new(0.4, 0.6), 8, 1_000, None, None);
        let runs = vec![run; 1];

        let config = FuzzerConfig::<VecLog<Counter<i32>>, Read<i32>>::new(
            "resettable_counter",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<VecLog<Counter<i32>>, Read<i32>>(config);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_resettable_counter_float() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run = RunConfig::new(ChurnConfig::new(0.4, 0.6), 8, 1_000, None, None);
        let runs = vec![run; 1];

        let config = FuzzerConfig::<VecLog<Counter<f64>>, Read<f64>>::new(
            "resettable_counter",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<VecLog<Counter<f64>>, Read<f64>>(config);
    }
}
