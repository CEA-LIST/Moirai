use std::{
    fmt::{Debug, Display},
    ops::{Add, AddAssign, SubAssign},
};

#[cfg(feature = "fuzz")]
use moirai_fuzz::op_generator::OpGenerator;
use moirai_protocol::{
    crdt::{
        eval::Eval,
        pure_crdt::PureCRDT,
        query::{QueryOperation, Read},
    },
    state::unstable_state::IsUnstableState,
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
pub enum Counter<V: Add + AddAssign + SubAssign + Default + Copy> {
    Inc(V),
    Dec(V),
}

impl<V> PureCRDT for Counter<V>
where
    V: Add + AddAssign + SubAssign + Default + Copy + Debug + PartialEq,
{
    type Value = V;
    type StableState = CounterStable<V>;

    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_R_WHEN_NOT_R: bool = true;
}

impl<V> Eval<Read<<Self as PureCRDT>::Value>> for Counter<V>
where
    V: Add + AddAssign + SubAssign + Default + Copy + Debug + PartialEq,
{
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Read<<Self as PureCRDT>::Value> as QueryOperation>::Response {
        let mut counter = *stable;
        for op in unstable.iter().map(|t| t.op()) {
            match op {
                Counter::Dec(v) => *counter.as_inner_mut() -= *v,
                Counter::Inc(v) => *counter.as_inner_mut() += *v,
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
        }
    }
}

#[cfg(feature = "fuzz")]
impl OpGenerator for Counter<i32> {
    type Config = ();

    fn generate(
        rng: &mut impl Rng,
        _config: &Self::Config,
        _stable: &<Self as PureCRDT>::StableState,
        _unstable: &impl IsUnstableState<Self>,
    ) -> Self {
        let choice = ["Inc", "Dec"][rng.next_u32() as usize % 2];
        match choice {
            "Inc" => Counter::Inc(rng.next_u32() as i32),
            "Dec" => Counter::Dec(rng.next_u32() as i32),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{crdt::query::Read, replica::IsReplica};

    use crate::{counter::simple_counter::Counter, utils::membership::twins};

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
    pub fn simple_counter_2() {
        let (mut replica_a, mut replica_b) = twins::<Counter<isize>>();

        let event = replica_a.send(Counter::Dec(5)).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(Counter::Inc(5)).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(Counter::Inc(5)).unwrap();
        replica_b.receive(event);

        let result = 5;
        assert_eq!(replica_a.query(Read::new()), result);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_resettable_counter() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run = RunConfig::new(0.4, 8, 100_000, None, None, false, false);
        let runs = vec![run.clone(); 1];

        let config =
            FuzzerConfig::<VecLog<Counter<i32>>>::new("counter", runs, true, |a, b| a == b, true);

        fuzzer::<VecLog<Counter<i32>>>(config);
    }
}
