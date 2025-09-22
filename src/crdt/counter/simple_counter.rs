use std::{
    fmt::{Debug, Display},
    ops::{Add, AddAssign, SubAssign},
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::protocol::{
    crdt::pure_crdt::{PureCRDT, RedundancyRelation},
    event::tagged_op::TaggedOp,
    state::{stable_state::IsStableState, unstable_state::IsUnstableState},
};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
pub enum Counter<V: Add + AddAssign + SubAssign + Default + Copy> {
    Inc(V),
    Dec(V),
}

impl<V> IsStableState<Counter<V>> for V
where
    V: Add + AddAssign + SubAssign + Default + Copy + Debug + PartialEq,
{
    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        *self == V::default()
    }

    fn apply(&mut self, value: Counter<V>) {
        match value {
            Counter::Inc(v) => *self += v,
            Counter::Dec(v) => *self -= v,
        }
    }

    fn clear(&mut self) {
        *self = V::default();
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<Counter<V>>,
        _tagged_op: &TaggedOp<Counter<V>>,
    ) {
    }
}

impl<V> PureCRDT for Counter<V>
where
    V: Add + AddAssign + SubAssign + Default + Copy + Debug + PartialEq,
{
    type Value = V;
    type StableState = V;
    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_R_WHEN_NOT_R: bool = true;

    fn eval(stable: &Self::StableState, unstable: &impl IsUnstableState<Self>) -> Self::Value {
        let mut counter = *stable;
        for op in unstable.iter().map(|t| t.op()) {
            match op {
                Counter::Inc(v) => counter += *v,
                Counter::Dec(v) => counter -= *v,
            }
        }
        counter
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

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{counter::simple_counter::Counter, test_util::twins},
        protocol::replica::IsReplica,
    };

    #[test]
    pub fn simple_counter() {
        let (mut replica_a, mut replica_b) = twins::<Counter<isize>>();

        let event = replica_a.send(Counter::Dec(5)).unwrap();
        replica_b.receive(event);

        let event = replica_a.send(Counter::Inc(5)).unwrap();
        replica_b.receive(event);

        let result = 0;
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
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
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    //     #[test]
    //     fn convergence_checker() {
    //         // TODO: Implement a convergence checker for Counter
    //     }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_resettable_counter() {
        use crate::{
            // crdt::test_util::init_tracing,
            fuzz::{
                config::{FuzzerConfig, OpConfig, RunConfig},
                fuzzer,
            },
            protocol::state::po_log::VecLog,
        };

        // init_tracing();

        let ops = OpConfig::Uniform(&[Counter::Inc(1), Counter::Dec(1)]);

        let run = RunConfig::new(0.4, 8, 100_000, None, None);
        let runs = vec![run.clone(); 1];

        let config = FuzzerConfig::<VecLog<Counter<i32>>>::new(
            "counter",
            runs,
            ops,
            true,
            |a, b| a == b,
            None,
        );

        fuzzer::<VecLog<Counter<i32>>>(config);
    }
}
