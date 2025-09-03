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
    state::stable_state::IsStableState,
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
        // TODO: maybe len is not necessary. Is empty would be better
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

    fn eval<'a>(
        stable: &Self::StableState,
        unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> Self::Value
    where
        V: 'a,
    {
        let mut counter = *stable;
        for op in unstable.map(|t| t.op()) {
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

// #[cfg(test)]
// mod tests {
//     use crate::{
//         crdt::{counter::simple_counter::Counter, test_util::twins},
//         protocol::event_graph::EventGraph,
//     };

//     #[test_log::test]
//     pub fn simple_counter() {
//         let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<Counter<isize>>>();

//         let event = tcsb_a.tc_bcast(Counter::Dec(5));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(Counter::Inc(5));
//         tcsb_b.try_deliver(event);

//         let result = 0;
//         assert_eq!(tcsb_a.eval(), result);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }

//     #[test_log::test]
//     pub fn simple_counter_2() {
//         let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<Counter<isize>>>();

//         let event = tcsb_a.tc_bcast(Counter::Dec(5));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(Counter::Inc(5));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(Counter::Inc(5));
//         tcsb_b.try_deliver(event);

//         let result = 5;
//         assert_eq!(tcsb_a.eval(), result);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }

//     #[test_log::test]
//     fn convergence_checker() {
//         // TODO: Implement a convergence checker for Counter
//     }

//     #[cfg(feature = "op_weaver")]
//     #[test_log::test]
//     fn op_weaver_counter() {
//         use crate::utils::op_weaver::{op_weaver, EventGraphConfig};

//         let ops = vec![Counter::Inc(1), Counter::Dec(1)];

//         let config = EventGraphConfig {
//             name: "counter",
//             num_replicas: 8,
//             num_operations: 10_000,
//             operations: &ops,
//             final_sync: true,
//             churn_rate: 0.3,
//             reachability: None,
//             compare: |a: &isize, b: &isize| a == b,
//             record_results: true,
//             seed: None,
//             witness_graph: false,
//             concurrency_score: false,
//         };

//         op_weaver::<EventGraph<Counter<isize>>>(config);
//     }
// }
