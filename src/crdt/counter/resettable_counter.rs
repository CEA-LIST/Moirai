use std::{
    fmt::{Debug, Display},
    ops::{Add, AddAssign, SubAssign},
};

use crate::protocol::{
    crdt::pure_crdt::{PureCRDT, RedundancyRelation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::stable_state::IsStableState,
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
pub enum Counter<V: Add + AddAssign + SubAssign + Default + Copy> {
    Inc(V),
    Dec(V),
    Reset,
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
            _ => {}
        }
    }

    fn clear(&mut self) {
        *self = V::default();
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<Counter<V>>,
        tagged_op: &TaggedOp<Counter<V>>,
    ) {
        if let Counter::Reset = tagged_op.op() {
            <V as IsStableState<Counter<V>>>::clear(self);
        }
    }
}

impl<V> PureCRDT for Counter<V>
where
    V: Add<Output = V> + AddAssign + SubAssign + Default + Copy + Debug + PartialEq,
{
    type StableState = V;
    type Value = V;
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
                _ => {}
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
            Counter::Reset => write!(f, "Reset"),
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::{
//         crdt::{
//             counter::resettable_counter::Counter,
//             test_util::{triplet, twins},
//         },
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
//     pub fn stable_counter() {
//         let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<Counter<isize>>>();

//         let event = tcsb_a.tc_bcast(Counter::Dec(1));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_a.tc_bcast(Counter::Inc(2));
//         tcsb_b.try_deliver(event);

//         let event = tcsb_b.tc_bcast(Counter::Inc(3));
//         tcsb_a.try_deliver(event);

//         let event = tcsb_b.tc_bcast(Counter::Inc(4));
//         tcsb_a.try_deliver(event);

//         let event = tcsb_a.tc_bcast(Counter::Inc(5));
//         tcsb_b.try_deliver(event);

//         let result = 13;
//         assert_eq!(tcsb_a.eval(), result);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }

//     #[test_log::test]
//     pub fn concurrent_counter() {
//         let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<EventGraph<Counter<isize>>>();

//         let event_a_1 = tcsb_a.tc_bcast(Counter::Dec(1));
//         tcsb_b.try_deliver(event_a_1.clone());

//         let event_b_1 = tcsb_b.tc_bcast(Counter::Reset);
//         let event_c_1 = tcsb_c.tc_bcast(Counter::Inc(18));

//         tcsb_a.try_deliver(event_b_1.clone());
//         tcsb_a.try_deliver(event_c_1.clone());

//         tcsb_b.try_deliver(event_c_1);

//         tcsb_c.try_deliver(event_b_1);
//         tcsb_c.try_deliver(event_a_1);

//         let result = 18;
//         assert_eq!(tcsb_a.eval(), result);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//         assert_eq!(tcsb_a.eval(), tcsb_c.eval());
//     }

//     #[cfg(feature = "utils")]
//     #[test_log::test]
//     fn convergence_check() {
//         use crate::{
//             protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
//         };

//         convergence_checker::<EventGraph<Counter<isize>>>(
//             &[Counter::Inc(7), Counter::Dec(15), Counter::Reset],
//             -8,
//             |a, b| a == b,
//         );
//     }

//     #[cfg(feature = "op_weaver")]
//     #[test_log::test]
//     fn op_weaver_resettable_counter() {
//         use crate::{
//             protocol::event_graph::EventGraph,
//             utils::op_weaver::{op_weaver, EventGraphConfig},
//         };

//         let ops: Vec<Counter<isize>> = vec![
//             Counter::Inc(1),
//             Counter::Dec(1),
//             Counter::Inc(2),
//             Counter::Dec(2),
//             Counter::Inc(3),
//             Counter::Dec(3),
//             Counter::Reset,
//         ];

//         let config = EventGraphConfig {
//             name: "resettable_counter",
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
