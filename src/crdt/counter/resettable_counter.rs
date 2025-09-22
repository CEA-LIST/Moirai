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
    event::{tag::Tag, tagged_op::TaggedOp},
    state::{stable_state::IsStableState, unstable_state::IsUnstableState},
};

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
            Counter::Reset => unreachable!(),
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

    fn eval(stable: &Self::StableState, unstable: &impl IsUnstableState<Self>) -> Self::Value {
        let mut counter = *stable;
        for op in unstable.iter().map(|t| t.op()) {
            match op {
                Counter::Inc(v) => counter += *v,
                Counter::Dec(v) => counter -= *v,
                Counter::Reset => unreachable!(),
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

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{
            counter::resettable_counter::Counter,
            test_util::{triplet, twins},
        },
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

        let event = replica_a.send(Counter::Inc(5)).unwrap();
        replica_b.receive(event);

        let result = 13;
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    pub fn concurrent_counter() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet::<Counter<isize>>();

        let event_a_1 = replica_a.send(Counter::Dec(1)).unwrap();
        replica_b.receive(event_a_1.clone());

        let event_b_1 = replica_b.send(Counter::Reset).unwrap();
        let event_c_1 = replica_c.send(Counter::Inc(18)).unwrap();

        replica_a.receive(event_b_1.clone());
        replica_a.receive(event_c_1.clone());

        replica_b.receive(event_c_1);

        replica_c.receive(event_b_1);
        replica_c.receive(event_a_1);

        let result = 18;
        assert_eq!(replica_a.query(), result);
        assert_eq!(replica_a.query(), replica_b.query());
        assert_eq!(replica_a.query(), replica_c.query());
    }

    // #[cfg(feature = "utils")]
    // #[test]
    // fn convergence_check() {
    //     use crate::{
    //         protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
    //     };

    //     convergence_checker::<EventGraph<Counter<isize>>>(
    //         &[Counter::Inc(7), Counter::Dec(15), Counter::Reset],
    //         -8,
    //         |a, b| a == b,
    //     );
    // }

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

        let ops = OpConfig::Uniform(&[Counter::Inc(1), Counter::Dec(1), Counter::Reset]);

        let run = RunConfig::new(0.4, 8, 100_000, None, None);
        let runs = vec![run.clone(); 1];

        let config = FuzzerConfig::<VecLog<Counter<i32>>>::new(
            "resettable_counter",
            runs,
            ops,
            true,
            |a, b| a == b,
            None,
        );

        fuzzer::<VecLog<Counter<i32>>>(config);
    }
}
