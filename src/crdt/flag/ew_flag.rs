use std::fmt::Debug;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::{
    clocks::dot::Dot,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT, stable::Stable},
};

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
pub enum EWFlag {
    Enable,
    Disable,
    Clear,
}

impl Stable<EWFlag> for bool {
    fn is_default(&self) -> bool {
        !(*self)
    }

    fn apply_redundant(
        &mut self,
        _rdnt: fn(&EWFlag, Option<&Dot>, bool, &EWFlag, &Dot) -> bool,
        _op: &EWFlag,
        _dot: &Dot,
    ) {
        *self = false; //Clear the flag in all cases because it will be re-evaluated later
                       // old implementation: It will not cause any problems,
                       // but it doesn't comply to the function goal
                       // match op {
                       //     EWFlag::Enable => *self = true,
                       //     EWFlag::Disable | EWFlag::Clear => *self = false,
                       // }
    }

    fn apply(&mut self, value: EWFlag) {
        match value {
            EWFlag::Enable => *self = true,
            EWFlag::Disable | EWFlag::Clear => *self = false,
        }
    }
}

impl PureCRDT for EWFlag {
    type Value = bool;
    type Stable = bool;
    // const DISABLE_R_WHEN_R: bool = true;
    // const DISABLE_R_WHEN_NOT_R: bool = true;

    fn redundant_itself(new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        matches!(new_op, EWFlag::Disable | EWFlag::Clear)
    }
    // return true if the old op must be redundant by the new op
    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_dot: Option<&Dot>,
        is_conc: bool,
        _new_op: &Self,
        _new_dot: &Dot,
    ) -> bool {
        // any new operation with larger timestamp makes the previous ones redundant
        !is_conc
    }

    fn redundant_by_when_not_redundant(
        old_op: &Self,
        old_dot: Option<&Dot>,
        is_conc: bool,
        new_op: &Self,
        new_dot: &Dot,
    ) -> bool {
        Self::redundant_by_when_redundant(old_op, old_dot, is_conc, new_op, new_dot)
    }
    // takes a variable of type bool and array of ......
    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
        let mut flag = *stable;
        for op in unstable.iter() {
            if let EWFlag::Enable = op {
                flag = true;
            }
        }
        flag
    }

    fn stabilize(_dot: &Dot, _state: &mut EventGraph<Self>) {}
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{flag::ew_flag::EWFlag, test_util::twins},
        protocol::event_graph::EventGraph,
    };

    // Test the Enable-Wins Flag CRDT using two replicas (twins)
    #[test_log::test]
    fn enable_wins_flag() {
        let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<EWFlag>>();

        // Replica A enables the flag
        let event = tcsb_a.tc_bcast(EWFlag::Enable);
        tcsb_b.try_deliver(event);
        assert_eq!(tcsb_a.eval(), true);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());

        // Replica B disables the flag
        let event = tcsb_b.tc_bcast(EWFlag::Disable);
        tcsb_a.try_deliver(event);
        assert_eq!(tcsb_b.eval(), false);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());

        // Replica A enables again
        let event = tcsb_a.tc_bcast(EWFlag::Enable);
        tcsb_b.try_deliver(event);
        assert_eq!(tcsb_a.eval(), true);
        assert_eq!(tcsb_a.eval(), tcsb_b.eval());
        // Concurrent Enable and Disable: Disable wins
        let event_a = tcsb_a.tc_bcast(EWFlag::Enable);
        let event_b = tcsb_b.tc_bcast(EWFlag::Disable);
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);
        assert_eq!(tcsb_a.eval(), true);
        assert_eq!(tcsb_b.eval(), true);
    }

    #[cfg(feature = "op_weaver")]
    #[test_log::test]
    fn op_weaver_ew_flag() {
        use crate::utils::op_weaver::{op_weaver, EventGraphConfig};

        let ops = vec![EWFlag::Enable, EWFlag::Disable, EWFlag::Clear];

        let config = EventGraphConfig {
            name: "ewflag",
            num_replicas: 8,
            num_operations: 10_000,
            operations: &ops,
            final_sync: true,
            churn_rate: 0.3,
            reachability: None,
            compare: |a: &bool, b: &bool| a == b,
            record_results: true,
            seed: None,
            witness_graph: false,
            concurrency_score: false,
        };

        op_weaver::<EventGraph<EWFlag>>(config);
    }
}
