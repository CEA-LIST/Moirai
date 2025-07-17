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
pub enum DWFlag {
    Enable,
    Disable,
    Clear,
}

impl Stable<DWFlag> for bool {
    fn is_default(&self) -> bool {
        !(*self)
    }

    fn apply_redundant(
        &mut self,
        _rdnt: fn(&DWFlag, Option<&Dot>, bool, &DWFlag, &Dot) -> bool,
        _op: &DWFlag,
        _dot: &Dot,
    ) {
        // No-op for redundant
    }

    fn apply(&mut self, value: DWFlag) {
        match value {
            DWFlag::Enable => *self = true,
            DWFlag::Disable | DWFlag::Clear => *self = false,
        }
    }
}

impl PureCRDT for DWFlag {
    type Value = bool;
    type Stable = bool;
    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_R_WHEN_NOT_R: bool = true;

    fn redundant_itself(new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        matches!(new_op, DWFlag::Clear)
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_dot: Option<&Dot>,
        is_conc: bool,
        _new_op: &Self,
        _new_dot: &Dot,
    ) -> bool {
        // In DWFlag, any new operation with larger timestamp makes previous ones redundant
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

    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
        let mut flag = *stable;
        // In DWFlag, any concurrent Disable wins over Enable
        for op in unstable.iter() {
            match op {
                DWFlag::Disable => {
                    flag = false;
                    break;
                }
                DWFlag::Enable => flag = true,
                _ => flag = false, // Clear does not affect the flag state
            }
        }
        flag
    }

    fn stabilize(_dot: &Dot, _state: &mut EventGraph<Self>) {}
}

// #[cfg(test)]
// mod tests {
//     use crate::{
//         crdt::{dw_flag::DWFlag, test_util::twins},
//         protocol::event_graph::EventGraph,
//     };

// Test the Disable-Wins Flag CRDT using two replicas (twins)
//     #[test_log::test]
//     fn disable_wins_flag() {
//         let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<DWFlag>>();

//         // Replica A enables the flag
//         let event = tcsb_a.tc_bcast(DWFlag::Enable);
//         tcsb_b.try_deliver(event);
//         assert_eq!(tcsb_a.eval(), true);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());

//         // Replica B disables the flag
//         let event = tcsb_b.tc_bcast(DWFlag::Disable);
//         tcsb_a.try_deliver(event);
//         assert_eq!(tcsb_b.eval(), false);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());

//         // Replica A enables again
//         let event = tcsb_a.tc_bcast(DWFlag::Enable);
//         tcsb_b.try_deliver(event);
//         assert_eq!(tcsb_a.eval(), true);
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());

//         // Concurrent Enable and Disable: Disable wins
//         let event_a = tcsb_a.tc_bcast(DWFlag::Enable);
//         let event_b = tcsb_b.tc_bcast(DWFlag::Disable);
//         tcsb_a.try_deliver(event_b.clone());
//         tcsb_b.try_deliver(event_a.clone());
//         assert_eq!(tcsb_a.eval(), false);
//         assert_eq!(tcsb_b.eval(), false);
//     }
// }
