use std::fmt::Debug;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::protocol::{
    crdt::pure_crdt::{PureCRDT, RedundancyRelation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::stable_state::IsStableState,
};

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
pub enum DWFlag {
    Enable,
    Disable,
    Clear,
}

impl IsStableState<DWFlag> for bool {
    fn len(&self) -> usize {
        // TODO: change to 'is_empty'
        1
    }

    fn is_empty(&self) -> bool {
        !*self
    }

    fn apply(&mut self, value: DWFlag) {
        match value {
            DWFlag::Enable => *self = true,
            DWFlag::Disable => *self = false,
            DWFlag::Clear => *self = false,
        }
    }

    fn clear(&mut self) {
        *self = false;
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<DWFlag>,
        _tagged_op: &TaggedOp<DWFlag>,
    ) {
        <bool as IsStableState<DWFlag>>::clear(self);
    }
}

impl PureCRDT for DWFlag {
    type Value = bool;
    type StableState = bool;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(new_tagged_op.op(), DWFlag::Clear)
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_event_id: Option<&Tag>,
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

    fn eval<'a>(
        stable: &Self::StableState,
        unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> Self::Value {
        let mut flag = *stable;
        // In DWFlag, any concurrent Disable wins over Enable
        for op in unstable.map(|t| t.op()) {
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
