use std::fmt::Debug;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::protocol::{
    crdt::pure_crdt::{PureCRDT, RedundancyRelation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::{stable_state::IsStableState, unstable_state::IsUnstableState},
};

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
pub enum DWFlag {
    Enable,
    Disable,
    Clear,
}

impl IsStableState<DWFlag> for Option<bool> {
    fn len(&self) -> usize {
        if self.is_some() {
            1
        } else {
            0
        }
    }

    fn is_empty(&self) -> bool {
        <Option<bool> as IsStableState<DWFlag>>::len(self) == 0
    }

    fn apply(&mut self, value: DWFlag) {
        match value {
            DWFlag::Enable => *self = Some(true),
            DWFlag::Disable => *self = Some(false),
            DWFlag::Clear => *self = None,
        }
    }

    fn clear(&mut self) {
        *self = None;
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<DWFlag>,
        _tagged_op: &TaggedOp<DWFlag>,
    ) {
        <Option<bool> as IsStableState<DWFlag>>::clear(self);
    }
}

impl PureCRDT for DWFlag {
    type Value = bool;
    type StableState = Option<bool>;

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

    fn eval(stable: &Self::StableState, unstable: &impl IsUnstableState<Self>) -> Self::Value {
        let mut flag = false;

        if let Some(v) = stable {
            if !v {
                return false;
            } else {
                flag = true;
            }
        }

        // In DWFlag, any concurrent Disable wins over Enable
        for op in unstable.iter().map(|t| t.op()) {
            match op {
                DWFlag::Disable => {
                    flag = false;
                    break;
                }
                DWFlag::Enable => flag = true,
                DWFlag::Clear => unreachable!(),
            }
        }
        flag
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{flag::dw_flag::DWFlag, test_util::twins},
        protocol::replica::IsReplica,
    };

    // Test the Disable-Wins Flag CRDT using two replicas (twins)
    #[test]
    fn disable_wins_flag() {
        let (mut replica_a, mut replica_b) = twins::<DWFlag>();

        // Replica A enables the flag
        let event = replica_a.send(DWFlag::Enable).unwrap();
        replica_b.receive(event);
        assert_eq!(replica_a.query(), true);
        assert_eq!(replica_a.query(), replica_b.query());

        // Replica B disables the flag
        let event = replica_b.send(DWFlag::Disable).unwrap();
        replica_a.receive(event);
        assert_eq!(replica_b.query(), false);
        assert_eq!(replica_a.query(), replica_b.query());

        // Replica A enables again
        let event = replica_a.send(DWFlag::Enable).unwrap();
        replica_b.receive(event);
        assert_eq!(replica_a.query(), true);
        assert_eq!(replica_a.query(), replica_b.query());
    }

    #[test]
    fn disable_wins_concurrent() {
        let (mut replica_a, mut replica_b) = twins::<DWFlag>();

        // Concurrent Enable and Disable: Disable wins
        let event_a = replica_a.send(DWFlag::Enable).unwrap();
        assert_eq!(replica_a.query(), true);

        let event_b = replica_b.send(DWFlag::Disable).unwrap();
        assert_eq!(replica_b.query(), false);

        replica_a.receive(event_b.clone());
        replica_b.receive(event_a.clone());

        assert_eq!(replica_a.query(), false);
        assert_eq!(replica_b.query(), false);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_ew_flag() {
        use crate::{
            // crdt::test_util::init_tracing,
            fuzz::{
                config::{FuzzerConfig, OpConfig, RunConfig},
                fuzzer,
            },
            protocol::state::po_log::VecLog,
        };

        // init_tracing();

        let ops = OpConfig::Uniform(&[DWFlag::Enable, DWFlag::Disable, DWFlag::Clear]);

        let run = RunConfig::new(0.4, 8, 100_000, None, None);
        let runs = vec![run.clone(); 1];

        let config =
            FuzzerConfig::<VecLog<DWFlag>>::new("dw_flag", runs, ops, true, |a, b| a == b, None);

        fuzzer::<VecLog<DWFlag>>(config);
    }
}
