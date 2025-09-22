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
pub enum EWFlag {
    Enable,
    Disable,
    Clear,
}

impl IsStableState<EWFlag> for Option<bool> {
    fn len(&self) -> usize {
        if self.is_some() {
            1
        } else {
            0
        }
    }

    fn is_empty(&self) -> bool {
        <Option<bool> as IsStableState<EWFlag>>::len(self) == 0
    }

    fn apply(&mut self, value: EWFlag) {
        match value {
            EWFlag::Enable => *self = Some(true),
            EWFlag::Disable => *self = Some(false),
            EWFlag::Clear => *self = None,
        }
    }

    fn clear(&mut self) {
        *self = None;
    }

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<EWFlag>,
        _tagged_op: &TaggedOp<EWFlag>,
    ) {
        <Option<bool> as IsStableState<EWFlag>>::clear(self);
    }
}

impl PureCRDT for EWFlag {
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
        matches!(new_tagged_op.op(), EWFlag::Disable | EWFlag::Clear)
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
        let mut flag = match stable {
            Some(v) => *v,
            None => false,
        };
        for op in unstable.iter().map(|t| t.op()) {
            if let EWFlag::Enable = op {
                flag = true;
                break;
            }
        }
        flag
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{flag::ew_flag::EWFlag, test_util::twins},
        protocol::replica::IsReplica,
    };

    // Test the Enable-Wins Flag CRDT using two replicas (twins)
    #[test]
    fn enable_wins_flag() {
        let (mut replica_a, mut replica_b) = twins::<EWFlag>();

        // Replica A enables the flag
        let event = replica_a.send(EWFlag::Enable).unwrap();
        replica_b.receive(event);
        assert_eq!(replica_a.query(), true);
        assert_eq!(replica_a.query(), replica_b.query());

        // Replica B disables the flag
        let event = replica_b.send(EWFlag::Disable).unwrap();
        replica_a.receive(event);
        assert_eq!(replica_b.query(), false);
        assert_eq!(replica_a.query(), replica_b.query());

        // Replica A enables again
        let event = replica_a.send(EWFlag::Enable).unwrap();
        replica_b.receive(event);
        assert_eq!(replica_a.query(), true);
        assert_eq!(replica_a.query(), replica_b.query());
        // Concurrent Enable and Disable: Disable wins
        let event_a = replica_a.send(EWFlag::Enable).unwrap();
        let event_b = replica_b.send(EWFlag::Disable).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);
        assert_eq!(replica_a.query(), true);
        assert_eq!(replica_b.query(), true);
    }

    // #[cfg(feature = "op_weaver")]
    // #[test]
    // fn op_weaver_ew_flag() {
    //     use crate::utils::op_weaver::{op_weaver, EventGraphConfig};

    //     let ops = vec![EWFlag::Enable, EWFlag::Disable, EWFlag::Clear];

    //     let config = EventGraphConfig {
    //         name: "ewflag",
    //         num_replicas: 8,
    //         num_operations: 10_000,
    //         operations: &ops,
    //         final_sync: true,
    //         churn_rate: 0.3,
    //         reachability: None,
    //         compare: |a: &bool, b: &bool| a == b,
    //         record_results: true,
    //         seed: None,
    //         witness_graph: false,
    //         concurrency_score: false,
    //     };

    //     op_weaver::<EventGraph<EWFlag>>(config);
    // }

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

        let ops = OpConfig::Uniform(&[EWFlag::Enable, EWFlag::Disable, EWFlag::Clear]);

        let run = RunConfig::new(0.4, 8, 100_000, None, None);
        let runs = vec![run.clone(); 1];

        let config =
            FuzzerConfig::<VecLog<EWFlag>>::new("ew_flag", runs, ops, true, |a, b| a == b, None);

        fuzzer::<VecLog<EWFlag>>(config);
    }
}
