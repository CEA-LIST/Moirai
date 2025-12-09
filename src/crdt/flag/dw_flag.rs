use std::fmt::Debug;

#[cfg(feature = "fuzz")]
use rand::RngCore;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

#[cfg(feature = "fuzz")]
use crate::fuzz::config::OpGenerator;
use crate::protocol::{
    crdt::{eval::Eval, pure_crdt::PureCRDT, query::Read, redundancy::RedundancyRelation},
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
    fn is_default(&self) -> bool {
        self.is_none()
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
}

impl Eval<Read<<Self as PureCRDT>::Value>> for DWFlag {
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> bool {
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

#[cfg(feature = "fuzz")]
impl OpGenerator for DWFlag {
    type Config = ();

    fn generate(
        rng: &mut impl RngCore,
        _config: &Self::Config,
        _stable: &<Self as PureCRDT>::StableState,
        _unstable: &impl IsUnstableState<Self>,
    ) -> Self {
        let choice = rand::seq::IteratorRandom::choose(
            [DWFlag::Enable, DWFlag::Disable, DWFlag::Clear].iter(),
            rng,
        )
        .unwrap();
        choice.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{flag::dw_flag::DWFlag, test_util::twins},
        protocol::{crdt::query::Read, replica::IsReplica},
    };

    // Test the Disable-Wins Flag CRDT using two replicas (twins)
    #[test]
    fn disable_wins_flag() {
        let (mut replica_a, mut replica_b) = twins::<DWFlag>();

        // Replica A enables the flag
        let event = replica_a.send(DWFlag::Enable).unwrap();
        replica_b.receive(event);
        assert_eq!(replica_a.query(Read::new()), true);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));

        // Replica B disables the flag
        let event = replica_b.send(DWFlag::Disable).unwrap();
        replica_a.receive(event);
        assert_eq!(replica_b.query(Read::new()), false);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));

        // Replica A enables again
        let event = replica_a.send(DWFlag::Enable).unwrap();
        replica_b.receive(event);
        assert_eq!(replica_a.query(Read::new()), true);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
    }

    #[test]
    fn disable_wins_concurrent() {
        let (mut replica_a, mut replica_b) = twins::<DWFlag>();

        // Concurrent Enable and Disable: Disable wins
        let event_a = replica_a.send(DWFlag::Enable).unwrap();
        assert_eq!(replica_a.query(Read::new()), true);

        let event_b = replica_b.send(DWFlag::Disable).unwrap();
        assert_eq!(replica_b.query(Read::new()), false);

        replica_a.receive(event_b.clone());
        replica_b.receive(event_a.clone());

        assert_eq!(replica_a.query(Read::new()), false);
        assert_eq!(replica_b.query(Read::new()), false);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_ew_flag() {
        use crate::{
            // crdt::test_util::init_tracing,
            fuzz::{
                config::{FuzzerConfig, RunConfig},
                fuzzer,
            },
            protocol::state::po_log::VecLog,
        };

        // init_tracing();

        let run = RunConfig::new(0.4, 8, 100_000, None, None, false);
        let runs = vec![run.clone(); 1];

        let config =
            FuzzerConfig::<VecLog<DWFlag>>::new("dw_flag", runs, true, |a, b| a == b, true);

        fuzzer::<VecLog<DWFlag>>(config);
    }
}
