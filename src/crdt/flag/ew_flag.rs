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
pub enum EWFlag {
    Enable,
    Disable,
    Clear,
}

impl IsStableState<EWFlag> for Option<bool> {
    fn is_default(&self) -> bool {
        self.is_none()
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
}

impl Eval<Read<<Self as PureCRDT>::Value>> for EWFlag {
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> bool {
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

#[cfg(feature = "fuzz")]
impl OpGenerator for EWFlag {
    type Config = ();

    fn generate(
        rng: &mut impl RngCore,
        _config: &Self::Config,
        _stable: &<Self as PureCRDT>::StableState,
        _unstable: &impl IsUnstableState<Self>,
    ) -> Self {
        let choice = rand::seq::IteratorRandom::choose(
            [EWFlag::Enable, EWFlag::Disable, EWFlag::Clear].iter(),
            rng,
        )
        .unwrap();
        choice.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{flag::ew_flag::EWFlag, test_util::twins},
        protocol::{crdt::query::Read, replica::IsReplica},
    };

    // Test the Enable-Wins Flag CRDT using two replicas (twins)
    #[test]
    fn enable_wins_flag() {
        let (mut replica_a, mut replica_b) = twins::<EWFlag>();

        // Replica A enables the flag
        let event = replica_a.send(EWFlag::Enable).unwrap();
        replica_b.receive(event);
        assert_eq!(replica_a.query(Read::new()), true);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));

        // Replica B disables the flag
        let event = replica_b.send(EWFlag::Disable).unwrap();
        replica_a.receive(event);
        assert_eq!(replica_b.query(Read::new()), false);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));

        // Replica A enables again
        let event = replica_a.send(EWFlag::Enable).unwrap();
        replica_b.receive(event);
        assert_eq!(replica_a.query(Read::new()), true);
        assert_eq!(replica_a.query(Read::new()), replica_b.query(Read::new()));
        // Concurrent Enable and Disable: Disable wins
        let event_a = replica_a.send(EWFlag::Enable).unwrap();
        let event_b = replica_b.send(EWFlag::Disable).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);
        assert_eq!(replica_a.query(Read::new()), true);
        assert_eq!(replica_b.query(Read::new()), true);
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
            FuzzerConfig::<VecLog<EWFlag>>::new("ew_flag", runs, true, |a, b| a == b, true);

        fuzzer::<VecLog<EWFlag>>(config);
    }
}
