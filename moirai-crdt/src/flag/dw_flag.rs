use std::fmt::Debug;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::op_generator::OpGenerator;
use moirai_protocol::{
    crdt::{eval::Eval, pure_crdt::PureCRDT, query::Read, redundancy::RedundancyRelation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::{stable_state::IsStableState, unstable_state::IsUnstableState},
    utils::intern_str::{InternalizeOp, Interner},
};
#[cfg(feature = "fuzz")]
use rand::Rng;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize, Tsify))]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
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
            // A batch of causally stable operations is folded in one at a time, in the
            // order each replica happened to deliver them, so the stable value has to be
            // a function of the *set* that stabilized. Disable wins over everything it is
            // concurrent with, and a causally later Enable clears the stable state through
            // `prune_redundant_ops` before it gets here, so a stable Disable is never
            // legitimately overwritten by an Enable.
            DWFlag::Enable => {
                if *self != Some(false) {
                    *self = Some(true)
                }
            }
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
        rng: &mut impl Rng,
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

impl InternalizeOp for DWFlag {
    fn internalize(self, _interner: &Interner) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{
        crdt::query::{Get, Read},
        replica::IsReplica,
        state::po_log::VecLog,
    };

    use crate::{
        flag::dw_flag::DWFlag,
        map::uw_map::{UWMap, UWMapLog},
        utils::membership::{twins, twins_log},
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

    /// Two replicas that stabilize the same concurrent `Enable` and `Disable`
    /// in opposite orders read the same flag.
    ///
    /// A replica learns that its peer holds an operation only from a later
    /// message, so on a bare flag log the message that carries the
    /// acknowledgement is itself a flag operation and retires what it
    /// acknowledges. Under a map the second round lands on another key, and the
    /// two flag operations become causally stable on both replicas with nothing
    /// having retired them — each replica folding them in the order it
    /// delivered them, which is `Enable` then `Disable` on `a` and the reverse
    /// on `b`. Folding by overwriting made the stable value whichever operation
    /// stabilized last, so `a` read `true` and `b` read `false`. Disable wins
    /// over an operation it is concurrent with, whichever order the fold sees.
    #[test]
    fn two_replicas_stabilizing_in_opposite_orders_agree() {
        let (mut replica_a, mut replica_b) = twins_log::<UWMapLog<&str, VecLog<DWFlag>>>();

        let enable = replica_a
            .send(UWMap::Update("flag", DWFlag::Enable))
            .unwrap();
        let disable = replica_b
            .send(UWMap::Update("flag", DWFlag::Disable))
            .unwrap();
        replica_a.receive(disable);
        replica_b.receive(enable);

        // The second round is what makes the first one causally stable on both.
        let spacer_a = replica_a
            .send(UWMap::Update("spacer", DWFlag::Enable))
            .unwrap();
        let spacer_b = replica_b
            .send(UWMap::Update("spacer", DWFlag::Enable))
            .unwrap();
        replica_a.receive(spacer_b);
        replica_b.receive(spacer_a);

        let read_a = replica_a.query(Get::new(&"flag", Read::<bool>::new()));
        let read_b = replica_b.query(Get::new(&"flag", Read::<bool>::new()));
        assert_eq!(read_a, Some(false), "the concurrent disable wins on `a`");
        assert_eq!(read_b, Some(false), "the concurrent disable wins on `b`");
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_dw_flag() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run = RunConfig::new(0.4, 8, 1_000, None, None, false, false);
        let runs = vec![run.clone(); 1];

        let config =
            FuzzerConfig::<VecLog<DWFlag>>::new("dw_flag", runs, true, |a, b| a == b, false, None);

        fuzzer::<VecLog<DWFlag>>(config);
    }
}
