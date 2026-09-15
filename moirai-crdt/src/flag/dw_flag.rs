use std::{convert::Infallible, fmt::Debug};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::op_generator::OpGenerator;
use moirai_protocol::{
    crdt::{
        eval::Eval,
        query::Read,
        redundancy::RedundancyRelation,
        replicated_data_type::{ReplicatedDataType, UsesUnstableService},
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    state::{stable_state::IsStableState, unstable_state::IsUnstableCore},
};
#[cfg(feature = "fuzz")]
use rand::Rng;

#[derive(Clone, Debug, PartialEq)]
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

impl ReplicatedDataType for DWFlag {
    type StableState = Option<bool>;
    type Rejection = Infallible;

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

impl<U> UsesUnstableService<U> for DWFlag where U: IsUnstableCore<Self> {}

impl<U> Eval<Read<bool>, U> for DWFlag
where
    U: IsUnstableCore<Self>,
{
    fn execute_query(_q: &Read<bool>, stable: &Self::StableState, unstable: &U) -> bool {
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
        _stable: &<Self as ReplicatedDataType>::StableState,
        _unstable: &impl IsUnstableCore<Self>,
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
        assert_eq!(replica_a.query(&Read::new()), true);
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));

        // Replica B disables the flag
        let event = replica_b.send(DWFlag::Disable).unwrap();
        replica_a.receive(event);
        assert_eq!(replica_b.query(&Read::new()), false);
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));

        // Replica A enables again
        let event = replica_a.send(DWFlag::Enable).unwrap();
        replica_b.receive(event);
        assert_eq!(replica_a.query(&Read::new()), true);
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }

    #[test]
    fn disable_wins_concurrent() {
        let (mut replica_a, mut replica_b) = twins::<DWFlag>();

        // Concurrent Enable and Disable: Disable wins
        let event_a = replica_a.send(DWFlag::Enable).unwrap();
        assert_eq!(replica_a.query(&Read::new()), true);

        let event_b = replica_b.send(DWFlag::Disable).unwrap();
        assert_eq!(replica_b.query(&Read::new()), false);

        replica_a.receive(event_b.clone());
        replica_b.receive(event_a.clone());

        assert_eq!(replica_a.query(&Read::new()), false);
        assert_eq!(replica_b.query(&Read::new()), false);
    }

    /// A concurrent `Enable` and `Disable` under a map key, made causally stable
    /// on both replicas by a second round of writes on another key. Each replica
    /// folds the two into its stable state in its own delivery order, and
    /// `apply` overwrites, so the stable value is whichever stabilized last.
    /// Disable wins over an operation it is concurrent with, on both replicas.
    #[test]
    #[ignore = "reproduces dw_flag stable fold depending on order; fix pending"]
    fn dw_flag_converges_when_replicas_stabilize_in_different_orders() {
        let (mut replica_a, mut replica_b) = twins_log::<UWMapLog<&str, VecLog<DWFlag>>>();

        let enable = replica_a
            .send(UWMap::Update("flag", DWFlag::Enable))
            .unwrap();
        let disable = replica_b
            .send(UWMap::Update("flag", DWFlag::Disable))
            .unwrap();
        replica_a.receive(disable);
        replica_b.receive(enable);

        let spacer_a = replica_a
            .send(UWMap::Update("spacer", DWFlag::Enable))
            .unwrap();
        let spacer_b = replica_b
            .send(UWMap::Update("spacer", DWFlag::Enable))
            .unwrap();
        replica_a.receive(spacer_b);
        replica_b.receive(spacer_a);

        let read_a = replica_a.query(&Get::new(&"flag", Read::<bool>::new()));
        let read_b = replica_b.query(&Get::new(&"flag", Read::<bool>::new()));
        assert_eq!(read_a, Some(false), "the concurrent disable wins on a");
        assert_eq!(read_b, Some(false), "the concurrent disable wins on b");
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_dw_flag() {
        use moirai_fuzz::{
            config::{FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run = RunConfig::new(0.4, 8, 1_000, None, None, false, false);
        let runs = vec![run.clone(); 1];

        let config = FuzzerConfig::<VecLog<DWFlag>, Read<bool>>::new(
            "dw_flag",
            runs,
            true,
            Predicate::new(Read::new(), |a, b| a == b),
            false,
        );

        fuzzer::<VecLog<DWFlag>, Read<bool>>(config);
    }
}
