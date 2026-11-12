use std::{convert::Infallible, fmt::Debug};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::generator::op_generator::OpGenerator;
use moirai_protocol::{
    data_type::{
        query::Read,
        replicated::{
            CausalCompaction, CausalReset, CausalStabilize, Eval, Precondition, RedundancyRelation,
            ReplicatedDataType,
        },
        sequential::SequentialDataType,
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableCore,
};
#[cfg(feature = "fuzz")]
use rand::Rng;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::flag::update::FlagUpdate;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DWFlag;

impl ReplicatedDataType for DWFlag {
    type Update = FlagUpdate;
    type Stable = FlagState;
    type Rejection = Infallible;
}

impl<U: IsUnstableCore<FlagUpdate>> CausalCompaction<U> for DWFlag {
    fn compact_stable(
        stable: &mut FlagState,
        _: RedundancyRelation<FlagUpdate>,
        _: &TaggedOp<FlagUpdate>,
    ) {
        stable.0 = None;
    }
    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self::Update>,
        _stable: &Self::Stable,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self::Update>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(new_tagged_op.op(), FlagUpdate::Clear)
    }

    fn redundant_by_when_redundant(
        _old_op: &Self::Update,
        _old_event_id: Option<&Tag>,
        is_conc: bool,
        _new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        !is_conc
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self::Update,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        _new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        !is_conc
    }
}

impl<U> CausalReset<U> for DWFlag {}

impl<U> CausalStabilize<U> for DWFlag {}

impl<U> Eval<U, Read<bool>> for DWFlag
where
    U: IsUnstableCore<FlagUpdate>,
{
    fn execute_query(stable: &FlagState, unstable: &U, _q: &Read<bool>) -> bool {
        let mut flag = false;

        if let Some(v) = stable.0 {
            if !v {
                return false;
            } else {
                flag = true;
            }
        }

        // In DWFlag, any concurrent Disable wins over Enable
        for op in unstable.iter().map(|t| t.op()) {
            match op {
                FlagUpdate::Disable => {
                    flag = false;
                    break;
                }
                FlagUpdate::Enable => flag = true,
                FlagUpdate::Clear => unreachable!(),
            }
        }
        flag
    }
}

#[cfg(feature = "fuzz")]
impl<U> OpGenerator<U> for DWFlag
where
    U: IsUnstableCore<FlagUpdate>,
{
    type Config = ();

    fn generate(
        rng: &mut impl Rng,
        _config: &Self::Config,
        _stable: &<Self as ReplicatedDataType>::Stable,
        _unstable: &U,
    ) -> Self::Update {
        let choice = rand::seq::IteratorRandom::choose(
            [FlagUpdate::Enable, FlagUpdate::Disable, FlagUpdate::Clear].iter(),
            rng,
        )
        .unwrap();
        choice.clone()
    }
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FlagState(Option<bool>);

impl SequentialDataType for FlagState {
    type Update = FlagUpdate;
    type Rejection = Infallible;

    fn is_default(&self) -> bool {
        self.0.is_none()
    }

    fn apply(&mut self, update: &FlagUpdate) {
        match update {
            FlagUpdate::Enable => {
                // A remaining stable disable is concurrent with this enable.
                if self.0 != Some(false) {
                    self.0 = Some(true);
                }
            }
            FlagUpdate::Disable => self.0 = Some(false),
            FlagUpdate::Clear => self.0 = None,
        }
    }

    fn is_enabled(&self, _update: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }
}

impl<U> Precondition<U> for DWFlag {
    fn is_enabled(_: &FlagState, _: &U, _: &FlagUpdate) -> Result<(), Infallible> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{
        data_type::query::{Get, Read},
        replica::IsReplica,
        state::po_log::VecLog,
    };

    use crate::{
        flag::{dw_flag::DWFlag, update::FlagUpdate},
        map::{update::MapUpdate, uw_map::UWMapLog},
        utils::bootstrap::{twins, twins_log},
    };

    // Test the Disable-Wins Flag CRDT using two replicas (twins)
    #[test]
    fn disable_wins_flag() {
        let (mut replica_a, mut replica_b) = twins::<DWFlag>();

        // Replica A enables the flag
        let event = replica_a.send(FlagUpdate::Enable).unwrap();
        replica_b.receive(event);
        assert!(replica_a.query(&Read::new()));
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));

        // Replica B disables the flag
        let event = replica_b.send(FlagUpdate::Disable).unwrap();
        replica_a.receive(event);
        assert!(!replica_b.query(&Read::new()));
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));

        // Replica A enables again
        let event = replica_a.send(FlagUpdate::Enable).unwrap();
        replica_b.receive(event);
        assert!(replica_a.query(&Read::new()));
        assert_eq!(replica_a.query(&Read::new()), replica_b.query(&Read::new()));
    }

    #[test]
    fn disable_wins_concurrent() {
        let (mut replica_a, mut replica_b) = twins::<DWFlag>();

        // Concurrent Enable and Disable: Disable wins
        let event_a = replica_a.send(FlagUpdate::Enable).unwrap();
        assert!(replica_a.query(&Read::new()));

        let event_b = replica_b.send(FlagUpdate::Disable).unwrap();
        assert!(!replica_b.query(&Read::new()));

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert!(!replica_a.query(&Read::new()));
        assert!(!replica_b.query(&Read::new()));
    }

    /// A concurrent `Enable` and `Disable` under a map key, made causally stable
    /// on both replicas by a second round of writes on another key. Each replica
    /// folds the two into its stable state in its own delivery order, and
    /// `apply` overwrites, so the stable value is whichever stabilized last.
    /// Disable wins over an operation it is concurrent with, on both replicas.
    #[test]
    fn dw_flag_converges_when_replicas_stabilize_in_different_orders() {
        let (mut replica_a, mut replica_b) = twins_log::<UWMapLog<&str, VecLog<DWFlag>>>();

        let enable = replica_a
            .send(MapUpdate::Update("flag", FlagUpdate::Enable))
            .unwrap();
        let disable = replica_b
            .send(MapUpdate::Update("flag", FlagUpdate::Disable))
            .unwrap();
        replica_a.receive(disable);
        replica_b.receive(enable);

        let spacer_a = replica_a
            .send(MapUpdate::Update("spacer", FlagUpdate::Enable))
            .unwrap();
        let spacer_b = replica_b
            .send(MapUpdate::Update("spacer", FlagUpdate::Enable))
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
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run = RunConfig::new(ChurnConfig::new(0.4, 0.6), 8, 1_000, None, None);
        let runs = vec![run; 1];

        let config = FuzzerConfig::<VecLog<DWFlag>, Read<bool>>::new(
            "dw_flag",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<VecLog<DWFlag>, Read<bool>>(config);
    }
}
