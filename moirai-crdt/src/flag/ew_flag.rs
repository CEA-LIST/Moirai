use std::{convert::Infallible, fmt::Debug};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::generator::op_generator::OpGenerator;
use moirai_protocol::{
    crdt::{
        eval::Eval,
        query::Read,
        redundancy::RedundancyRelation,
        replicated_data_type::{
            CausalCompaction, CausalReset, CausalStabilize, Precondition, ReplicatedDataType,
            Sequential,
        },
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
pub struct EWFlag;

impl ReplicatedDataType for EWFlag {
    type Update = FlagUpdate;
    type Stable = FlagState;
    type Rejection = Infallible;
}

impl<U: IsUnstableCore<FlagUpdate>> CausalCompaction<U> for EWFlag {
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
        matches!(new_tagged_op.op(), FlagUpdate::Disable | FlagUpdate::Clear)
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

impl<U> CausalReset<U> for EWFlag {}

impl<U> CausalStabilize<U> for EWFlag {}

impl<U> Eval<FlagState, U, Read<bool>> for EWFlag
where
    U: IsUnstableCore<FlagUpdate>,
{
    fn execute_query(stable: &FlagState, unstable: &U, _q: &Read<bool>) -> bool {
        let mut flag = stable.0.unwrap_or_default();
        for op in unstable.iter().map(|t| t.op()) {
            if matches!(op, FlagUpdate::Enable) {
                flag = true;
                break;
            }
        }
        flag
    }
}

#[cfg(feature = "fuzz")]
impl<U> OpGenerator<U> for EWFlag
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

impl Sequential for FlagState {
    type Update = FlagUpdate;
    fn is_default(&self) -> bool {
        self.0.is_none()
    }
    fn apply(&mut self, update: &FlagUpdate) {
        match update {
            FlagUpdate::Enable => self.0 = Some(true),
            FlagUpdate::Disable => self.0 = Some(false),
            FlagUpdate::Clear => self.0 = None,
        }
    }
}

impl<U> Precondition<U> for EWFlag {
    fn is_enabled(_: &FlagState, _: &U, _: &FlagUpdate) -> Result<(), Infallible> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{crdt::query::Read, replica::IsReplica};

    use crate::{
        flag::{ew_flag::EWFlag, update::FlagUpdate},
        utils::bootstrap::twins,
    };

    // Test the Enable-Wins Flag CRDT using two replicas (twins)
    #[test]
    fn enable_wins_flag() {
        let (mut replica_a, mut replica_b) = twins::<EWFlag>();

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
        // Concurrent Enable and Disable: Disable wins
        let event_a = replica_a.send(FlagUpdate::Enable).unwrap();
        let event_b = replica_b.send(FlagUpdate::Disable).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);
        assert!(replica_a.query(&Read::new()));
        assert!(replica_b.query(&Read::new()));
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_ew_flag() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run = RunConfig::new(ChurnConfig::new(0.95, 0.05), 4, 10_000, None, None)
            .with_execution_graph();
        let runs = vec![run; 1];

        let config = FuzzerConfig::<VecLog<EWFlag>, Read<bool>>::new(
            "ew_flag",
            runs,
            Predicate::new(Read::new(), |a, b| a == b),
        );

        fuzzer::<VecLog<EWFlag>, Read<bool>>(config);
    }
}
