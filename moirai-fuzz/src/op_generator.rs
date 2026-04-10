use std::fmt::Debug;

use deepsize::DeepSizeOf;
use moirai_protocol::{
    crdt::pure_crdt::PureCRDT,
    state::{
        event_graph::EventGraph,
        log::{IsLog, IsLogTest},
        po_log::POLog,
        unstable_state::IsUnstableState,
    },
};
use rand::Rng;

pub trait OpGenerator: PureCRDT {
    type Config: Default;

    fn generate(
        rng: &mut impl Rng,
        config: &Self::Config,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> Self;
}

pub trait OpGeneratorNested: IsLog {
    fn generate(&self, rng: &mut impl Rng) -> Self::Op;
}

impl<O> OpGeneratorNested for EventGraph<O>
where
    O: PureCRDT + Clone + OpGenerator,
    EventGraph<O>: IsLog<Op = O>,
{
    fn generate(&self, rng: &mut impl Rng) -> <EventGraph<O> as IsLog>::Op {
        O::generate(rng, &O::Config::default(), &O::StableState::default(), self)
    }
}

impl<O, U> OpGeneratorNested for POLog<O, U>
where
    O: PureCRDT + Clone + OpGenerator + DeepSizeOf,
    U: IsUnstableState<O> + Default + Debug + DeepSizeOf,
{
    fn generate(&self, rng: &mut impl Rng) -> Self::Op {
        O::generate(
            rng,
            &<O as OpGenerator>::Config::default(),
            self.stable(),
            self.unstable(),
        )
    }
}
