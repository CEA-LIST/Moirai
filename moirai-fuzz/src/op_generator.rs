use std::fmt::Debug;

use deepsize::DeepSizeOf;
use moirai_protocol::{
    crdt::pure_crdt::PureCRDT,
    state::{
        cache::CachedLog,
        graph_log::GraphLog,
        log::{BoxedLog, IsLog, IsLogTest},
        po_log::POLog,
        unstable_state::{CausalReplay, IsUnstableState},
    },
};
use rand::Rng;

pub trait OpGenerator: PureCRDT {
    type Config: Default;

    fn generate(
        rng: &mut impl Rng,
        config: &Self::Config,
        stable: &Self::StableState,
        unstable: &impl CausalReplay<Self>,
    ) -> Self;
}

pub trait OpGeneratorNested: IsLog {
    fn generate(&self, rng: &mut impl Rng) -> Self::Op;
}

impl<O> OpGeneratorNested for GraphLog<O>
where
    O: PureCRDT + Clone + OpGenerator + DeepSizeOf,
{
    fn generate(&self, rng: &mut impl Rng) -> <GraphLog<O> as IsLog>::Op {
        O::generate(rng, &O::Config::default(), self.stable(), self.unstable())
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

impl<L> OpGeneratorNested for CachedLog<L>
where
    L: OpGeneratorNested,
{
    fn generate(&self, rng: &mut impl Rng) -> Self::Op {
        self.inner().generate(rng)
    }
}

impl<L> OpGeneratorNested for BoxedLog<L>
where
    L: OpGeneratorNested,
{
    fn generate(&self, rng: &mut impl Rng) -> Self::Op {
        Box::new(self.inner().generate(rng))
    }
}
