use std::fmt::Debug;

use moirai_protocol::{
    crdt::pure_crdt::{PureCRDT, UsesUnstableService},
    state::{
        cache::CachedLog,
        graph_log::RawGraphLog,
        log::{BoxedLog, IsLog},
        po_log::POLog,
        unstable_state::{CausalReplay, IsUnstableCore, IsUnstablePrune, event_graph::EventGraph},
    },
};
use rand::Rng;

pub trait OpGenerator: PureCRDT {
    type Config: Default;

    fn generate(
        rng: &mut impl Rng,
        config: &Self::Config,
        stable: &Self::StableState,
        unstable: &impl IsUnstableCore<Self>,
    ) -> Self;
}

// TODO: Find a way to get rid of this trait and just use OpGenerator instead

pub trait CausalOpGenerator: PureCRDT {
    type Config: Default;

    fn generate_causal(
        rng: &mut impl Rng,
        config: &Self::Config,
        stable: &Self::StableState,
        unstable: &impl CausalReplay<Self>,
    ) -> Self;
}

impl<O> CausalOpGenerator for O
where
    O: OpGenerator,
{
    type Config = <O as OpGenerator>::Config;

    fn generate_causal(
        rng: &mut impl Rng,
        config: &Self::Config,
        stable: &Self::StableState,
        unstable: &impl CausalReplay<Self>,
    ) -> Self {
        O::generate(rng, config, stable, unstable)
    }
}

pub trait OpGeneratorNested: IsLog {
    fn generate(&self, rng: &mut impl Rng) -> Self::Op;
}

impl<O> OpGeneratorNested for RawGraphLog<O>
where
    O: PureCRDT + Clone + CausalOpGenerator + UsesUnstableService<EventGraph<O>>,
{
    fn generate(&self, rng: &mut impl Rng) -> <RawGraphLog<O> as IsLog>::Op {
        O::generate_causal(
            rng,
            &<O as CausalOpGenerator>::Config::default(),
            self.stable(),
            self.unstable(),
        )
    }
}

impl<O, U> OpGeneratorNested for POLog<O, U>
where
    O: PureCRDT + Clone + OpGenerator + UsesUnstableService<U>,
    U: IsUnstablePrune<O> + Default + Debug,
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
