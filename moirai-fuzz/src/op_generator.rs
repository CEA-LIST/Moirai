use std::fmt::Debug;

use moirai_protocol::{
    commitment::commit_log::CommitmentLog,
    crdt::replicated_data_type::{ReplicatedDataType, UsesUnstableService},
    state::{
        cache::CachedLog,
        graph_log::RawGraphLog,
        log::{BoxedLog, IsLog},
        po_log::POLog,
        unstable_state::{CausalReplay, IsUnstableCore, IsUnstablePrune, event_graph::EventGraph},
    },
};
use rand::Rng;

pub trait OpGenerator: ReplicatedDataType {
    type Config: Default;

    fn generate(
        rng: &mut impl Rng,
        config: &Self::Config,
        stable: &Self::StableState,
        unstable: &impl IsUnstableCore<Self>,
    ) -> Self;
}

// TODO: Find a way to get rid of this trait and just use OpGenerator instead

pub trait CausalOpGenerator: ReplicatedDataType {
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

/// Generates client commands from the current state of a log.
///
/// Commands are deliberately kept distinct from replicated operations: log adapters such as
/// `CommitmentLog` use `prepare` to attach protocol metadata to a client command.
pub trait CommandGenerator: IsLog {
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command;
}

impl<O> CommandGenerator for RawGraphLog<O>
where
    O: ReplicatedDataType + Clone + CausalOpGenerator + UsesUnstableService<EventGraph<O>>,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        O::generate_causal(
            rng,
            &<O as CausalOpGenerator>::Config::default(),
            self.stable(),
            self.unstable(),
        )
    }
}

impl<O, U> CommandGenerator for POLog<O, U>
where
    O: ReplicatedDataType + Clone + OpGenerator + UsesUnstableService<U>,
    U: IsUnstablePrune<O> + Default + Debug,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        O::generate(
            rng,
            &<O as OpGenerator>::Config::default(),
            self.stable(),
            self.unstable(),
        )
    }
}

impl<L> CommandGenerator for CachedLog<L>
where
    L: CommandGenerator,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        self.inner().generate_command(rng)
    }
}

impl<L> CommandGenerator for BoxedLog<L>
where
    L: CommandGenerator,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        Box::new(self.inner().generate_command(rng))
    }
}

impl<L> CommandGenerator for CommitmentLog<L>
where
    L: IsLog + CommandGenerator,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        self.child().generate_command(rng)
    }
}
