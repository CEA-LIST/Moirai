use std::fmt::Debug;

use moirai_protocol::{
    commitment::commit_log::CommitmentLog,
    crdt::replicated_data_type::{ReplicatedDataType, UsesUnstableService},
    state::{
        cache::{CachedLog, IncrementalCache},
        graph_log::GraphLog,
        log::IsLog,
        po_log::POLog,
        unstable_state::{IsUnstablePrune, event_graph::EventGraph},
    },
};
use rand::Rng;

use crate::generator::op_generator::OpGenerator;

/// Generates client commands from the current state of a log.
pub trait CommandGenerator: IsLog {
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command;
}

//* Default implementations of CommandGenerator for some common log types.

impl<O> CommandGenerator for GraphLog<O>
where
    O: ReplicatedDataType + Clone + OpGenerator<EventGraph<O>> + UsesUnstableService<EventGraph<O>>,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        O::generate(
            rng,
            &<O as OpGenerator<EventGraph<O>>>::Config::default(),
            self.stable(),
            self.unstable(),
        )
    }
}

impl<O, U> CommandGenerator for POLog<O, U>
where
    O: ReplicatedDataType + Clone + OpGenerator<U> + UsesUnstableService<U>,
    U: IsUnstablePrune<O> + Default + Debug,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        O::generate(
            rng,
            &<O as OpGenerator<U>>::Config::default(),
            self.stable(),
            self.unstable(),
        )
    }
}

impl<L, V> CommandGenerator for CachedLog<L, V>
where
    L: IsLog + CommandGenerator,
    V: IncrementalCache<L::Op> + Debug,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        self.inner().generate_command(rng)
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

impl<L: CommandGenerator> CommandGenerator for Box<L> {
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        (**self).generate_command(rng)
    }
}
