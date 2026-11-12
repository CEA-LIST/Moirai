use std::fmt::Debug;

use moirai_protocol::{
    commitment::commit_log::CommitmentLog, crdt::replicated_data_type::{
        CausalCompaction, CausalReset, CausalStabilize, Precondition, ReplicatedDataType,
    }, state::{
        cache::{CachedLog, IncrementalCache}, log::IsLog, po_log::POLog, po_log_lamport::POLogLamport, unstable_state::{IsUnstableCore, IsUnstablePrune},
    },
};
use rand::Rng;

use crate::generator::op_generator::OpGenerator;

/// Generates client commands from the current state of a log.
pub trait CommandGenerator: IsLog {
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command;
}

//* Default implementations of CommandGenerator for some common log types.

impl<R, U> CommandGenerator for POLog<R, U>
where
    R: OpGenerator<U>
        + ReplicatedDataType
        + Precondition<U>
        + CausalCompaction<U>
        + CausalStabilize<U>
        + CausalReset<U>,
    U: IsUnstableCore<R::Update> + IsUnstablePrune<R::Update> + Default + Debug,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        R::generate(
            rng,
            &<R as OpGenerator<U>>::Config::default(),
            self.stable(),
            self.unstable(),
        )
    }
}

impl<R, U> CommandGenerator for POLogLamport<R, U>
where
    R: OpGenerator<U>
        + ReplicatedDataType
        + Precondition<U>
        + CausalCompaction<U>
        + CausalStabilize<U>
        + CausalReset<U>,
    U: IsUnstableCore<R::Update> + IsUnstablePrune<R::Update> + Default + Debug,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        R::generate(
            rng,
            &<R as OpGenerator<U>>::Config::default(),
            self.stable(),
            self.unstable(),
        )
    }
}

impl<L, V> CommandGenerator for CachedLog<L, V>
where
    L: IsLog + CommandGenerator,
    V: IncrementalCache<L::Update> + Debug,
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
