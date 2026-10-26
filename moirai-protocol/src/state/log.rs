use std::fmt::{Debug, Display};

use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    state::effect_context::EffectContext,
};

pub trait IsLog: Default {
    /// Client input command type
    type Command: Debug + Clone;
    /// Stored operation type
    type Op: Debug + Clone;
    /// Rejection type for operations that are not enabled in the current state
    type Rejection: Debug + Display;

    fn new() -> Self {
        Self::default()
    }

    /// Prepare a client command as the operation stored and replicated by this log.
    fn prepare(&self, command: Self::Command) -> Self::Op;

    /// Check if an update operation is enabled in the current state.
    fn is_enabled(&self, _op: &Self::Op) -> Result<(), Self::Rejection> {
        Ok(())
    }

    /// Apply an event to the log, updating the state.
    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>);

    /// Evaluate a query operation on the log, returning a value.
    fn eval<Q>(&self, q: &Q) -> Q::Response
    where
        Q: QueryOperation,
        Self: EvalNested<Q>,
    {
        Self::execute_query(self, q)
    }

    fn stabilize(&mut self, frontier: &CausalFrontier);

    /// Prune the log by removing events that are redundant.
    fn redundant_by_parent(&mut self, version: &Version, conservative: bool);

    /// Check if the log is in its default state (no events).
    /// # Note
    /// Default state is a structural property of the log, not a semantic property of the underlying CRDT.
    /// For example, a log may be in its default state even if the underlying CRDT has been mutated,
    /// if the log has been pruned to remove all events.
    fn is_default(&self) -> bool;
}

// TODO: this is potentially garbage
#[doc(hidden)]
pub trait DefaultSinkExpansion: IsLog {
    fn default_sink_expansion(&self, _ctx: &mut EffectContext<'_>) {}
}

impl<L: IsLog> DefaultSinkExpansion for L {}

/// Explicit indirection for inline recursive logs.
impl<L: IsLog> IsLog for Box<L> {
    type Command = L::Command;
    type Op = L::Op;
    type Rejection = Box<L::Rejection>;

    fn new() -> Self {
        Box::new(L::new())
    }

    fn prepare(&self, command: Self::Command) -> Self::Op {
        (**self).prepare(command)
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        (**self).is_enabled(op).map_err(Box::new)
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        (**self).effect(event, ctx);
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        (**self).stabilize(frontier);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        (**self).redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        (**self).is_default()
    }
}
