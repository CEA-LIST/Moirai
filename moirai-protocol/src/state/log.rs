use std::fmt::{Debug, Display};

use crate::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
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

    /// Stabilize the log at a given version.
    fn stabilize(&mut self, version: &Version);

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

/// Blanket implementation of `IsLog` for `Box<L>` where `L: IsLog`
impl<L: IsLog> IsLog for Box<L> {
    type Command = Box<L::Command>;
    type Op = Box<L::Op>;
    type Rejection = L::Rejection;

    fn new() -> Self {
        Box::new(L::new())
    }

    fn prepare(&self, command: Self::Command) -> Self::Op {
        Box::new((**self).prepare(*command))
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        (**self).is_enabled(op)
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        let inner_op = *event.op().clone();
        let inner_event = event.unfold(inner_op);
        (**self).effect(inner_event, ctx);
    }

    fn stabilize(&mut self, version: &Version) {
        (**self).stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        (**self).redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        (**self).is_default()
    }
}

/// Log adapter that preserves indirection in read responses.
///
/// `Box<L>` is a transparent log wrapper. Use `BoxedLog<L>` when recursive
/// generated log types need `Read<V>` to produce `Box<V>`.
#[derive(Debug, Clone)]
pub struct BoxedLog<L: IsLog>(Box<L>);

impl<L: IsLog> BoxedLog<L> {
    pub fn inner(&self) -> &L {
        &self.0
    }

    pub fn inner_mut(&mut self) -> &mut L {
        &mut self.0
    }

    pub fn into_inner(self) -> Box<L> {
        self.0
    }
}

impl<L: IsLog> Default for BoxedLog<L> {
    fn default() -> Self {
        Self(Box::default())
    }
}

impl<L: IsLog> IsLog for BoxedLog<L> {
    type Command = Box<L::Command>;
    type Op = Box<L::Op>;
    type Rejection = Box<L::Rejection>;

    fn new() -> Self {
        Self(Box::new(L::new()))
    }

    fn prepare(&self, command: Self::Command) -> Self::Op {
        Box::new(self.0.as_ref().prepare(*command))
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        self.0.as_ref().is_enabled(op.as_ref()).map_err(Box::new)
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        let inner_op = *event.op().clone();
        let inner_event = event.unfold(inner_op);
        self.0.as_mut().effect(inner_event, ctx);
    }

    fn stabilize(&mut self, version: &Version) {
        self.0.as_mut().stabilize(version);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.0.as_mut().redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.0.as_ref().is_default()
    }
}

impl<L, V> EvalNested<Read<Box<V>>> for BoxedLog<L>
where
    L: IsLog + EvalNested<Read<V>>,
{
    fn execute_query(&self, _q: &Read<Box<V>>) -> Box<V> {
        Box::new(self.0.as_ref().execute_query(&Read::new()))
    }
}
