use std::fmt::{Debug, Display};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::Event,
    state::effect_context::EffectContext,
};
#[cfg(feature = "test_utils")]
use crate::{
    crdt::pure_crdt::{PureCRDT, UsesUnstableService},
    state::{
        po_log::POLog,
        stable_state::IsStableState,
        unstable_state::{CausalReplay, IsUnstableState},
    },
};

pub trait IsLog: Default + Debug {
    // TODO: is Value really needed?
    type Value: Default + Debug;
    /// Stored operation type
    type Op: Debug + Clone;
    /// Rejection type for operations that are not enabled in the current state
    type Rejection: Debug + Display;

    fn new() -> Self {
        Self::default()
    }
    /// `prepare` cannot inspect the state, being limited to returning the operation (including potential parameters).
    fn prepare(op: Self::Op) -> Self::Op {
        op
    }
    /// Check if an update operation is enabled in the current state.
    fn is_enabled(&self, _op: &Self::Op) -> Result<(), Self::Rejection> {
        Ok(())
    }
    /// Apply an event to the log, updating the state.
    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>);
    /// Evaluate a query operation on the log, returning a value.
    fn eval<Q>(&self, q: Q) -> Q::Response
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
    /// For example, a log may be in its default state even if the underlying CRDT is not in its default state,
    /// if the log has been pruned to remove all events.
    fn is_default(&self) -> bool;
}

// TODO: this is potentially garbage
#[doc(hidden)]
pub trait __DefaultSinkExpansion: IsLog {
    fn default_sink_expansion(&self, _ctx: &mut EffectContext<'_>) {}
}

impl<L: IsLog> __DefaultSinkExpansion for L {}

#[cfg(feature = "test_utils")]
pub trait IsLogTest: IsLog
where
    Self::Op: PureCRDT + DeepSizeOf,
    <Self::Op as PureCRDT>::StableState: IsStableState<Self::Op>,
{
    fn stable(&self) -> &<Self::Op as PureCRDT>::StableState;
    fn unstable(&self) -> &(impl CausalReplay<Self::Op> + DeepSizeOf);
    fn unstable_mut(&mut self) -> &mut (impl CausalReplay<Self::Op> + DeepSizeOf);
}

#[cfg(feature = "test_utils")]
impl<O, U> IsLogTest for POLog<O, U>
where
    O: PureCRDT + Clone + DeepSizeOf + UsesUnstableService<U>,
    U: Default + Debug + DeepSizeOf + IsUnstableState<O>,
{
    fn stable(&self) -> &<Self::Op as PureCRDT>::StableState {
        &self.stable
    }

    fn unstable(&self) -> &(impl CausalReplay<Self::Op> + DeepSizeOf) {
        &self.unstable
    }

    fn unstable_mut(&mut self) -> &mut (impl CausalReplay<Self::Op> + DeepSizeOf) {
        &mut self.unstable
    }
}

/// Blanket implementation of `IsLog` for `Box<L>` where `L: IsLog`
impl<L: IsLog> IsLog for Box<L> {
    type Value = L::Value;
    type Op = Box<L::Op>;
    type Rejection = L::Rejection;

    fn new() -> Self {
        Box::new(L::new())
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

/// Log adapter that preserves indirection in the associated value type.
///
/// `Box<L>` is a transparent log wrapper: its `Value` is still `L::Value`.
/// Use `BoxedLog<L>` when recursive generated log types need the corresponding
/// read value to remain boxed as `Box<L::Value>`.
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
    type Value = Box<L::Value>;
    type Op = Box<L::Op>;
    type Rejection = Box<L::Rejection>;

    fn new() -> Self {
        Self(Box::new(L::new()))
    }

    fn prepare(op: Self::Op) -> Self::Op {
        Box::new(L::prepare(*op))
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

impl<L> EvalNested<Read<<Self as IsLog>::Value>> for BoxedLog<L>
where
    L: IsLog + EvalNested<Read<<L as IsLog>::Value>>,
{
    fn execute_query(&self, _q: Read<<Self as IsLog>::Value>) -> Box<L::Value> {
        Box::new(self.0.as_ref().execute_query(Read::new()))
    }
}
