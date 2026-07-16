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
    crdt::pure_crdt::PureCRDT,
    state::{
        po_log::POLog,
        stable_state::IsStableState,
        unstable_state::{CausalReplay, IsUnstableState},
    },
};

pub trait IsLog: Default + Debug {
    // TODO: is Value really needed?
    type Value: Default + Debug;
    /// Stored operation type.
    type Op: Debug + Clone;
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
    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>);
    fn eval<Q>(&self, q: Q) -> Q::Response
    where
        Q: QueryOperation,
        Self: EvalNested<Q>,
    {
        Self::execute_query(self, q)
    }
    fn stabilize(&mut self, version: &Version);
    fn redundant_by_parent(&mut self, version: &Version, conservative: bool);
    fn is_default(&self) -> bool;
}

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
    O: PureCRDT + Clone + DeepSizeOf,
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

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    #[derive(Debug, Default)]
    struct ScalarLog;

    impl IsLog for ScalarLog {
        type Value = u8;
        type Op = u8;
        type Rejection = Infallible;

        fn effect(&mut self, _event: Event<Self::Op>, _ctx: &mut EffectContext<'_>) {}

        fn stabilize(&mut self, _version: &Version) {}

        fn redundant_by_parent(&mut self, _version: &Version, _conservative: bool) {}

        fn is_default(&self) -> bool {
            true
        }
    }

    impl EvalNested<Read<u8>> for ScalarLog {
        fn execute_query(&self, _q: Read<u8>) -> u8 {
            7
        }
    }

    #[test]
    fn boxed_log_boxes_read_value() {
        let log = BoxedLog::<ScalarLog>::default();
        let value: Box<u8> = log.execute_query(Read::new());

        assert_eq!(*value, 7);
    }
}
