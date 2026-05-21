use std::fmt::Debug;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::{
    clock::version_vector::Version,
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    state::effect_context::EffectContext,
};
#[cfg(feature = "test_utils")]
use crate::{
    crdt::pure_crdt::PureCRDT,
    state::{po_log::POLog, stable_state::IsStableState, unstable_state::IsUnstableLog},
};

pub trait IsLog: Default + Debug {
    // TODO: is Value really needed?
    type Value: Default + Debug;
    type Op: Debug + Clone;

    fn new() -> Self {
        Self::default()
    }
    /// `prepare` cannot inspect the state, being limited to returning the operation (including potential parameters).
    fn prepare(op: Self::Op) -> Self::Op {
        op
    }
    // TODO: replace by Result
    fn is_enabled(&self, op: &Self::Op) -> bool;
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
    fn default_sink_expansion(&self, _ctx: &mut EffectContext<'_>) {}
}

#[cfg(feature = "test_utils")]
pub trait IsLogTest
where
    Self: IsLog,
    Self::Op: PureCRDT + DeepSizeOf,
    <Self::Op as PureCRDT>::StableState: IsStableState<Self::Op>,
{
    fn stable(&self) -> &<Self::Op as PureCRDT>::StableState;
    fn unstable(&self) -> &(impl IsUnstableLog<Self::Op> + DeepSizeOf);
    fn unstable_mut(&mut self) -> &mut (impl IsUnstableLog<Self::Op> + DeepSizeOf);
}

#[cfg(feature = "test_utils")]
impl<O, U> IsLogTest for POLog<O, U>
where
    O: PureCRDT + Clone + DeepSizeOf,
    U: IsUnstableLog<O> + Default + Debug + DeepSizeOf,
{
    fn stable(&self) -> &<O as PureCRDT>::StableState {
        &self.stable
    }

    fn unstable(&self) -> &(impl IsUnstableLog<Self::Op> + DeepSizeOf) {
        &self.unstable
    }

    fn unstable_mut(&mut self) -> &mut (impl IsUnstableLog<Self::Op> + DeepSizeOf) {
        &mut self.unstable
    }
}

/// Blanket implementation of `IsLog` for `Box<L>` where `L: IsLog`
impl<L: IsLog> IsLog for Box<L> {
    type Value = L::Value;
    type Op = Box<L::Op>;

    fn new() -> Self {
        Box::new(L::new())
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
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
