#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use std::fmt::Debug;

#[cfg(feature = "test_utils")]
use crate::state::{log::IsLogTest, unstable_state::CausalReplay};
use crate::{
    clock::version_vector::Version,
    crdt::{
        eval::{Eval, EvalNested},
        pure_crdt::{CausalReset, PureCRDT, UsesUnstableService},
        query::QueryOperation,
    },
    event::{Event, id::EventId, lamport::Lamport},
    state::{
        cache::CachedLog,
        effect_context::EffectContext,
        log::IsLog,
        stable_state::IsStableState,
        unstable_state::{IsUnstableCore, IsUnstablePrune, event_graph::EventGraph},
    },
};

// TODO: This should be renamed CachedGraphLog
pub type GraphLog<O> = CachedLog<RawGraphLog<O>>;

#[derive(Debug)]
pub struct RawGraphLog<O>
where
    O: PureCRDT,
{
    stable: <O as PureCRDT>::StableState,
    unstable: EventGraph<O>,
}

impl<O> Clone for RawGraphLog<O>
where
    O: PureCRDT + Clone,
    O::StableState: Clone,
{
    fn clone(&self) -> Self {
        Self {
            stable: self.stable.clone(),
            unstable: self.unstable.clone(),
        }
    }
}

impl<O> IsLog for RawGraphLog<O>
where
    O: PureCRDT + Clone + std::fmt::Debug + UsesUnstableService<EventGraph<O>>,
{
    type Value = <O as PureCRDT>::Value;
    type Op = O;
    type Rejection = O::Rejection;

    fn new() -> Self {
        const {
            debug_assert!(O::DISABLE_R_WHEN_NOT_R && O::DISABLE_R_WHEN_R);
        }
        Self {
            stable: <O as PureCRDT>::StableState::default(),
            unstable: Default::default(),
        }
    }

    fn effect(&mut self, event: Event<Self::Op>, _ctx: &mut EffectContext<'_>) {
        self.unstable.append(event);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        debug_assert!(self.unstable.graph().node_count() >= self.unstable.heads().len());
        match <O as UsesUnstableService<EventGraph<O>>>::causal_reset(
            version,
            conservative,
            &self.stable,
            &self.unstable,
        ) {
            CausalReset::Inject(ops) => {
                for op in ops {
                    let event_id = EventId::from(version);
                    let lamport = Lamport::from(version);
                    let event = Event::new(event_id, lamport, op, version.clone());
                    self.unstable.append(event);
                }
            }
            CausalReset::Prune => {
                debug_assert!(!conservative);
                self.stable.clear();
                self.unstable.clear();
            }
        }
    }

    fn is_default(&self) -> bool {
        self.stable.is_default() && self.unstable.graph().node_count() == 0
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        <O as UsesUnstableService<EventGraph<O>>>::is_enabled(op, &self.stable, &self.unstable)
    }

    fn stabilize(&mut self, version: &Version) {
        self.unstable.stabilize(version);
    }
}

impl<O> Default for RawGraphLog<O>
where
    O: PureCRDT,
{
    fn default() -> Self {
        Self {
            stable: <O as PureCRDT>::StableState::default(),
            unstable: Default::default(),
        }
    }
}

impl<O> RawGraphLog<O>
where
    O: PureCRDT,
{
    pub fn stable(&self) -> &O::StableState {
        &self.stable
    }

    pub fn unstable(&self) -> &EventGraph<O> {
        &self.unstable
    }

    pub fn from_stable(stable: <O as PureCRDT>::StableState) -> Self {
        Self {
            stable,
            unstable: Default::default(),
        }
    }
}

impl<O> GraphLog<O>
where
    O: PureCRDT + Clone + Debug + UsesUnstableService<EventGraph<O>>,
{
    pub fn stable(&self) -> &O::StableState {
        self.inner().stable()
    }

    pub fn unstable(&self) -> &EventGraph<O> {
        self.inner().unstable()
    }

    pub fn from_stable(stable: <O as PureCRDT>::StableState) -> Self {
        Self::from_inner(RawGraphLog::from_stable(stable))
    }
}

impl<O, Q> EvalNested<Q> for RawGraphLog<O>
where
    O: PureCRDT + Clone + Eval<Q, EventGraph<O>> + UsesUnstableService<EventGraph<O>>,
    Q: QueryOperation,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        O::execute_query(q, &self.stable, &self.unstable)
    }
}

#[cfg(feature = "test_utils")]
impl<O> IsLogTest for RawGraphLog<O>
where
    O: PureCRDT + Clone + DeepSizeOf + UsesUnstableService<EventGraph<O>>,
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
