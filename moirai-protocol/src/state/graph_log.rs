use std::fmt::Debug;

use crate::{
    clock::version_vector::Version,
    crdt::{
        eval::{Eval, EvalNested},
        query::QueryOperation,
        replicated_data_type::{CausalReset, ReplicatedDataType, UsesUnstableService},
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
    O: ReplicatedDataType,
{
    stable: <O as ReplicatedDataType>::StableState,
    unstable: EventGraph<O>,
}

impl<O> Clone for RawGraphLog<O>
where
    O: ReplicatedDataType + Clone,
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
    O: ReplicatedDataType + Clone + UsesUnstableService<EventGraph<O>>,
{
    type Value = <O as ReplicatedDataType>::Value;
    type Command = O;
    type Op = O;
    type Rejection = O::Rejection;

    fn new() -> Self {
        const {
            debug_assert!(O::DISABLE_R_WHEN_NOT_R && O::DISABLE_R_WHEN_R);
        }
        Self {
            stable: <O as ReplicatedDataType>::StableState::default(),
            unstable: Default::default(),
        }
    }

    fn prepare(&self, command: Self::Command) -> Self::Op {
        command
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
    O: ReplicatedDataType,
{
    fn default() -> Self {
        Self {
            stable: <O as ReplicatedDataType>::StableState::default(),
            unstable: Default::default(),
        }
    }
}

impl<O> RawGraphLog<O>
where
    O: ReplicatedDataType,
{
    pub fn stable(&self) -> &O::StableState {
        &self.stable
    }

    pub fn unstable(&self) -> &EventGraph<O> {
        &self.unstable
    }

    pub fn from_stable(stable: <O as ReplicatedDataType>::StableState) -> Self {
        Self {
            stable,
            unstable: Default::default(),
        }
    }
}

impl<O> GraphLog<O>
where
    O: ReplicatedDataType + Clone + Debug + UsesUnstableService<EventGraph<O>>,
{
    pub fn stable(&self) -> &O::StableState {
        self.inner().stable()
    }

    pub fn unstable(&self) -> &EventGraph<O> {
        self.inner().unstable()
    }

    pub fn from_stable(stable: <O as ReplicatedDataType>::StableState) -> Self {
        Self::from_inner(RawGraphLog::from_stable(stable))
    }
}

impl<O, Q> EvalNested<Q> for RawGraphLog<O>
where
    O: ReplicatedDataType + Clone + Eval<Q, EventGraph<O>> + UsesUnstableService<EventGraph<O>>,
    Q: QueryOperation,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        O::execute_query(q, &self.stable, &self.unstable)
    }
}
