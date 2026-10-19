use std::fmt::Debug;

use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::{
        eval::{Eval, EvalNested},
        query::QueryOperation,
        replicated_data_type::{CausalReset, ReplicatedDataType, UsesUnstableService},
    },
    event::{Event, id::EventId, lamport::Lamport},
    state::{
        effect_context::EffectContext,
        log::IsLog,
        stable_state::IsStableState,
        unstable_state::{IsUnstableCore, IsUnstablePrune, event_graph::EventGraph},
    },
};

#[derive(Debug)]
pub struct GraphLog<O>
where
    O: ReplicatedDataType,
{
    stable: <O as ReplicatedDataType>::StableState,
    unstable: EventGraph<O>,
}

impl<O> Clone for GraphLog<O>
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

#[cfg(feature = "test_utils")]
impl<O> deepsize::DeepSizeOf for GraphLog<O>
where
    O: ReplicatedDataType + deepsize::DeepSizeOf,
    O::StableState: deepsize::DeepSizeOf,
{
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.stable.deep_size_of_children(context) + self.unstable.deep_size_of_children(context)
    }
}

impl<O> IsLog for GraphLog<O>
where
    O: ReplicatedDataType + Clone + UsesUnstableService<EventGraph<O>>,
{
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

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        let prefix_len = self.unstable.stable_critical_prefix(frontier);
        if prefix_len > 0
            && let Some(snapshot) = O::checkpoint(&self.stable, &self.unstable, prefix_len)
        {
            // Build both replacements before committing either one.
            let retained = self.unstable.after_checkpoint(prefix_len);
            self.stable = snapshot;
            self.unstable = retained;
        }
        self.unstable.stabilize(frontier);
    }
}

impl<O> Default for GraphLog<O>
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

impl<O> GraphLog<O>
where
    O: ReplicatedDataType,
{
    pub fn stable(&self) -> &O::StableState {
        &self.stable
    }

    pub fn unstable(&self) -> &EventGraph<O> {
        &self.unstable
    }

    /// Start a new history from a supplied baseline. To restore flushed history,
    /// use `from_checkpoint` so its retention boundary is preserved as well.
    pub fn from_stable(stable: <O as ReplicatedDataType>::StableState) -> Self {
        Self {
            stable,
            unstable: Default::default(),
        }
    }

    /// Restore a snapshot together with its retention boundary. The frontier
    /// must use the same replica mapping as subsequent events and queries;
    /// the broadcast layer must also be restored to at least this causal cut.
    pub fn from_checkpoint(stable: O::StableState, boundary: CausalFrontier) -> Self {
        Self {
            stable,
            unstable: EventGraph::from_checkpoint(boundary),
        }
    }

    pub fn retention_boundary(&self) -> Option<&CausalFrontier> {
        self.unstable.retention_boundary()
    }
}

impl<O, Q> EvalNested<Q> for GraphLog<O>
where
    O: ReplicatedDataType + Clone + Eval<Q, EventGraph<O>> + UsesUnstableService<EventGraph<O>>,
    Q: QueryOperation,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        O::execute_query(q, &self.stable, &self.unstable)
    }
}
