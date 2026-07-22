#![allow(clippy::mutable_key_type)]

use crate::{
    clock::version_vector::Version,
    commitment::commit_op::CommitOp,
    crdt::{
        eval::EvalNested,
        policy::{LwwPolicy, Policy},
        query::QueryOperation,
        sequential::{ExecuteQuery, SequentialDataType},
    },
    event::{Event, id::EventId},
    state::{
        effect_context::EffectContext,
        event_graph::EventGraph,
        log::IsLog,
        unstable_state::{IsUnstableCore, IsUnstablePrune},
    },
    utils::hashmap::HashSet,
};

#[derive(Debug, Clone)]
pub struct CommitLog<A>
where
    A: SequentialDataType,
{
    /// Committed operations that have been applied to the sequential state
    committed: A,
    /// Directed Acyclic Graph (DAG) of operations that have been delivered but not yet committed
    unstable: EventGraph<CommitOp<A::Update>>,
}

impl<A> Default for CommitLog<A>
where
    A: SequentialDataType,
{
    fn default() -> Self {
        Self {
            committed: A::default(),
            unstable: EventGraph::default(),
        }
    }
}

impl<A> IsLog for CommitLog<A>
where
    A: SequentialDataType,
{
    type Value = A::Value;
    type Op = CommitOp<A::Update>;
    type Rejection = A::Rejection;

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        self.replay().is_enabled(&op.update)
    }

    fn effect(&mut self, event: Event<Self::Op>, _ctx: &mut EffectContext<'_>) {
        self.unstable.append(event);
    }

    // TODO: How to use both committment and causal stability?
    fn stabilize(&mut self, _version: &Version) {}

    // TODO: useless?
    fn redundant_by_parent(&mut self, _version: &Version, _conservative: bool) {}

    fn is_default(&self) -> bool {
        self.unstable.is_empty()
    }
}

impl<A> CommitLog<A>
where
    A: SequentialDataType,
{
    pub fn unstable(&self) -> &EventGraph<CommitOp<A::Update>> {
        &self.unstable
    }

    /// Commit every operation from the unstable state that is causally before the given frontier.
    /// Returns the list of committed event IDs.
    pub fn commit_frontier<P: Policy>(&mut self, frontier: &HashSet<EventId>) -> Vec<EventId> {
        let predecessors = self.unstable.predecessors_by_ids::<P>(frontier);
        self.commit(predecessors)
    }

    /// Commit the operations whose event IDs are in the given list, in the same order as the list.
    /// Returns the list of committed event IDs.
    fn commit(&mut self, ordered: Vec<EventId>) -> Vec<EventId> {
        let mut committed = Vec::new();

        for id in ordered {
            if let Some(entry) = self.unstable.get(&id) {
                self.committed.apply(&entry.op().update);
                self.unstable.remove(&id);
                committed.push(id);
            }
        }

        committed
    }

    /// Replay the operations from the log on the committed state to produce the current state.
    /// It applies the operations from the unstable state to the committed state, sorted in a deterministic topological order.
    fn replay(&self) -> A {
        let mut state = self.committed.clone();

        for (_, tagged_op) in self.unstable.linearize::<LwwPolicy>() {
            state.apply(&tagged_op.op().update);
        }

        state
    }
}

impl<Q, A> EvalNested<Q> for CommitLog<A>
where
    Q: QueryOperation,
    A: SequentialDataType + ExecuteQuery<Q>,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        self.replay().execute_query(q)
    }
}
