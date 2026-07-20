#![allow(clippy::mutable_key_type)]

use std::{collections::BTreeSet, fmt::Debug};

use crate::{
    clock::version_vector::Version,
    commitment::{CommitOp, CommitmentProtocol, MajorityOmegaCommitment},
    crdt::{
        eval::EvalNested,
        query::QueryOperation,
        sequential::{ExecuteQuery, SequentialADT},
    },
    event::{Event, id::EventId},
    state::{
        effect_context::EffectContext,
        log::IsLog,
        unstable_state::{IsUnstableCore, event_graph::EventGraph},
    },
};

/// Partially ordered log for sequential data types with a commitment protocol.
///
/// The graph stores delivered updates. The committed prefix is replayed
/// into `committed`.
/// Uncommitted events stay in the graph and are replayed on
/// demand for reads/precondition checks.
#[derive(Debug, Clone)]
pub struct CommitLog<A, C = MajorityOmegaCommitment>
where
    A: SequentialADT,
    C: CommitmentProtocol<A::Update>,
{
    /// Committed state of the sequential ADT, which is the result of applying all committed updates.
    committed: A,
    /// Unstable graph of delivered updates that have not yet been committed.
    unstable: EventGraph<CommitOp<A::Update>>,
    /// Commitment protocol that determines which updates are considered committed.
    commitment: C,
    /// Record of applied commits to avoid reapplying them.
    applied_commits: BTreeSet<EventId>,
    /// Number of members in the system, used by the commitment protocol.
    // TODO: use the resolver to get this information
    n_members: usize,
}

impl<A, C> CommitLog<A, C>
where
    A: SequentialADT,
    C: CommitmentProtocol<A::Update>,
{
    pub fn new(n_members: usize, commitment: C) -> Self {
        Self {
            committed: A::default(),
            unstable: EventGraph::default(),
            commitment,
            applied_commits: BTreeSet::new(),
            n_members: n_members.max(1),
        }
    }

    // Getters

    pub fn committed(&self) -> &A {
        &self.committed
    }

    pub fn unstable(&self) -> &EventGraph<CommitOp<A::Update>> {
        &self.unstable
    }

    pub fn commitment(&self) -> &C {
        &self.commitment
    }

    pub fn n_members(&self) -> usize {
        self.n_members
    }

    fn commit(&mut self, event_id: &EventId) {
        let cut = self.unstable.causal_cut_ids(event_id);
        let ordered = self.unstable.deterministic_causal_order(&cut);

        for event_id in ordered {
            if self.applied_commits.contains(&event_id) {
                continue;
            }

            let Some(entry) = self.unstable.get(&event_id) else {
                continue;
            };

            if let Err(err) = self.committed.apply(&entry.op().update) {
                panic!("committed update {event_id} rejected by sequential ADT: {err}");
            }

            self.applied_commits.insert(event_id);
        }
    }

    // fn materialize(&self) -> A {
    //     let mut state = self.committed.clone();
    //     let event_ids: BTreeSet<EventId> = self
    //         .unstable
    //         .iter()
    //         .map(|tagged| tagged.id().clone())
    //         .collect();
    //     let ordered = self.unstable.deterministic_causal_order(&event_ids);

    //     for event_id in ordered {
    //         if self.applied_commits.contains(&event_id) {
    //             continue;
    //         }

    //         let Some(entry) = self.unstable.get(&event_id) else {
    //             continue;
    //         };

    //         if let Err(err) = state.apply(&entry.op().update) {
    //             panic!("tentative update {event_id} rejected by sequential ADT: {err}");
    //         }
    //     }

    //     state
    // }
}

impl<A, C> Default for CommitLog<A, C>
where
    A: SequentialADT,
    C: CommitmentProtocol<A::Update> + Default,
{
    fn default() -> Self {
        Self::new(1, C::default())
    }
}

impl<A, C> IsLog for CommitLog<A, C>
where
    A: SequentialADT,
    C: CommitmentProtocol<A::Update> + Default,
{
    type Value = A;
    type Op = CommitOp<A::Update>;
    type Rejection = A::Rejection;

    fn new() -> Self {
        Self::default()
    }

    fn is_enabled(&self, _op: &Self::Op) -> Result<(), Self::Rejection> {
        // self.materialize().is_enabled(&op.update)
        todo!()
    }

    fn effect(&mut self, event: Event<Self::Op>, _ctx: &mut EffectContext<'_>) {
        let delivered = event.id().clone();
        self.unstable.append(event);

        let commits = self
            .commitment
            .on_deliver(&delivered, &self.unstable, self.n_members);
        for commit in commits {
            self.commit(&commit);
        }
    }

    fn stabilize(&mut self, _version: &Version) {}

    fn redundant_by_parent(&mut self, _version: &Version, _conservative: bool) {}

    fn is_default(&self) -> bool {
        self.committed.is_default() && self.unstable.is_empty()
    }
}

impl<Q, A, C> EvalNested<Q> for CommitLog<A, C>
where
    Q: QueryOperation,
    A: SequentialADT + ExecuteQuery<Q>,
    C: CommitmentProtocol<A::Update> + Default,
{
    fn execute_query(&self, _q: Q) -> Q::Response {
        // self.materialize().execute_query(q)
        todo!()
    }
}
