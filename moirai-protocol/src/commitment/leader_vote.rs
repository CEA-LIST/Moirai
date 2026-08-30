use std::{convert::Infallible, range::Range};

use crate::{
    clock::version_vector::Version,
    crdt::{
        eval::Eval,
        query::QueryOperation,
        redundancy::RedundancyRelation,
        replicated_data_type::{ReplicatedDataType, UsesUnstableService},
    },
    event::{id::EventId, tagged_op::TaggedOp},
    replica::{ReplicaId, ReplicaIdOwned, ReplicaIdx},
    state::{
        stable_state::IsStableState,
        unstable_state::{IsUnstableCausal, IsUnstableCore, IsUnstablePrune},
    },
};

#[derive(Debug, Clone)]
pub enum LeaderVote {
    Vote(ReplicaIdOwned),
}

impl ReplicatedDataType for LeaderVote {
    type Value = Option<Version>;
    type StableState = ();
    type Rejection = Infallible;

    const DISABLE_R_WHEN_R: bool = false;
    const DISABLE_R_WHEN_NOT_R: bool = false;
    const DISABLE_STABILIZE: bool = false;

    fn redundant_itself<'a>(
        _new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        false
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_tag: Option<&crate::event::tag::Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        false
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        _old_tag: Option<&crate::event::tag::Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        false
    }

    fn stabilize(
        _tagged_op: &TaggedOp<Self>,
        _stable: &mut Self::StableState,
        _unstable: &mut impl IsUnstablePrune<Self>,
    ) {
    }

    fn eval<Q, U>(q: Q, stable: &Self::StableState, unstable: &U) -> Q::Response
    where
        Q: QueryOperation,
        Self: Eval<Q, U>,
    {
        Self::execute_query(q, stable, unstable)
    }
}

impl IsStableState<LeaderVote> for () {
    fn is_default(&self) -> bool {
        true
    }

    fn apply(&mut self, _value: LeaderVote) {}

    fn clear(&mut self) {}

    fn prune_redundant_ops(
        &mut self,
        _rdnt: RedundancyRelation<LeaderVote>,
        _tagged_op: &TaggedOp<LeaderVote>,
    ) {
    }
}

impl LeaderVote {
    pub fn id(&self) -> &ReplicaId {
        match self {
            LeaderVote::Vote(replica_id) => replica_id,
        }
    }
}

impl<U> UsesUnstableService<U> for LeaderVote where U: IsUnstableCore<Self> {}

fn supports<U>(log: &U, event_id: &EventId, supporter: ReplicaIdx) -> bool
where
    U: IsUnstableCausal<LeaderVote>,
{
    let previous = log.previous(event_id, supporter);
    let next = log.next(event_id, supporter);

    let (Some(first), Some(last)) = (previous, next) else {
        return false;
    };

    let first = first.id().seq() - 1;
    let last = last.id().seq() - 1;

    log.replica_events(
        supporter,
        Range {
            start: first,
            end: last + 1,
        },
    )
    .all(|tagged_op| tagged_op.op().id() == event_id.origin_id())
}

/// A newly decided positive `(candidate, supporter)` relationship.
#[derive(Debug, Clone)]
pub struct SupportDelta {
    pub candidate: EventId,
    pub supporter: ReplicaIdx,
}

/// Computes only the positive support relationships decided by one delivered observer event.
#[derive(Debug, Clone)]
pub struct NewSupports {
    observer: EventId,
}

impl NewSupports {
    pub fn new(observer: EventId) -> Self {
        Self { observer }
    }
}

impl QueryOperation for NewSupports {
    type Response = Vec<SupportDelta>;
}

impl<U> Eval<NewSupports, U> for LeaderVote
where
    U: IsUnstableCausal<Self>,
{
    /// Compute `supports(v, r)` for every `v` newly observed by delivered event from replica `r`.
    fn execute_query(
        q: NewSupports,
        _stable: &Self::StableState,
        unstable: &U,
    ) -> <NewSupports as QueryOperation>::Response {
        let supporter = q.observer.idx();

        unstable
            .newly_observed_by(&q.observer)
            .into_iter()
            .filter(|candidate| supports(unstable, candidate.id(), supporter))
            .map(|candidate| SupportDelta {
                candidate: candidate.id().clone(),
                supporter,
            })
            .collect()
    }
}
