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

pub struct CommitFrontier {
    members: Vec<ReplicaIdx>,
    quorum: usize,
}

impl CommitFrontier {
    pub fn new(members: Vec<ReplicaIdx>, quorum: usize) -> Self {
        Self { members, quorum }
    }
}

impl QueryOperation for CommitFrontier {
    type Response = Option<Version>;
}

impl<U> Eval<CommitFrontier, U> for LeaderVote
where
    U: IsUnstableCausal<Self>,
{
    fn execute_query(
        q: CommitFrontier,
        _stable: &Self::StableState,
        unstable: &U,
    ) -> <CommitFrontier as QueryOperation>::Response {
        fn supports<U: IsUnstableCausal<LeaderVote>>(
            log: &U,
            candidate_version: &Version,
            r: ReplicaIdx,
        ) -> bool {
            // An event becomes ready to be committed if it gathers support from "enough" events around its past and future
            // The causal region of an event e is comprised of its immediate past (the latest events from each replica seen by e),
            // its immediate future (the earliest events from each replica to see e), and vertices "in between".

            let previous = log.previous(candidate_version, r);
            let next = log.next(&EventId::from(candidate_version), r);

            let (Some(first), Some(last)) = (previous, next) else {
                return false;
            };

            let first = first.id().seq() - 1;
            let last = last.id().seq() - 1;

            log.replica_events(
                r,
                Range {
                    start: first,
                    end: last + 1,
                },
            )
            .all(|to| to.op().id() == candidate_version.origin_id())
        }

        let leaders: Vec<_> = unstable
            .versioned_events()
            .filter(|(_, v)| {
                q.members
                    .iter()
                    .filter(|&&r| supports(unstable, v, r))
                    .take(q.quorum)
                    .count()
                    >= q.quorum
            })
            .collect();

        leaders
            .iter()
            .copied()
            .find(|(_, candidate_version)| {
                leaders
                    .iter()
                    .all(|(_, v)| EventId::from(*v).is_predecessor_of(candidate_version))
            })
            .map(move |(_, v)| v.clone())
    }
}
