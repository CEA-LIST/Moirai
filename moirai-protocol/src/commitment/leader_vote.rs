/////////////////////////////////////////////////////////////////////////////
//  Copyright (c) 2025, 2026 CEA LIST.
//
//  This program and the accompanying materials
//  are made available under the terms of the Apache License, Version 2.0
//  which accompanies this distribution, and is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Contributors:
// Léo Olivier -  (CEA LIST) - Initial implementation
//
/////////////////////////////////////////////////////////////////////////////

use std::{convert::Infallible, range::Range};

use crate::{
    clock::causal_frontier::CausalFrontier,
    data_type::{
        query::QueryOperation,
        replicated::{
            CausalCompaction, CausalReset, CausalStabilize, Eval, Precondition, ReplicatedDataType,
        },
        sequential::SequentialDataType,
    },
    event::id::EventId,
    replica::{ReplicaId, ReplicaIdx},
    state::unstable_state::{IsUnstableCausal, IsUnstableCore},
};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub enum LeaderVoteUpdate {
    Vote(ReplicaId),
}

impl LeaderVoteUpdate {
    pub const fn id(&self) -> &ReplicaId {
        match self {
            Self::Vote(replica_id) => replica_id,
        }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct LeaderVote;

impl LeaderVote {
    fn supports<U>(log: &U, event_id: &EventId, supporter: ReplicaIdx) -> bool
    where
        U: IsUnstableCausal<<Self as ReplicatedDataType>::Update>
            + IsUnstableCore<<Self as ReplicatedDataType>::Update>,
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
}

impl SequentialDataType for () {
    type Rejection = Infallible;
    type Update = LeaderVoteUpdate;

    fn is_enabled(&self, _update: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }

    fn is_default(&self) -> bool {
        true
    }

    fn apply(&mut self, _: &Self::Update) {}
}

impl ReplicatedDataType for LeaderVote {
    type Update = LeaderVoteUpdate;
    type Stable = ();
    type Rejection = Infallible;
}

impl<U> Precondition<U> for LeaderVote {
    fn is_enabled(
        _stable: &<Self as ReplicatedDataType>::Stable,
        _unstable: &U,
        _update: &Self::Update,
    ) -> Result<(), Infallible> {
        Ok(())
    }
}

impl<U> CausalCompaction<U> for LeaderVote
where
    U: IsUnstableCore<Self::Update>,
{
    const DISABLE_R_WHEN_R: bool = true;
    const DISABLE_R_WHEN_NOT_R: bool = true;
}

impl<U> CausalStabilize<U> for LeaderVote
where
    U: IsUnstableCausal<Self::Update>,
{
    const DISABLE_STABILIZE: bool = false;
}

impl<U> CausalReset<U> for LeaderVote where U: IsUnstableCausal<Self::Update> {}

/// A newly decided positive `(candidate, supporter)` relationship.
#[derive(Debug, Clone)]
pub struct SupportDelta {
    pub candidate: EventId,
    pub supporter: ReplicaIdx,
}

/// Computes only the positive support relationships decided by one delivered observer event.
///
/// # Complexity
///
/// Expected time is `O(N + E * H)`, where:
/// - `N` is the number of replicas,
/// - `E` is the number of newly observed candidates,
/// - `H` is the number of stored events from the observer's replica.
#[derive(Debug, Clone)]
pub struct NewSupports {
    observer: EventId,
    last_committed: Option<CausalFrontier>,
}

impl NewSupports {
    pub const fn new(observer: EventId, last_committed: Option<CausalFrontier>) -> Self {
        Self {
            observer,
            last_committed,
        }
    }
}

impl QueryOperation for NewSupports {
    type Response = Vec<SupportDelta>;

    #[cfg(feature = "test_utils")]
    fn name() -> &'static str {
        "NewSupports"
    }

    #[cfg(feature = "test_utils")]
    fn args(&self) -> Vec<String> {
        vec![format!("observer={:?}", self.observer)]
    }
}

impl<U> Eval<U, NewSupports> for LeaderVote
where
    U: IsUnstableCausal<<Self as ReplicatedDataType>::Update>
        + IsUnstableCore<<Self as ReplicatedDataType>::Update>,
{
    /// Compute `supports(v, r)` for every `v` newly observed by delivered event from replica `r`.
    fn execute_query(
        _stable: &(),
        unstable: &U,
        q: &NewSupports,
    ) -> <NewSupports as QueryOperation>::Response {
        let supporter = q.observer.idx();

        unstable
            .newly_observed_by(&q.observer)
            .into_iter()
            .filter(|candidate| {
                if let Some(last_committed) = &q.last_committed {
                    if last_committed.contains(candidate.id()) {
                        return false;
                    }
                    if let Some(version) = unstable.version(candidate.id())
                        && !version.includes_frontier(last_committed)
                    {
                        return false;
                    }
                }
                Self::supports(unstable, candidate.id(), supporter)
            })
            .map(|candidate| SupportDelta {
                candidate: candidate.id().clone(),
                supporter,
            })
            .collect()
    }
}
