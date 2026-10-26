use std::fmt::{Debug, Display};

use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::{eval::Eval, query::QueryOperation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::{
        stable_state::IsStableState,
        unstable_state::{IsUnstableCore, IsUnstablePrune},
    },
};

pub trait ReplicatedDataType: Debug + Sized {
    type StableState: IsStableState<Self>;
    type Rejection: Debug + Display;

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
        _old_tag: Option<&Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        false
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        _old_tag: Option<&Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        false
    }

    fn on_stabilize(
        _tagged_op: &TaggedOp<Self>,
        _stable: &mut Self::StableState,
        _unstable: &mut impl IsUnstablePrune<Self>,
    ) {
    }

    fn eval<Q, U>(q: &Q, stable: &Self::StableState, unstable: &U) -> Q::Response
    where
        Q: QueryOperation,
        Self: Eval<Q, U>,
    {
        Self::execute_query(q, stable, unstable)
    }
}

pub enum CausalReset<O> {
    Prune,
    Inject(Vec<O>),
}

/// How a CRDT folds causally stable history into its stable state.
pub enum Stabilization<S> {
    Regular,
    Critical { frontier: CausalFrontier, stable: S },
    None,
}

pub trait UsesUnstableService<U>: ReplicatedDataType
where
    U: IsUnstableCore<Self>,
{
    fn causal_stabilize(
        _frontier: &CausalFrontier,
        _stable: &Self::StableState,
        _unstable: &U,
    ) -> Stabilization<Self::StableState> {
        if Self::DISABLE_STABILIZE {
            Stabilization::None
        } else {
            Stabilization::Regular
        }
    }

    fn causal_reset(
        _version: &Version,
        _conservative: bool,
        _stable: &Self::StableState,
        _unstable: &U,
    ) -> CausalReset<Self> {
        CausalReset::Prune
    }

    /// `is_enabled` can inspect the state to determine if the operation violates any precondition.
    fn is_enabled(
        _op: &Self,
        _stable: &Self::StableState,
        _unstable: &U,
    ) -> Result<(), Self::Rejection> {
        Ok(())
    }
}
