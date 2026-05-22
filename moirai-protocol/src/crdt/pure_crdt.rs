use std::fmt::{Debug, Display};

use crate::{
    clock::version_vector::Version,
    crdt::{eval::Eval, query::QueryOperation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::{
        stable_state::IsStableState,
        unstable_state::{CausalReplay, IsUnstablePrune},
    },
};

pub enum CausalReset<O> {
    Prune,
    Inject(Vec<O>),
}

pub trait PureCRDT: Debug + Sized {
    // TODO: try to get rid of this
    type Value: Default + Debug;
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

    fn causal_reset(
        _version: &Version,
        _conservative: bool,
        _stable: &Self::StableState,
        _unstable: &impl CausalReplay<Self>,
    ) -> CausalReset<Self> {
        CausalReset::Prune
    }

    /// `is_enabled` can inspect the state to determine if the operation violates any precondition.
    fn is_enabled(
        _op: &Self,
        _stable: &Self::StableState,
        _unstable: &impl CausalReplay<Self>,
    ) -> Result<(), Self::Rejection> {
        Ok(())
    }
}
