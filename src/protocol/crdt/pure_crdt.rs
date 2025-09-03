use std::fmt::Debug;

use crate::protocol::{
    event::{tag::Tag, tagged_op::TaggedOp},
    state::{stable_state::IsStableState, unstable_state::IsUnstableState},
};

pub type RedundancyRelation<O> = fn(
    _old_op: &O,
    _old_event_id: Option<&Tag>,
    is_conc: bool,
    new_tagged_op: &TaggedOp<O>,
) -> bool;

pub trait PureCRDT: Debug + Sized {
    type Value: Debug + Default;
    type StableState: IsStableState<Self>;

    const DISABLE_R_WHEN_R: bool = false;
    const DISABLE_R_WHEN_NOT_R: bool = false;

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
        _unstable: &mut impl IsUnstableState<Self>,
    ) {
    }

    fn eval<'a>(
        stable: &Self::StableState,
        unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> Self::Value
    where
        Self: 'a;
}
