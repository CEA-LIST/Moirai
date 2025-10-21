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
    type Value: Default + Debug;
    type StableState: IsStableState<Self>;

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
        _unstable: &mut impl IsUnstableState<Self>,
    ) {
    }

    fn eval<Q>(
        q: Q,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> Q::Response
    where
        Q: QueryOperation,
        Self: Eval<Q>,
    {
        Self::execute_query(q, stable, unstable)
    }

    // `is_enabled` can inspect the state to determine if the operation violates any precondition.
    // fn is_enabled(_op: &Self, _state: impl Fn() -> Self::Value) -> bool {
    //     true
    // }
}

pub trait QueryOperation {
    type Response;
}

pub trait Eval<Q>
where
    Q: QueryOperation,
    Self: PureCRDT,
{
    fn execute_query(
        q: Q,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> Q::Response;
}

#[derive(Debug)]
pub struct Read<Crdt>(std::marker::PhantomData<Crdt>);

impl<V> QueryOperation for Read<V> {
    type Response = V;
}

impl<V> Read<V> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<V> Default for Read<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct Contains<V>(pub V);

impl<V> QueryOperation for Contains<V>
where
    V: Debug + Clone,
{
    type Response = bool;
}

pub struct Get<K, V>(pub K, std::marker::PhantomData<V>);

impl<K, V> Get<K, V> {
    pub fn new(key: K) -> Self {
        Self(key, std::marker::PhantomData)
    }
}

impl<K, V> QueryOperation for Get<K, V> {
    type Response = Option<V>;
}
