use crate::{
    crdt::{
        query::{QueryOperation, Read, ReadStable},
        replicated_data_type::ReplicatedDataType,
    },
    state::log::IsLog,
};

pub trait Eval<Q, U>: ReplicatedDataType
where
    Q: QueryOperation,
{
    fn execute_query(q: &Q, stable: &Self::StableState, unstable: &U) -> Q::Response;
}

impl<O, U> Eval<ReadStable<O::StableState>, U> for O
where
    O: ReplicatedDataType,
    O::StableState: Clone,
{
    fn execute_query(
        _q: &ReadStable<O::StableState>,
        stable: &O::StableState,
        _unstable: &U,
    ) -> O::StableState {
        stable.clone()
    }
}

pub trait EvalNested<Q>: IsLog
where
    Q: QueryOperation,
{
    fn execute_query(&self, q: &Q) -> Q::Response;
}

/// Reading an explicitly boxed child preserves that indirection in its value.
/// Collection logs can still read unboxed children without allocating per value.
impl<L, V> EvalNested<Read<Box<V>>> for Box<L>
where
    L: EvalNested<Read<V>>,
{
    fn execute_query(&self, _q: &Read<Box<V>>) -> Box<V> {
        Box::new((**self).execute_query(&Read::<V>::new()))
    }
}
