use crate::{
    crdt::{
        query::{QueryOperation, ReadStable},
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

pub trait BorrowedRead<V>: IsLog {
    fn read_ref(&self) -> &V;
}

impl<L, Q> EvalNested<Q> for Box<L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q>,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        (**self).execute_query(q)
    }
}

impl<L, V> BorrowedRead<V> for Box<L>
where
    L: IsLog + BorrowedRead<V>,
{
    fn read_ref(&self) -> &V {
        (**self).read_ref()
    }
}
