use crate::{
    crdt::{pure_crdt::PureCRDT, query::QueryOperation},
    state::log::IsLog,
};

pub trait Eval<Q, U>
where
    Q: QueryOperation,
    Self: PureCRDT,
{
    fn execute_query(q: Q, stable: &Self::StableState, unstable: &U) -> Q::Response;
}

pub trait EvalNested<Q>
where
    Q: QueryOperation,
    Self: IsLog,
{
    fn execute_query(&self, q: Q) -> Q::Response;
}

/// Read capability for logs that keep a materialized value available by reference.
///
/// This is intentionally separate from `Read<V>` because not every log can return
/// a borrowed value without first materializing or caching it.
pub trait BorrowedRead: IsLog {
    fn read_ref(&self) -> &Self::Value;
}

impl<L, Q> EvalNested<Q> for Box<L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q>,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        (**self).execute_query(q)
    }
}

impl<L> BorrowedRead for Box<L>
where
    L: IsLog + BorrowedRead,
{
    fn read_ref(&self) -> &Self::Value {
        (**self).read_ref()
    }
}
