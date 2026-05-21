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

impl<L, Q> EvalNested<Q> for Box<L>
where
    Q: QueryOperation,
    L: IsLog + EvalNested<Q>,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        (**self).execute_query(q)
    }
}
