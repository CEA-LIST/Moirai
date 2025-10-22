use crate::protocol::{
    crdt::{pure_crdt::PureCRDT, query::QueryOperation},
    state::{log::IsLog, unstable_state::IsUnstableState},
};

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

pub trait EvalNested<Q>
where
    Q: QueryOperation,
    Self: IsLog,
{
    fn execute_query(&self, q: Q) -> Q::Response;
}
