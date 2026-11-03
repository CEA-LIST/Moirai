use crate::crdt::query::{QueryOperation, Read};

pub trait Eval<S, U, Q>
where
    Q: QueryOperation,
{
    fn execute_query(stable: &S, unstable: &U, q: &Q) -> Q::Response;
}

pub trait EvalNested<Q>
where
    Q: QueryOperation,
{
    fn execute_query(&self, q: &Q) -> Q::Response;
}

impl<L, V> EvalNested<Read<Box<V>>> for Box<L>
where
    L: EvalNested<Read<V>>,
{
    fn execute_query(&self, _q: &Read<Box<V>>) -> Box<V> {
        Box::new((**self).execute_query(&Read::<V>::new()))
    }
}
