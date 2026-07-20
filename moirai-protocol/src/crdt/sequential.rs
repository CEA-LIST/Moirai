use std::fmt::{Debug, Display};

use crate::{
    broadcast::internalizer::InternalizeOp,
    crdt::query::{QueryOperation, Read},
};

pub trait SequentialADT: Default + Clone + Debug {
    type Update: Clone + Debug + InternalizeOp;
    type Rejection: Debug + Display;

    fn is_enabled(&self, update: &Self::Update) -> Result<(), Self::Rejection>;
    fn apply(&mut self, update: &Self::Update) -> Result<(), Self::Rejection>;
    fn is_default(&self) -> bool;
}

// TODO: merge this trait with the query execution of CRDTs?

pub trait ExecuteQuery<Q>: SequentialADT
where
    Q: QueryOperation,
{
    fn execute_query(&self, q: Q) -> Q::Response;
}

impl<A> ExecuteQuery<Read<A>> for A
where
    A: SequentialADT,
{
    fn execute_query(&self, _q: Read<A>) -> A {
        self.clone()
    }
}
