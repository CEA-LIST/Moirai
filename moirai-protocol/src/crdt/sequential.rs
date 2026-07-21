use std::fmt::{Debug, Display};

use crate::crdt::query::QueryOperation;

pub trait SequentialDataType: Default + Clone + Debug {
    type Update: Debug + Clone;
    type Value: Default + Debug;
    type Rejection: Debug + Display;

    fn is_enabled(&self, update: &Self::Update) -> Result<(), Self::Rejection>;

    fn apply(&mut self, update: &Self::Update) -> Result<(), Self::Rejection>;
}

pub trait ExecuteQuery<Q>: SequentialDataType
where
    Q: QueryOperation,
{
    fn execute_query(&self, q: Q) -> Q::Response;
}
