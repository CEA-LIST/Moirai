use std::fmt::Debug;

use crate::event::id::EventId;

pub trait QueryOperation {
    type Response;
}

#[derive(Debug)]
pub struct Read<V>(std::marker::PhantomData<V>);

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

pub struct ReadId;

impl QueryOperation for ReadId {
    type Response = Option<EventId>;
}

impl Default for ReadId {
    fn default() -> Self {
        Self
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

pub struct Get<'a, K, Q> {
    pub key: &'a K,
    pub nested_query: Q,
}

impl<'a, K, Q> Get<'a, K, Q> {
    pub fn new(key: &'a K, nested_query: Q) -> Self {
        Self { key, nested_query }
    }
}

impl<'a, K, Q> QueryOperation for Get<'a, K, Q>
where
    Q: QueryOperation,
{
    type Response = Option<Q::Response>;
}
