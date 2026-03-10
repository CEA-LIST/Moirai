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

pub struct Get<K, Q> {
    pub key: K,
    pub nested_query: Q,
}

impl<K, Q> Get<K, Q> {
    pub fn new(key: K, nested_query: Q) -> Self {
        Self { key, nested_query }
    }
}

impl<K, Q> QueryOperation for Get<K, Q>
where
    Q: QueryOperation,
{
    type Response = Option<Q::Response>;
}
