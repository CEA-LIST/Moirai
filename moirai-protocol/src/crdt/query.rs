use std::fmt::Debug;

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

#[derive(Debug)]
pub struct ReadStable<V>(std::marker::PhantomData<V>);

impl<V> QueryOperation for ReadStable<V> {
    type Response = V;
}

impl<V> ReadStable<V> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<V> Default for ReadStable<V> {
    fn default() -> Self {
        Self::new()
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
