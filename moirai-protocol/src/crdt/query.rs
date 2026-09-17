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

#[derive(Clone, Copy, Debug, Default)]
pub struct Size;

impl Size {
    pub fn new() -> Self {
        Self
    }
}

impl QueryOperation for Size {
    type Response = usize;
}

pub struct Get<K, Q> {
    pub key: K,
    pub nested_query: Q,
}

impl<K, Q> Get<K, Q>
where
    K: Clone,
{
    pub fn new(key: &K, nested_query: Q) -> Self {
        Self {
            key: key.clone(),
            nested_query,
        }
    }
}

impl<K, Q> QueryOperation for Get<K, Q>
where
    Q: QueryOperation,
{
    type Response = Option<Q::Response>;
}
