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

#[derive(Debug, Clone)]
pub struct Contains<V>(pub V);

impl<V> QueryOperation for Contains<V>
where
    V: Debug + Clone,
{
    type Response = bool;
}

/// A query operation to get the value associated with a key.
/// # Type Parameters
/// - `K`: The type of the key.
/// - `V`: The type of the returned value.
pub struct Get<K, V>(pub K, std::marker::PhantomData<V>);

impl<K, V> Get<K, V> {
    pub fn new(key: K) -> Self {
        Self(key, std::marker::PhantomData)
    }
}

impl<K, V> QueryOperation for Get<K, V> {
    type Response = Option<V>;
}

pub struct NestedGet<K, Q> {
    pub key: K,
    pub nested_query: Q,
}

impl<K, Q> NestedGet<K, Q> {
    pub fn new(key: K, nested_query: Q) -> Self {
        Self { key, nested_query }
    }
}

impl<K, Q> QueryOperation for NestedGet<K, Q>
where
    Q: QueryOperation,
{
    type Response = Option<Q::Response>;
}
