use std::fmt::Debug;

pub trait QueryOperation {
    type Response;
}

#[derive(Debug)]
pub struct Read<Crdt>(std::marker::PhantomData<Crdt>);

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

pub struct Get<K, V>(pub K, std::marker::PhantomData<V>);

impl<K, V> Get<K, V> {
    pub fn new(key: K) -> Self {
        Self(key, std::marker::PhantomData)
    }
}

impl<K, V> QueryOperation for Get<K, V> {
    type Response = Option<V>;
}
