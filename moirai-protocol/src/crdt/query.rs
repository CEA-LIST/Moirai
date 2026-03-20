use std::fmt::Debug;

use crate::event::id::EventId;

pub trait IsSemanticallyEmpty {
    fn is_semantically_empty(&self) -> bool;
}

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

impl IsSemanticallyEmpty for bool {
    fn is_semantically_empty(&self) -> bool {
        !*self
    }
}

impl IsSemanticallyEmpty for i32 {
    fn is_semantically_empty(&self) -> bool {
        *self == 0
    }
}

impl IsSemanticallyEmpty for isize {
    fn is_semantically_empty(&self) -> bool {
        *self == 0
    }
}

impl IsSemanticallyEmpty for u32 {
    fn is_semantically_empty(&self) -> bool {
        *self == 0
    }
}

impl IsSemanticallyEmpty for u64 {
    fn is_semantically_empty(&self) -> bool {
        *self == 0
    }
}

impl IsSemanticallyEmpty for u8 {
    fn is_semantically_empty(&self) -> bool {
        *self == 0
    }
}

impl IsSemanticallyEmpty for f32 {
    fn is_semantically_empty(&self) -> bool {
        *self == 0.0
    }
}

impl IsSemanticallyEmpty for f64 {
    fn is_semantically_empty(&self) -> bool {
        *self == 0.0
    }
}

impl IsSemanticallyEmpty for i64 {
    fn is_semantically_empty(&self) -> bool {
        *self == 0
    }
}

impl IsSemanticallyEmpty for usize {
    fn is_semantically_empty(&self) -> bool {
        *self == 0
    }
}

impl IsSemanticallyEmpty for i16 {
    fn is_semantically_empty(&self) -> bool {
        *self == 0
    }
}

impl IsSemanticallyEmpty for char {
    fn is_semantically_empty(&self) -> bool {
        false
    }
}

impl IsSemanticallyEmpty for String {
    fn is_semantically_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<T> IsSemanticallyEmpty for Vec<T> {
    fn is_semantically_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<T: IsSemanticallyEmpty> IsSemanticallyEmpty for Option<T> {
    fn is_semantically_empty(&self) -> bool {
        match self {
            None => true,
            Some(v) => v.is_semantically_empty(),
        }
    }
}

impl<T: IsSemanticallyEmpty> IsSemanticallyEmpty for Box<T> {
    fn is_semantically_empty(&self) -> bool {
        self.as_ref().is_semantically_empty()
    }
}

impl<K, V, S> IsSemanticallyEmpty for std::collections::HashMap<K, V, S>
where
    S: std::hash::BuildHasher,
{
    fn is_semantically_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<V, S> IsSemanticallyEmpty for std::collections::HashSet<V, S>
where
    S: std::hash::BuildHasher,
{
    fn is_semantically_empty(&self) -> bool {
        self.is_empty()
    }
}
