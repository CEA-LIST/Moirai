use super::{
    metadata::Metadata,
    pure_crdt::PureCRDT,
    utils::{Incrementable, Keyable},
};
use std::{fmt::Debug, path::PathBuf};

#[derive(Clone, Debug)]
pub struct Event<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT + Clone + Debug,
{
    pub op: O,
    pub path: PathBuf,
    pub metadata: Metadata<K, C>,
}

impl<K, C, O> Event<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT + Clone + Debug,
{
    pub fn new(path: PathBuf, op: O, metadata: Metadata<K, C>) -> Self {
        Self { path, op, metadata }
    }
}

#[derive(Clone, Debug)]
pub struct NestedOp<O> {
    pub op: O,
    pub path: PathBuf,
}

impl<O> NestedOp<O> {
    pub fn new(path: PathBuf, op: O) -> Self {
        Self { path, op }
    }
}
