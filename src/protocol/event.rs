use super::{metadata::Metadata, pure_crdt::PureCRDT};
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct Event<O>
where
    O: PureCRDT,
{
    pub op: O,
    pub metadata: Metadata,
}

impl<O> Event<O>
where
    O: PureCRDT,
{
    pub fn new(op: O, metadata: Metadata) -> Self {
        Self { op, metadata }
    }
}
