use super::metadata::Metadata;
use std::fmt::Debug;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Event<O> {
    pub op: O,
    pub metadata: Metadata,
}

impl<O> Event<O> {
    pub fn new(op: O, metadata: Metadata) -> Self {
        Self { op, metadata }
    }
}
