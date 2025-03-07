use std::fmt::Debug;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::clocks::dependency_clock::DependencyClock;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Event<O> {
    pub op: O,
    pub metadata: DependencyClock,
}

impl<O> Event<O> {
    pub fn new(op: O, metadata: DependencyClock) -> Self {
        Self { op, metadata }
    }
}
