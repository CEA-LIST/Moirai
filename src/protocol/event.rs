use std::fmt::Debug;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

// use std::fmt::Display;
// use std::fmt::Error;
// use std::fmt::Formatter;
use crate::clocks::dependency_clock::DependencyClock;
// use super::log::Log;

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

// impl<O, L> Display for Event<O>
// where
//     L: Log<Op = O>,
// {
//     fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
//         write!(f, "[")?;
//         write!(f, "{:?}, {}", self.op, self.metadata)?;
//         write!(f, "]")?;
//         Ok(())
//     }
// }
