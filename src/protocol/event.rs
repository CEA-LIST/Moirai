use std::fmt::{Debug, Display, Error, Formatter};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use tsify::Tsify;

use crate::clocks::dependency_clock::DependencyClock;

#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct Event<O> {
    pub op: O,
    pub metadata: DependencyClock,
}

impl<O> Event<O> {
    pub fn new(op: O, metadata: DependencyClock) -> Self {
        Self { op, metadata }
    }
}

impl<O> Display for Event<O>
where
    O: Debug,
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "[")?;
        write!(f, "{:?}, {}", self.op, self.metadata)?;
        write!(f, "]")?;
        Ok(())
    }
}
