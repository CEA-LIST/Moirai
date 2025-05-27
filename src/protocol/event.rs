use std::{
    collections::VecDeque,
    fmt::{Debug, Display, Error, Formatter},
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::clocks::clock::{Clock, Partial};

#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct Event<O> {
    pub op: O,
    /// An event can contain multiple metadata clocks if its a nested operation.
    /// The first level always exists.
    pub metadata: VecDeque<Clock<Partial>>,
}

impl<O> Event<O> {
    pub fn new_nested(op: O, metadata: VecDeque<Clock<Partial>>) -> Self {
        Self { op, metadata }
    }

    pub fn new(op: O, clock: Clock<Partial>) -> Self {
        let mut metadata = VecDeque::new();
        metadata.push_front(clock);
        Self { op, metadata }
    }

    /// Returns the first level dependency clock
    pub fn metadata(&self) -> &Clock<Partial> {
        &self.metadata[0]
    }
}

impl<O> Display for Event<O>
where
    O: Debug,
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "[")?;
        write!(f, "{:?}, {}", self.op, self.metadata())?;
        write!(f, "]")?;
        Ok(())
    }
}
