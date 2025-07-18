use std::{
    collections::VecDeque,
    fmt::{Debug, Display, Error, Formatter},
};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::clocks::{
    clock::{Clock, Lamport, Partial},
    dot::Dot,
};

/// An event is pair (op, timestamp)
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
    pub lamport: Lamport,
}

impl<O> Event<O> {
    pub fn new_nested(op: O, metadata: VecDeque<Clock<Partial>>, lamport: Lamport) -> Self {
        Self {
            op,
            metadata,
            lamport,
        }
    }

    pub fn new(op: O, clock: Clock<Partial>, lamport: Lamport) -> Self {
        let mut metadata = VecDeque::new();
        metadata.push_front(clock);
        Self {
            op,
            metadata,
            lamport,
        }
    }

    /// Returns the first level dependency clock
    pub fn metadata(&self) -> &Clock<Partial> {
        &self.metadata[0]
    }

    /// Returns the first level dependency clock
    pub fn metadata_mut(&mut self) -> &mut Clock<Partial> {
        &mut self.metadata[0]
    }

    pub fn lamport(&self) -> Lamport {
        self.lamport
    }

    pub fn origin(&self) -> &str {
        self.metadata().origin()
    }
}

impl<O> From<&Event<O>> for Dot {
    fn from(event: &Event<O>) -> Self {
        Dot::new(
            event
                .metadata()
                .origin
                .expect("Event metadata should have an origin"),
            event.metadata().dot_val(),
            event.lamport(),
            &event.metadata().view,
        )
    }
}

impl<O> Display for Event<O>
where
    O: Debug,
{
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        // TODO: improve the nesting display
        write!(f, "[")?;
        write!(f, "{:?},", self.op)?;
        for (i, c) in self.metadata.iter().enumerate() {
            if i + 1 == self.metadata.len() {
                write!(f, "{c}")?;
            } else {
                write!(f, "{c} -> ")?;
            }
        }
        write!(f, "]")?;
        Ok(())
    }
}
