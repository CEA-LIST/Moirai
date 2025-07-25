use std::{
    cmp::Ordering,
    fmt::{Display, Error, Formatter},
    hash::Hash,
    rc::Rc,
};

#[cfg(feature = "utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::{
    clocks::clock::{Clock, Lamport, Partial},
    protocol::membership::ViewData,
};

/// A dot is a pair (id, counter) that uniquely identifies an operation.
#[derive(Debug, Clone, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
#[cfg_attr(feature = "utils", derive(DeepSizeOf))]
pub struct Dot {
    view: Rc<ViewData>,
    origin: usize,
    counter: usize,
    /// Lamport timestamp (just the sum of the vector clock entries)
    lamport: Lamport,
}

impl Dot {
    pub fn new(origin: usize, counter: usize, lamport: Lamport, view: &Rc<ViewData>) -> Self {
        Self {
            view: Rc::clone(view),
            origin,
            counter,
            lamport,
        }
    }

    pub fn view(&self) -> Rc<ViewData> {
        Rc::clone(&self.view)
    }

    /// Compute in O(1)
    pub fn origin(&self) -> &str {
        &self.view.members[self.origin]
    }

    pub fn origin_idx(&self) -> usize {
        self.origin
    }

    pub fn val(&self) -> usize {
        self.counter
    }

    pub fn lamport(&self) -> Lamport {
        self.lamport
    }
}

impl Display for Dot {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "({}{})", self.origin(), self.counter)
    }
}

impl From<&Dot> for Clock<Partial> {
    fn from(dot: &Dot) -> Clock<Partial> {
        let mut clock = Clock::<Partial>::new(&Rc::clone(&dot.view), dot.origin());
        clock.set_by_idx(dot.origin, dot.counter);
        clock
    }
}

impl PartialOrd for Dot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.view.id != other.view.id {
            None
        } else if self.origin == other.origin {
            Some(self.counter.cmp(&other.counter))
        } else {
            None
        }
    }
}

impl PartialEq for Dot {
    fn eq(&self, other: &Self) -> bool {
        self.view.id == other.view.id
            && self.origin == other.origin
            && self.counter == other.counter
    }
}

impl Hash for Dot {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.view.id.hash(state);
        self.origin.hash(state);
        self.counter.hash(state);
    }
}
