use std::{
    cmp::Ordering,
    fmt::{Display, Error, Formatter},
    rc::Rc,
};

use serde::{Deserialize, Serialize};

use crate::protocol::membership::ViewData;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Dot {
    view: Rc<ViewData>,
    origin: usize,
    counter: usize,
}

impl Dot {
    pub fn new(origin: usize, counter: usize, view: &Rc<ViewData>) -> Self {
        Self {
            view: Rc::clone(view),
            origin,
            counter,
        }
    }

    pub fn view(&self) -> Rc<ViewData> {
        Rc::clone(&self.view)
    }

    pub fn origin(&self) -> &str {
        &self.view.members[self.origin]
    }

    pub fn val(&self) -> usize {
        self.counter
    }
}

impl Display for Dot {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "({}{})", self.origin(), self.counter)
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
