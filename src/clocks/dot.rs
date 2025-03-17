use std::{
    cmp::Ordering,
    fmt::{Display, Error, Formatter},
    rc::Rc,
};

use serde::{Deserialize, Serialize};

use crate::protocol::membership::View;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Dot {
    view: Rc<View>,
    origin: usize,
    counter: usize,
}

impl Dot {
    pub fn new(origin: usize, counter: usize, view: &Rc<View>) -> Self {
        Self {
            view: Rc::clone(view),
            origin,
            counter,
        }
    }

    pub fn view(&self) -> Rc<View> {
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
        write!(f, "({}{})", self.origin, self.counter)
    }
}

impl PartialOrd for Dot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.view.id != other.view.id {
            None
        } else {
            if self.origin == other.origin {
                Some(self.counter.cmp(&other.counter))
            } else {
                None
            }
        }
    }
}

// TODO: Dumb shit
// impl Ord for Dot {
//     fn cmp(&self, other: &Self) -> Ordering {
//         match self.view.id.cmp(&other.view.id) {
//             Ordering::Less => Ordering::Less,
//             Ordering::Equal => match self.origin.cmp(&other.origin) {
//                 Ordering::Less => Ordering::Less,
//                 Ordering::Equal => self.counter.cmp(&other.counter),
//                 Ordering::Greater => Ordering::Greater,
//             },
//             Ordering::Greater => Ordering::Greater,
//         }
//     }
// }
