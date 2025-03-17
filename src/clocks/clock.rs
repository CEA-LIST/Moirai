use std::{
    fmt::{Debug, Display},
    rc::Rc,
};

use crate::protocol::membership::View;

pub trait Clock: PartialOrd + Debug + Display + Clone + Eq + PartialEq {
    fn new(members: &Rc<View>, origin: &str) -> Self;

    fn merge(&mut self, other: &Self);

    fn increment(&mut self);

    fn min(&self, other: &Self) -> Self;

    fn remove(&mut self, member: &str);

    /// Returns the dimension of the clock
    /// The dimension is distinct from the number of members in the system
    fn dim(&self) -> usize;

    fn get(&self, member: &str) -> usize;

    fn set(&mut self, member: &str, value: usize);

    fn origin(&self) -> &str;
}
