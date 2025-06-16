use crate::{clocks::dot::Dot};

use super::pure_crdt::PureCRDT;
use std::fmt::Debug;

pub trait Stable<O>: Default + Clone + Debug {
    fn is_default(&self) -> bool;

    fn clear(&mut self) {
        *self = Self::default();
    }

    fn apply_redundant(&mut self, rdnt: fn(&O, Option<&Dot>, bool, &O, &Dot) -> bool, op: &O, dot: &Dot);

    fn apply(&mut self, value: O);
}

impl<O: PureCRDT> Stable<O> for Vec<O> {
    fn is_default(&self) -> bool {
        self.is_empty()
    }

    fn clear(&mut self) {
        Vec::clear(self);
    }

    fn apply(&mut self, value: O) {
        self.push(value);
    }

    fn apply_redundant(&mut self, rdnt: fn(&O, Option<&Dot>, bool, &O, &Dot) -> bool, op: &O, dot: &Dot) {
        self.retain(|o| !(rdnt(o, None, false, op, dot)));
    }
}
