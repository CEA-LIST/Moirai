use std::fmt::Debug;

use super::pure_crdt::PureCRDT;
use crate::clocks::dot::Dot;

/// Define the interface of a stable storage structure for CRDTs.
/// A stable storage structure is a specialized data structure used to store efficiently
/// the operations of a CRDT that are causally stable. It usually require only O(1) time complexity to apply an operation to the stable storage, whereas the unstable storage
/// requires O(e) time complexity to apply an operation (e being the causal predecessors of that operation).
pub trait Stable<O>: Default + Clone + Debug {
    fn is_default(&self) -> bool;

    fn clear(&mut self) {
        *self = Self::default();
    }

    fn apply_redundant(
        &mut self,
        rdnt: fn(&O, Option<&Dot>, bool, &O, &Dot) -> bool,
        op: &O,
        dot: &Dot,
    );

    fn apply(&mut self, value: O);
}

/// The default implementation of the Stable trait is just a vector.
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

    fn apply_redundant(
        &mut self,
        rdnt: fn(&O, Option<&Dot>, bool, &O, &Dot) -> bool,
        op: &O,
        dot: &Dot,
    ) {
        self.retain(|o| !(rdnt(o, None, false, op, dot)));
    }
}
