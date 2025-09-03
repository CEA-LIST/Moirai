use std::fmt::Debug;

use crate::protocol::{clock::version_vector::Version, event::Event};

/// Define the interface of a log structure for CRDTs that store events.
pub trait IsLog: Default + Debug {
    type Op: Debug + Clone;
    type Value: Debug;

    fn new() -> Self;
    /// `prepare` cannot inspect the state, being limited to returning the operation (including potential parameters).
    fn prepare(op: Self::Op) -> Self::Op {
        op
    }
    fn effect(&mut self, event: Event<Self::Op>);
    fn eval(&self) -> Self::Value;
    fn stabilize(&mut self, version: &Version);
    fn redundant_by_parent(&mut self, version: &Version, conservative: bool);
}
