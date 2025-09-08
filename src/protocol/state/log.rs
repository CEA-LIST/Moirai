use std::fmt::Debug;

#[cfg(test)]
use crate::protocol::state::{stable_state::IsStableState, unstable_state::IsUnstableState};
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
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
}

#[cfg(test)]
pub trait IsLogTest: IsLog {
    fn stable(&self) -> &impl IsStableState<Self::Op>;
    fn unstable(&self) -> &impl IsUnstableState<Self::Op>;
}
