use std::fmt::Debug;

#[cfg(feature = "fuzz")]
use rand::RngCore;

#[cfg(feature = "fuzz")]
use crate::fuzz::config::OpConfig;
#[cfg(feature = "test_utils")]
use crate::protocol::state::{stable_state::IsStableState, unstable_state::IsUnstableState};
use crate::protocol::{
    clock::version_vector::Version,
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
};

/// Define the interface of a log structure for CRDTs that store events.
pub trait IsLog: Default + Debug {
    type Value: Default + Debug;
    type Op: Debug + Clone;

    fn new() -> Self;
    /// `prepare` cannot inspect the state, being limited to returning the operation (including potential parameters).
    fn prepare(op: Self::Op) -> Self::Op {
        op
    }
    fn is_enabled(&self, op: &Self::Op) -> bool;
    fn effect(&mut self, event: Event<Self::Op>);
    fn eval<Q>(&self, q: Q) -> Q::Response
    where
        Q: QueryOperation,
        Self: EvalNested<Q>,
    {
        Self::execute_query(self, q)
    }
    fn stabilize(&mut self, version: &Version);
    fn redundant_by_parent(&mut self, version: &Version, conservative: bool);
    fn is_default(&self) -> bool;
}

#[cfg(feature = "test_utils")]
pub trait IsLogTest: IsLog {
    fn stable(&self) -> &impl IsStableState<Self::Op>;
    fn unstable(&self) -> &impl IsUnstableState<Self::Op>;
}

#[cfg(feature = "fuzz")]
pub trait IsLogFuzz: IsLog {
    fn generate_op(&self, rng: &mut impl RngCore, config: &OpConfig) -> Self::Op;
}
