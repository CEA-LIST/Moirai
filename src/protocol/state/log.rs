use std::fmt::Debug;

use crate::protocol::{
    clock::version_vector::Version,
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
};
#[cfg(feature = "test_utils")]
use crate::protocol::{
    crdt::pure_crdt::PureCRDT,
    state::{stable_state::IsStableState, unstable_state::IsUnstableState},
};

/// Define the interface of a log structure for CRDTs that store events.
pub trait IsLog: Default + Debug {
    // TODO: is Value really needed?
    type Value: Default + Debug;
    type Op: Debug + Clone;

    fn new() -> Self;
    /// `prepare` cannot inspect the state, being limited to returning the operation (including potential parameters).
    fn prepare(op: Self::Op) -> Self::Op {
        op
    }
    // TODO replace by Result
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
pub trait IsLogTest
where
    Self: IsLog,
    Self::Op: PureCRDT,
    <Self::Op as PureCRDT>::StableState: IsStableState<Self::Op>,
{
    fn stable(&self) -> &<Self::Op as PureCRDT>::StableState;
    fn unstable(&self) -> &impl IsUnstableState<Self::Op>;
}
