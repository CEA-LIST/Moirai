use std::{cmp::Ordering, fmt::Debug};

use super::event_graph::EventGraph;
use crate::clocks::dependency_clock::DependencyClock;

/// An op-based CRDT is pure if disseminated messages contain only the operation and its potential arguments.
pub trait PureCRDT: Clone + Debug {
    type Value: Debug + Default;

    /// Does `r_zero` always return the same boolean value?
    const R_ZERO: Option<bool> = None;
    /// Does `r_one` always return the same boolean value?
    const R_ONE: Option<bool> = None;

    /// Datatype-specific relation used to define causal redundancy.
    /// R relation defines whether the delivered operation is itself
    /// redundant and does not need to be added itself to the PO-Log.
    /// `true` means the operation is redundant and can be discarded immediately.
    fn redundant_itself(new_op: &Self) -> bool;

    /// Datatype-specific relation used to define causal redundancy.
    /// R0 defines which operations in the current PO-Log become redundant
    /// given the delivery of the new operation.
    /// R0 is used when the new arrival is discarded being redundant.
    /// `true` means the operation is redundant and can be discarded immediately.`
    fn redundant_by_when_redundant(old_op: &Self, order: Option<Ordering>, new_op: &Self) -> bool;

    /// Datatype-specific relation used to define causal redundancy.
    /// R1 defines which operations in the current PO-Log become redundant
    /// given the delivery of the new operation.
    /// R1 is used when the new arrivals added to the PO-Log.
    /// `true` means the operation is redundant and can be discarded immediately.
    fn redundant_by_when_not_redundant(
        old_op: &Self,
        order: Option<Ordering>,
        new_op: &Self,
    ) -> bool;

    /// `stabilize` takes a stable timestamp `t` (fed by the TCSB middleware) and
    /// the full PO-Log `s` as input, and returns a new PO-Log (i.e., a map),
    /// possibly discarding a set of operations at once.
    fn stabilize(metadata: &DependencyClock, state: &mut EventGraph<Self>);

    /// `eval` takes the query and the state as input and returns a result, leaving the state unchanged.
    /// Note: only supports the `read` query for now.
    fn eval(ops: &[Self]) -> Self::Value;
}
