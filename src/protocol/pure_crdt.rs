use super::{event::Event, metadata::Metadata, po_log::POLog};
use std::fmt::Debug;

/// An op-based CRDT is pure if disseminated messages contain only the operation and its potential arguments.
pub trait PureCRDT: Clone + Debug {
    type Value;

    /// Datatype-specific relation used to define causal redundancy.
    /// R relation defines whether the delivered operation is itself
    /// redundant and does not need to be added itself to the PO-Log.
    fn r(new_event: &Event<Self>, old_event: &Event<Self>) -> bool;

    /// Datatype-specific relation used to define causal redundancy.
    /// R0 defines which operations in the current PO-Log become redundant
    /// given the delivery of the new operation.
    /// R0 is used when the new arrival is discarded being redundant.
    fn r_zero(old_event: &Event<Self>, new_event: &Event<Self>) -> bool;

    /// Datatype-specific relation used to define causal redundancy.
    /// R1 defines which operations in the current PO-Log become redundant
    /// given the delivery of the new operation.
    /// R1 is used when the new arrivalis added to the PO-Log.
    fn r_one(old_event: &Event<Self>, new_event: &Event<Self>) -> bool;

    /// `stabilize` takes a stable timestamp `t` (fed by the TCSB middleware) and
    /// the full PO-Log `s` as input, and returns a new PO-Log (i.e., a map),
    /// possibly discarding a set of operations at once.
    fn stabilize(metadata: &Metadata, state: &mut POLog<Self>);

    /// `eval` takes the query and the state as input and returns a result, leaving the state unchanged.
    /// Note: only supports the `read` query for now.
    fn eval(ops: &[Self]) -> Self::Value;
}
