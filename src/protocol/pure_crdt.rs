use std::fmt::Debug;

use crate::clocks::dot::Dot;

use super::{event_graph::EventGraph, stable::Stable};

/// An op-based CRDT is pure if disseminated messages contain only the operation and its potential arguments.
pub trait PureCRDT: Clone + Debug {
    type Value: Debug + Default;
    type Stable: Stable<Self>;

    /// Does `redundant_by_when_redundant` always return `false`?
    const DISABLE_R_WHEN_R: bool = false;
    /// Does `redundant_by_when_not_redundant` always return `false`?
    const DISABLE_R_WHEN_NOT_R: bool = false;

    /// Datatype-specific relation used to define causal redundancy.
    /// R relation defines whether the delivered operation is itself
    /// redundant and does not need to be added itself to the PO-Log.
    /// `true` means the operation is redundant and can be discarded immediately.
    fn redundant_itself(_new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        false
    }

    /// Datatype-specific relation used to define causal redundancy.
    /// R0 defines which operations in the current PO-Log become redundant
    /// given the delivery of the new operation.
    /// R0 is used when the new arrival is discarded being redundant.
    /// `true` means the operation is redundant and can be discarded immediately.
    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_dot: Option<&Dot>,
        _is_conc: bool,
        _new_op: &Self,
        _new_dot: &Dot,
    ) -> bool {
        false
    }

    /// Datatype-specific relation used to define causal redundancy.
    /// R1 defines which operations in the current PO-Log become redundant
    /// given the delivery of the new operation.
    /// R1 is used when the new arrivals added to the PO-Log.
    /// `true` means the operation is redundant and can be discarded immediately.
    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        _old_dot: Option<&Dot>,
        _is_conc: bool,
        _new_op: &Self,
        _new_dot: &Dot,
    ) -> bool {
        false
    }

    /// `stabilize` takes a stable timestamp `t` (fed by the TCSB middleware) and
    /// the full PO-Log `s` as input, and returns a new PO-Log (i.e., a map),
    /// possibly discarding a set of operations at once.
    fn stabilize(_dot: &Dot, _state: &mut EventGraph<Self>) {}

    /// `eval` takes the query and the state as input and returns a result, leaving the state unchanged.
    /// Note: only supports the `read` query for now.
    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value;
}
