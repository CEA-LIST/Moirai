use std::fmt::Debug;

use camino::{Utf8Path, Utf8PathBuf};

use super::tcsb::RedundantRelation;
use super::{event::Event, metadata::Metadata, po_log::POLog, utils::prune_redundant_events};

/// An op-based CRDT is pure if disseminated messages contain only the operation and its potential arguments.
pub trait PureCRDT: Sized + Clone + Debug {
    type Value: Default + Debug;

    /// `prepare` cannot inspect the state, being limited to returning the operation (including potential parameters)
    fn prepare(op: Self, _state: POLog<Self>) -> Self {
        op
    }

    /// Apply the effect of an operation to the local state.
    /// Check if the operation is causally redundant and update the PO-Log accordingly.
    fn effect(event: &Event<Self>, state: &POLog<Self>) -> (bool, Vec<usize>, Vec<Metadata>) {
        let (keep, prune_fn): (bool, RedundantRelation<Self>) = if Self::r(event, state) {
            (false, Self::r_zero)
        } else {
            (true, Self::r_one)
        };

        let (remove_stable_by_index, remove_unstable_by_key) =
            prune_redundant_events(event, state, prune_fn);

        (keep, remove_stable_by_index, remove_unstable_by_key)
    }

    /// The `stable` handler invokes `stabilize` and then strips
    /// the timestamp (if the operation has not been discarded by `stabilize`),
    /// by replacing a (t′, o′) pair that is present in the returned PO-Log by (⊥,o′)
    fn stable(metadata: &Metadata, state: &mut POLog<Self>) {
        Self::stabilize(metadata, state);
        if let Some(n) = state.unstable.get(metadata) {
            state.stable.push(n.clone());
            state.unstable.remove(metadata);
        }
    }

    /// Datatype-specific relation used to define causal redundancy.
    /// R relation defines whether the delivered operation is itself
    /// redundant and does not need to be added itself to the PO-Log.
    fn r(event: &Event<Self>, state: &POLog<Self>) -> bool;

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
    fn eval(state: &POLog<Self>, path: &Utf8Path) -> Self::Value;

    fn to_path(_op: &Self) -> Utf8PathBuf {
        Utf8PathBuf::default()
    }
}
