use super::{
    event::{Message, OpEvent},
    metadata::Metadata,
    tcsb::POLog,
    utils::{prune_redundant_events, Incrementable, Keyable},
};
use std::fmt::Debug;

/// An op-based CRDT is pure if disseminated messages contain only the operation and its potential arguments.
pub trait PureCRDT: Clone + Debug {
    type Value: Clone + Debug + Default;

    /// `prepare` cannot inspect the state, being limited to returning the operation (including potential parameters)
    fn prepare<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        op: Self,
        _state: POLog<K, C, Self>,
    ) -> Self {
        op
    }

    /// Apply the effect of an operation to the local state.
    /// Check if the operation is causally redundant and update the PO-Log accordingly.
    fn effect<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        event: OpEvent<K, C, Self>,
        state: &mut POLog<K, C, Self>,
    ) {
        if Self::r(&event, state) {
            // The operation is redundant
            prune_redundant_events(&event, state, Self::r_zero);
        } else {
            // The operation is not redundant
            prune_redundant_events(&event, state, Self::r_one);
            state.1.insert(event.metadata, Message::Op(event.op));
        }
    }

    /// The `stable` handler invokes `stabilize` and then strips
    /// the timestamp (if the operation has not been discarded by `stabilize`),
    /// by replacing a (t′, o′) pair that is present in the returned PO-Log by (⊥,o′)
    fn stable<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        metadata: &Metadata<K, C>,
        state: &mut POLog<K, C, Self>,
    ) {
        Self::stabilize(metadata, state);
        if let Some(Message::Op(op)) = state.1.get(metadata) {
            state.0.push(op.clone());
            state.1.remove(metadata);
        }
    }

    /// Datatype-specific relation used to define causal redundancy.
    /// R relation defines whether the delivered operation is itself
    /// redundant and does not need to be added itself to the PO-Log.
    fn r<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        event: &OpEvent<K, C, Self>,
        state: &POLog<K, C, Self>,
    ) -> bool;

    /// Datatype-specific relation used to define causal redundancy.
    /// R0 defines which operations in the current PO-Log become redundant
    /// given the delivery of the new operation.
    /// R0 is used when the new arrival is discarded being redundant.
    fn r_zero<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        old_event: &OpEvent<K, C, Self>,
        new_event: &OpEvent<K, C, Self>,
    ) -> bool;

    /// Datatype-specific relation used to define causal redundancy.
    /// R1 defines which operations in the current PO-Log become redundant
    /// given the delivery of the new operation.
    /// R1 is used when the new arrivalis added to the PO-Log.
    fn r_one<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        old_event: &OpEvent<K, C, Self>,
        new_event: &OpEvent<K, C, Self>,
    ) -> bool;

    /// `stabilize` takes a stable timestamp `t` (fed by the TCSB middleware) and
    /// the full PO-Log `s` as input, and returns a new PO-Log (i.e., a map),
    /// possibly discarding a set of operations at once.
    fn stabilize<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        metadata: &Metadata<K, C>,
        state: &mut POLog<K, C, Self>,
    );

    /// `eval` takes the query and the state as input and returns a result, leaving the state unchanged.
    /// Note: only supports the `read` query for now.
    fn eval<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        state: &POLog<K, C, Self>,
    ) -> Self::Value;
}
