use crate::clocks::vector_clock::VectorClock;

use super::{
    event::{Message, OpEvent},
    metadata::Metadata,
    tcsb::{POLog, RedundantRelation},
    utils::{Incrementable, Keyable},
};
use std::fmt::Debug;
/// An op-based CRDT is pure if disseminated mes- sages contain only the operation and its potential arguments.
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
            Self::prune_redundant_events(&event, state, Self::r_zero);
        } else {
            // The operation is not redundant
            Self::prune_redundant_events(&event, state, Self::r_one);
            state.1.insert(event.metadata, Message::Op(event.op));
        }
    }

    fn prune_redundant_events<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        event: &OpEvent<K, C, Self>,
        state: &mut POLog<K, C, Self>,
        r_relation: RedundantRelation<K, C, Self>,
    ) {
        // Keep only the operations that are not made redundant by the new operation
        state.0.retain(|o| {
            let old_event: OpEvent<K, C, Self> = OpEvent::new(o.clone(), Metadata::default());
            !(r_relation(&old_event, event))
        });
        state.1.retain(|m, o| {
            if let Message::Op(op) = o {
                let old_event: OpEvent<K, C, Self> = OpEvent::new(op.clone(), m.clone());
                return !(r_relation(&old_event, event));
            } // TODO: handle the case of a protocol event
            true
        });
    }

    /// The `stable` handler invokes `stabilize` and then strips
    /// the timestamp (if the operation has not been discarded by `stabilize`),
    /// by replacing a (t′, o′) pair that is present in the returned PO-Log by (⊥,o′)
    fn stable<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        metadata: &Metadata<K, C>,
        state: &mut POLog<K, C, Self>,
    ) {
        Self::stabilize(&metadata.vc, state);
        if let Some(Message::Op(op)) = state.1.get(metadata) {
            state.0.push(op.clone());
            state.1.remove(metadata);
        } // TODO: handle the case of a protocol event
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
        vc: &VectorClock<K, C>,
        state: &mut POLog<K, C, Self>,
    );

    /// `eval` takes the query and the state as input and returns a result, leaving the state unchanged.
    /// Note: only supports the `read` query for now.
    fn eval<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        state: &POLog<K, C, Self>,
    ) -> Self::Value;
}
