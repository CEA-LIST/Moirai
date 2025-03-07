use crate::clocks::dependency_clock::DependencyClock;

use super::{event::Event, pulling::Since};
use std::fmt::Debug;

pub trait Log: Default + Clone {
    type Op: Debug + Clone;
    type Value;

    /// `prepare` cannot inspect the state, being limited to returning the operation (including potential parameters)
    fn prepare(&self, op: Self::Op) -> Self::Op {
        op
    }

    fn new_event(&mut self, event: &Event<Self::Op>);

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool);

    fn collect_events(&self, upper_bound: &DependencyClock) -> Vec<Event<Self::Op>>;

    fn collect_events_since(&self, since: &Since) -> Vec<Event<Self::Op>>;

    fn any_r(&self, event: &Event<Self::Op>) -> bool;

    fn r_n(&mut self, metadata: &DependencyClock, conservative: bool);

    /// `eval` takes the query and the state as input and returns a result, leaving the state unchanged.
    /// Note: only supports the `read` query for now.
    fn eval(&self) -> Self::Value;

    /// `stabilize` takes a stable timestamp `t` (fed by the TCSB middleware) and
    /// the full PO-Log `s` as input, and returns a new PO-Log (i.e., a map),
    /// possibly discarding a set of operations at once.
    fn stabilize(&mut self, metadata: &DependencyClock);

    /// Apply the effect of an operation to the local state.
    /// Check if the operation is causally redundant and update the PO-Log accordingly.
    fn effect(&mut self, event: Event<Self::Op>) {
        if self.any_r(&event) {
            // The operation is redundant
            self.prune_redundant_events(&event, true);
        } else {
            // The operation is not redundant
            self.prune_redundant_events(&event, false);
            self.new_event(&event);
        }
    }

    fn purge_stable_metadata(&mut self, metadata: &DependencyClock);

    /// The `stable` handler invokes `stabilize` and then strips
    /// the timestamp (if the operation has not been discarded by `stabilize`),
    /// by replacing a (t′, o′) pair that is present in the returned PO-Log by (⊥,o′)
    fn stable(&mut self, metadata: &DependencyClock) {
        self.stabilize(metadata);
        // The operation may have been removed by `stabilize`
        self.purge_stable_metadata(metadata);
    }

    fn is_empty(&self) -> bool;

    /// Return the lowest view ID in the log
    fn lowest_view_id(&self) -> usize;
}
