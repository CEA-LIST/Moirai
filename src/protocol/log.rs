use std::{collections::VecDeque, fmt::Debug, rc::Rc};

use super::{event::Event, membership::ViewData, pulling::Since};
use crate::clocks::{
    clock::{Clock, Full, Partial},
    dot::Dot,
};

pub trait Log: Default + Debug {
    type Op: Debug + Clone;
    type Value: Debug;

    fn new() -> Self;

    /// `prepare` cannot inspect the state, being limited to returning the operation (including potential parameters)
    fn prepare(&self, op: Self::Op) -> Self::Op {
        op
    }

    fn new_event(&mut self, event: &Event<Self::Op>);

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool);

    fn collect_events(
        &self,
        upper_bound: &Clock<Full>,
        lower_bound: &Clock<Full>,
    ) -> Vec<Event<Self::Op>>;

    fn collect_events_since(&self, since: &Since) -> Vec<Event<Self::Op>>;

    fn any_r(&self, event: &Event<Self::Op>) -> bool;

    /// Remove every stable operations and unstable that are:
    /// - less or equal to the metadata if conservative is true
    /// - less, equal or concurrent to the metadata if conservative is false
    fn r_n(&mut self, metadata: &Clock<Full>, conservative: bool);

    /// `eval` takes the query and the state as input and returns a result, leaving the state unchanged.
    /// Note: only supports the `read` query for now.
    fn eval(&self) -> Self::Value;

    /// `stabilize` takes a stable timestamp `t` (fed by the TCSB middleware) and
    /// the full PO-Log `s` as input, and returns a new PO-Log (i.e., a map),
    /// possibly discarding a set of operations at once.
    fn stabilize(&mut self, metadata: &Clock<Partial>);

    /// Apply the effect of an operation to the local state.
    /// Check if the operation is causally redundant and update the PO-Log accordingly.
    /// The event is added to the PO-Log during "prune redundant events".
    fn effect(&mut self, event: Event<Self::Op>) {
        self.new_event(&event);
        if self.any_r(&event) {
            // The operation is redundant
            self.prune_redundant_events(&event, true);
        } else {
            // The operation is not redundant
            self.prune_redundant_events(&event, false);
        }
    }

    fn purge_stable_metadata(&mut self, metadata: &Clock<Partial>);

    /// The `stable` handler invokes `stabilize` and then strips
    /// the timestamp (if the operation has not been discarded by `stabilize`),
    /// by replacing a (t′, o′) pair that is present in the returned PO-Log by (⊥,o′)
    fn stable(&mut self, metadata: &Clock<Partial>) {
        self.stabilize(metadata);
        // The operation may have been removed by `stabilize`
        self.purge_stable_metadata(metadata);
    }

    fn deps(
        &self,
        clocks: &mut VecDeque<Clock<Partial>>,
        view: &Rc<ViewData>,
        dot: &Dot,
        op: &Self::Op,
    );

    fn is_empty(&self) -> bool;

    fn size(&self) -> usize;
}
