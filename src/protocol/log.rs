use super::pure_crdt::PureCRDT;
use super::{event::Event, metadata::Metadata, po_log::POLog};
use std::fmt::Debug;
use std::ops::Bound;

pub trait Log: Default + Clone {
    type Op: Debug + Clone;
    type Value;

    /// `prepare` cannot inspect the state, being limited to returning the operation (including potential parameters)
    fn prepare(&self, op: Self::Op) -> Self::Op {
        op
    }

    fn new_event(&mut self, event: &Event<Self::Op>);

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool);

    fn collect_events(&self, upper_bound: &Metadata) -> Vec<Event<Self::Op>>;

    fn any_r(&self, event: &Event<Self::Op>) -> bool;

    fn r_n(&mut self, metadata: &Metadata, conservative: bool);

    /// `eval` takes the query and the state as input and returns a result, leaving the state unchanged.
    /// Note: only supports the `read` query for now.
    fn eval(&self) -> Self::Value;

    /// `stabilize` takes a stable timestamp `t` (fed by the TCSB middleware) and
    /// the full PO-Log `s` as input, and returns a new PO-Log (i.e., a map),
    /// possibly discarding a set of operations at once.
    fn stabilize(&mut self, metadata: &Metadata);

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

    fn purge_stable_metadata(&mut self, metadata: &Metadata);

    /// The `stable` handler invokes `stabilize` and then strips
    /// the timestamp (if the operation has not been discarded by `stabilize`),
    /// by replacing a (t′, o′) pair that is present in the returned PO-Log by (⊥,o′)
    fn stable(&mut self, metadata: &Metadata) {
        self.stabilize(metadata);
        // The operation may have been removed by `stabilize`
        self.purge_stable_metadata(metadata);
    }

    fn is_empty(&self) -> bool;
}

impl<O> Log for POLog<O>
where
    O: PureCRDT,
{
    type Op = O;
    type Value = O::Value;

    fn new_event(&mut self, event: &Event<Self::Op>) {
        self.new_event(event);
    }

    fn prune_redundant_events(&mut self, event: &Event<Self::Op>, is_r_0: bool) {
        // Keep only the operations that are not made redundant by the new operation
        self.stable.retain(|o| {
            let old_event: Event<O> = Event::new(o.as_ref().clone(), Metadata::default());
            if is_r_0 {
                !(Self::Op::r_zero(&old_event, event))
            } else {
                !(Self::Op::r_one(&old_event, event))
            }
        });
        self.unstable.retain(|m, o| {
            let old_event: Event<O> = Event::new(o.as_ref().clone(), m.clone());
            if is_r_0 {
                !(Self::Op::r_zero(&old_event, event))
            } else {
                !(Self::Op::r_one(&old_event, event))
            }
        });
    }

    fn purge_stable_metadata(&mut self, metadata: &Metadata) {
        if let Some(n) = self.unstable.get(metadata) {
            self.stable.push(n.clone());
            self.unstable.remove(metadata);
        }
    }

    /// Returns a list of operations that are ready to be stabilized.
    fn collect_events(&self, upper_bound: &Metadata) -> Vec<Event<Self::Op>> {
        let list = self
            .unstable
            .range((Bound::Unbounded, Bound::Included(upper_bound)))
            .filter_map(|(m, o)| {
                if m.clock <= upper_bound.clock {
                    Some(Event::new(o.as_ref().clone(), m.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<Event<Self::Op>>>();
        list
    }

    fn any_r(&self, event: &Event<Self::Op>) -> bool {
        for o in &self.stable {
            let old_event = Event::new(o.as_ref().clone(), Metadata::default());
            if O::r(event, &old_event) {
                return true;
            }
        }
        for (m, o) in self.unstable.iter() {
            let old_event = Event::new(o.as_ref().clone(), m.clone());
            if O::r(event, &old_event) {
                return true;
            }
        }
        false
    }

    /// conservative: keep concurrent operations
    fn r_n(&mut self, metadata: &Metadata, conservative: bool) {
        self.stable.clear();
        self.unstable.retain(|m, _| {
            if conservative {
                !(m.clock < metadata.clock)
            } else {
                m.clock > metadata.clock
            }
        });
    }

    fn stabilize(&mut self, metadata: &Metadata) {
        O::stabilize(metadata, self);
    }

    fn eval(&self) -> Self::Value {
        let ops: Vec<O> = self.iter().map(|o| o.as_ref().clone()).collect::<Vec<O>>();
        O::eval(&ops)
    }

    fn is_empty(&self) -> bool {
        self.stable.is_empty() && self.unstable.is_empty()
    }
}
