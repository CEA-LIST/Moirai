// #[cfg(feature = "test_utils")]
// use deepsize::DeepSizeOf;
use std::fmt::Debug;

#[cfg(feature = "sink")]
use crate::state::{object_path::ObjectPath, sink::SinkCollector, sink::SinkOwnership};
use crate::{
    HashMap,
    clock::version_vector::Version,
    crdt::{
        eval::{Eval, EvalNested},
        pure_crdt::PureCRDT,
        query::QueryOperation,
        redundancy::RedundancyRelation,
    },
    event::{Event, id::EventId, tagged_op::TaggedOp},
    state::{log::IsLog, stable_state::IsStableState, trace, unstable_state::IsUnstableState},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub type VecLog<O> = POLog<O, Vec<TaggedOp<O>>>;
pub type MapLog<O> = POLog<O, HashMap<EventId, TaggedOp<O>>>;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
// #[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct POLog<O, U>
where
    O: PureCRDT,
{
    pub(crate) stable: O::StableState,
    pub(crate) unstable: U,
}

impl<O, U> IsLog for POLog<O, U>
where
    O: PureCRDT + Clone,
    U: IsUnstableState<O> + Default + Debug,
{
    type Value = O::Value;
    type Op = O;

    fn new() -> Self {
        Self {
            stable: O::StableState::default(),
            unstable: U::default(),
        }
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        O::is_enabled(op, &self.stable, &self.unstable)
    }

    fn effect(
        &mut self,
        event: Event<Self::Op>,
        #[cfg(feature = "sink")] _path: ObjectPath,
        #[cfg(feature = "sink")] _sink: &mut SinkCollector,
        #[cfg(feature = "sink")] _ownership: SinkOwnership,
    ) {
        let new_tagged_op = TaggedOp::from(&event);
        if O::redundant_itself(&new_tagged_op, &self.stable, self.unstable.iter()) {
            trace::note_redundant_on_arrival();
            if !O::DISABLE_R_WHEN_R {
                self.prune_redundant_ops(
                    O::redundant_by_when_redundant,
                    &new_tagged_op,
                    event.version(),
                );
            }
        } else {
            if !O::DISABLE_R_WHEN_NOT_R {
                self.prune_redundant_ops(
                    O::redundant_by_when_not_redundant,
                    &new_tagged_op,
                    event.version(),
                );
            }
            self.unstable.append(event);
            trace::note_applied();
        }
    }

    fn stabilize(&mut self, version: &Version) {
        if O::DISABLE_STABILIZE {
            return;
        }
        // 1. select all ops in unstable that are predecessors of a version
        // 2. for each of them, call stabilize, which may modify stable and/or unstable
        // 3. if the operation is still in unstable, apply the op to stable and remove it from unstable

        let candidates = self.unstable.predecessors(version);

        for tagged_op in candidates {
            let tagged_op_key = self.unstable.key_of(&tagged_op);
            O::stabilize(&tagged_op, &mut self.stable, &mut self.unstable);
            if self.unstable.get_by_key(&tagged_op_key).is_some() {
                self.stable.apply(tagged_op.op().clone());
                self.unstable.remove_by_key(&tagged_op_key);
            }
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        // A parent resetting a child is update-wins being enforced: whatever is
        // causally below the removal goes, whatever is concurrent with it stays.
        // Recording which ids went is the difference between showing a viewer
        // "the key was removed" and showing them which edits it took with it.
        trace::note_reset();
        self.stable.clear();
        if conservative {
            self.unstable.retain(|tagged_op| {
                let dominated = tagged_op.id().is_predecessor_of(version);
                if dominated {
                    trace::note_superseded(tagged_op.id(), false);
                }
                !dominated
            })
        } else {
            if trace::is_enabled() {
                let removed: Vec<EventId> = self.unstable.iter().map(|t| t.id().clone()).collect();
                for id in &removed {
                    trace::note_superseded(id, false);
                }
            }
            self.unstable.clear();
        }
    }

    // TODO: we must decide if this function is semantic or structural, i.e. if it returns true if the log is semantically equivalent to the default value,
    // TODO: or if it returns true if the log is empty.
    fn is_default(&self) -> bool {
        self.stable.is_default() && self.unstable.is_empty()
    }
}

impl<O, U> Default for POLog<O, U>
where
    O: PureCRDT,
    U: Default,
{
    fn default() -> Self {
        Self {
            stable: Default::default(),
            unstable: Default::default(),
        }
    }
}

impl<O, U> POLog<O, U>
where
    O: PureCRDT,
    U: IsUnstableState<O>,
{
    fn prune_redundant_ops(
        &mut self,
        rdnt: RedundancyRelation<O>,
        new_tagged_op: &TaggedOp<O>,
        version: &Version,
    ) {
        self.stable.prune_redundant_ops(rdnt, new_tagged_op);
        self.unstable.retain(|old_tagged_op| {
            // Note: the new operation is not in the log at this point.
            let is_conc = !old_tagged_op.id().is_predecessor_of(version);

            let redundant = rdnt(
                old_tagged_op.op(),
                Some(old_tagged_op.tag()),
                is_conc,
                new_tagged_op,
            );
            if redundant {
                // The relation has already decided; this only writes down which
                // operation lost, and whether it lost to concurrency or to time.
                trace::note_superseded(old_tagged_op.id(), is_conc);
            }
            !redundant
        });
    }
}

impl<Q, O, U> EvalNested<Q> for POLog<O, U>
where
    Q: QueryOperation,
    O: PureCRDT + Clone + Debug + Eval<Q>,
    U: IsUnstableState<O> + Default + Debug,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        O::execute_query(q, &self.stable, &self.unstable)
    }
}
