use std::fmt::Debug;

#[cfg(feature = "fuzz")]
use rand::RngCore;

#[cfg(feature = "fuzz")]
use crate::fuzz::config::{OpConfig, OpGenerator};
#[cfg(feature = "fuzz")]
use crate::protocol::state::log::IsLogFuzz;
#[cfg(feature = "test_utils")]
use crate::protocol::state::log::IsLogTest;
use crate::{
    protocol::{
        clock::version_vector::Version,
        crdt::{
            eval::{Eval, EvalNested},
            pure_crdt::PureCRDT,
            query::QueryOperation,
            redundancy::RedundancyRelation,
        },
        event::{id::EventId, tagged_op::TaggedOp, Event},
        state::{log::IsLog, stable_state::IsStableState, unstable_state::IsUnstableState},
    },
    HashMap,
};

pub type VecLog<O> = POLog<O, Vec<TaggedOp<O>>>;
pub type MapLog<O> = POLog<O, HashMap<EventId, TaggedOp<O>>>;

#[derive(Debug, Clone)]
pub struct POLog<O, U>
where
    O: PureCRDT,
{
    stable: O::StableState,
    unstable: U,
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

    fn effect(&mut self, event: Event<Self::Op>) {
        let new_tagged_op = TaggedOp::from(&event);
        if O::redundant_itself(&new_tagged_op, &self.stable, self.unstable.iter()) {
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
            O::stabilize(&tagged_op, &mut self.stable, &mut self.unstable);
            if self.unstable.get(tagged_op.id()).is_some() {
                self.stable.apply(tagged_op.op().clone());
                self.unstable.remove(tagged_op.id());
            }
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.stable.clear();
        if conservative {
            self.unstable
                .retain(|tagged_op| !tagged_op.id().is_predecessor_of(version))
        } else {
            self.unstable.clear();
        }
    }

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
            let boo = !rdnt(
                old_tagged_op.op(),
                Some(old_tagged_op.tag()),
                is_conc,
                new_tagged_op,
            );
            boo
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

#[cfg(feature = "test_utils")]
impl<O, U> IsLogTest for POLog<O, U>
where
    O: PureCRDT + Clone,
    U: IsUnstableState<O> + Default + Debug,
{
    fn stable(&self) -> &impl IsStableState<<Self as IsLog>::Op> {
        &self.stable
    }

    fn unstable(&self) -> &impl IsUnstableState<<Self as IsLog>::Op> {
        &self.unstable
    }
}

#[cfg(feature = "fuzz")]
impl<O, U> IsLogFuzz for POLog<O, U>
where
    O: PureCRDT + Clone + OpGenerator,
    U: IsUnstableState<O> + Default + Debug,
{
    fn generate_op(&self, rng: &mut impl RngCore, config: &OpConfig) -> Self::Op {
        O::generate(rng, config, &self.stable, &self.unstable)
    }
}
