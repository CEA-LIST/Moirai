use std::fmt::Debug;

use tracing::info;

use crate::protocol::{
    clock::version_vector::Version,
    crdt::pure_crdt::{PureCRDT, RedundancyRelation},
    event::{tagged_op::TaggedOp, Event},
    state::{log::IsLog, stable_state::IsStableState, unstable_state::IsUnstableState},
};

#[cfg(test)]
use crate::protocol::state::log::IsLogTest;

pub type VecLog<O> = POLog<O, Vec<TaggedOp<O>>>;

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
    type Op = O;
    type Value = O::Value;

    fn new() -> Self {
        Self {
            stable: O::StableState::default(),
            unstable: U::default(),
        }
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        let new_tagged_op = TaggedOp::from(&event);
        if O::redundant_itself(&new_tagged_op, &self.stable, self.unstable.iter()) {
            if !O::DISABLE_R_WHEN_R {
                info!("Pruning redundant ops");
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

    fn eval(&self) -> Self::Value {
        O::eval(&self.stable, &self.unstable)
    }

    fn stabilize(&mut self, version: &Version) {
        if O::DISABLE_STABILIZE {
            return;
        }
        info!("Stabilizing with version: {}", version);
        // 1. select all ops in unstable that are predecessors of a version
        // 2. for each of them, call stabilize, which may modify stable and/or unstable
        // 3. if the operation is still in unstable, apply the op to stable and remove it from unstable

        let candidates = self.unstable.predecessors(version);
        info!("Candidates (stabilize): {:?}", candidates);

        for tagged_op in candidates {
            O::stabilize(&tagged_op, &mut self.stable, &mut self.unstable);
            if self.unstable.get(tagged_op.id()).is_some() {
                self.stable.apply(tagged_op.op().clone());
                self.unstable.remove(tagged_op.id());
            }
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        info!("<<< REDUNDANT BY PARENT>>>");
        info!("BEFORE stable: {:?}", self.stable);
        info!("BEFORE unstable: {:?}", self.unstable);

        self.stable.clear();
        if conservative {
            self.unstable
                .retain(|tagged_op| !tagged_op.id().is_predecessor_of(version))
        } else {
            self.unstable.clear();
        }

        info!("AFTER stable: {:?}", self.stable);
        info!("AFTER unstable: {:?}", self.unstable);
    }

    fn len(&self) -> usize {
        self.stable.len() + self.unstable.len()
    }

    fn is_empty(&self) -> bool {
        self.stable.is_empty() && self.unstable.is_empty()
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
        info!("<<<<< Pruning STABLE redundant ops >>>>");
        info!("Content stable: {:?}", self.stable);
        self.stable.prune_redundant_ops(rdnt, new_tagged_op);
        info!("<<<<< Pruning UNSTABLE redundant ops >>>>");
        self.unstable.retain(|old_tagged_op| {
            // Note: the new operation is not in the log at this point.
            let is_conc = !old_tagged_op.id().is_predecessor_of(version);
            let boo = !rdnt(
                old_tagged_op.op(),
                Some(old_tagged_op.tag()),
                is_conc,
                new_tagged_op,
            );
            info!(
                "The op {} is {} by {}",
                old_tagged_op,
                if boo { "not redundant" } else { "redundant" },
                new_tagged_op
            );
            boo
        });
    }
}

#[cfg(test)]
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
