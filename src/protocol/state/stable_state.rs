use std::fmt::Debug;

use crate::protocol::{crdt::pure_crdt::RedundancyRelation, event::tagged_op::TaggedOp};

pub trait IsStableState<O>: Default + Debug {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn apply(&mut self, value: O);
    fn clear(&mut self);
    // TODO: maybe give just the op and not the tagged_op
    fn prune_redundant_ops(&mut self, rdnt: RedundancyRelation<O>, tagged_op: &TaggedOp<O>);
}

impl<O> IsStableState<O> for Vec<O>
where
    O: Debug,
{
    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }

    fn apply(&mut self, value: O) {
        self.push(value);
    }

    fn clear(&mut self) {
        self.clear();
    }

    fn prune_redundant_ops(&mut self, rdnt: RedundancyRelation<O>, new_tagged_op: &TaggedOp<O>) {
        self.retain(|o| {
            let is_rdnt = rdnt(o, None, false, new_tagged_op);
            !is_rdnt
        });
    }
}
