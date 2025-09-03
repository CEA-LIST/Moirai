use crate::{
    protocol::{
        clock::version_vector::{Seq, Version},
        membership::{ReplicaId, ReplicaIdx, View},
    },
    utils::mut_owner::Reader,
};

/// Represents the unique identifier for an operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventId {
    idx: ReplicaIdx,
    seq: Seq,
    view: Reader<View>,
}

impl EventId {
    pub fn new(idx: ReplicaIdx, seq: Seq, view: Reader<View>) -> Self {
        Self { idx, seq, view }
    }

    pub fn origin_id(&self) -> ReplicaId {
        let view = self.view.borrow();
        view.get_id(self.idx).unwrap().clone()
    }

    pub fn seq(&self) -> Seq {
        self.seq
    }

    pub fn origin_idx(&self) -> ReplicaIdx {
        self.idx
    }

    pub fn is_predecessor_of(&self, version: &Version) -> bool {
        version.seq_by_idx(self.idx).unwrap_or(0) > self.seq
    }
}
