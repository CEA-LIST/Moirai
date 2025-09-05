use std::fmt::Display;

use crate::{
    protocol::{
        clock::version_vector::{Seq, Version},
        membership::{view::View, ReplicaId, ReplicaIdx},
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
        assert!(seq > 0);
        Self { idx, seq, view }
    }

    // TODO: should not clone
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

    /// Check if this event id is a predecessor of the given version.
    /// # Note
    /// Returns `true` if sequence number of the version for the replica id is greater OR equal.
    pub fn is_predecessor_of(&self, version: &Version) -> bool {
        let ver_seq = version.seq_by_id(&self.origin_id()).unwrap_or(0);
        tracing::info!(
            "Checking if {} is a predecessor of {}: {}",
            self,
            version,
            ver_seq >= self.seq
        );
        ver_seq >= self.seq
    }
}

impl Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "({}{})",
            self.view.borrow().get_id(self.idx).unwrap(),
            self.seq,
        )
    }
}
