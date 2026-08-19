use crate::replica::ReplicaIdOwned;

/// A commitment operation, which consists of an updateoperation and a vote for a leader
#[derive(Debug, Clone)]
pub struct CommitOp<O> {
    /// Update operation
    pub op: O,
    /// Leader vote
    pub leader: ReplicaIdOwned,
}

impl<O> CommitOp<O> {
    pub fn new(op: O, leader: ReplicaIdOwned) -> Self {
        Self { op, leader }
    }

    pub fn split(self) -> (O, ReplicaIdOwned) {
        (self.op, self.leader)
    }
}
