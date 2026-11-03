use crate::commitment::leader_vote::LeaderVoteUpdate;

/// A commitment operation, which consists of an updateoperation and a vote for a leader
#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct CommitOp<O> {
    /// Update operation
    pub op: O,
    /// Leader vote
    pub vote: LeaderVoteUpdate,
}

impl<O> CommitOp<O> {
    pub fn new(op: O, vote: LeaderVoteUpdate) -> Self {
        Self { op, vote }
    }

    pub fn split(self) -> (O, LeaderVoteUpdate) {
        (self.op, self.vote)
    }
}
