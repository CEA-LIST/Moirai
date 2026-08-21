use crate::commitment::leader_vote::LeaderVote;

/// A commitment operation, which consists of an updateoperation and a vote for a leader
#[derive(Debug, Clone)]
pub struct CommitOp<O> {
    /// Update operation
    pub op: O,
    /// Leader vote
    pub vote: LeaderVote,
}

impl<O> CommitOp<O> {
    pub fn new(op: O, vote: LeaderVote) -> Self {
        Self { op, vote }
    }

    pub fn split(self) -> (O, LeaderVote) {
        (self.op, self.vote)
    }
}
