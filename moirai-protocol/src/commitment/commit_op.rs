use crate::replica::ReplicaIdOwned;

/// Operation stored by a commitment log.
#[derive(Clone, Debug)]
pub struct CommitOp<U> {
    pub update: U,
    // TODO: replace with replica idx later
    pub leader: ReplicaIdOwned,
}

impl<U> CommitOp<U> {
    pub fn new(update: U, leader: ReplicaIdOwned) -> Self {
        Self { update, leader }
    }
}
