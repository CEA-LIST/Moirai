use crate::commitment::wire_id::{StableEventId, StableReplicaId};



/// A commitment operation with additional information to make computation more efficient
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct SupportOp<O> {
    /// Update operation
    pub op: O,
    /// Stable leader ID; local ReplicaIdx values must never travel in payloads.
    pub vote: StableReplicaId,
    /// Birth eligibility from predecessor votes; receivers verify actual support.
    pub is_partial: bool,
    /// Latest partial leader in the sender history before issuing this vertex.
    pub last_partial: Option<StableEventId>,
}

impl<O> SupportOp<O> {
    pub fn new(op: O, vote: StableReplicaId, is_partial: bool, last_partial: Option<StableEventId>) -> Self {
        Self { op, vote, is_partial, last_partial }
    }

    pub fn split(self) -> (O, StableReplicaId, bool, Option<StableEventId>) {
        (self.op, self.vote, self.is_partial, self.last_partial)
    }

    pub fn get_child_op(&self) -> O
    where
        O: Clone,
    {
        self.op.clone()
    }
}
