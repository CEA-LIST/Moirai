use crate::replica::ReplicaIdx;

/// Track the votes of replicas for a given leader in the commitment protocol.
#[derive(Debug, Clone, Default)]
pub struct LeaderVotes {
    votes: Vec<Option<bool>>,
    eligible_count: usize,
    validated_count: usize,
}

impl LeaderVotes {
    pub fn from_pending_voters(voters: impl IntoIterator<Item = ReplicaIdx>) -> Self {
        let mut votes = Self::default();
        for replica_idx in voters {
            votes.insert_pending(replica_idx);
        }
        votes
    }

    pub fn is_partial(&self, quorum_size: usize) -> bool {
        self.eligible_count >= quorum_size
    }

    pub fn is_pre_committed(&self, quorum_size: usize) -> bool {
        self.validated_count >= quorum_size
    }

    pub fn is_missing_or_validated(&self, replica_idx: ReplicaIdx) -> bool {
        self.status(replica_idx).unwrap_or(true)
    }

    pub fn validate(&mut self, replica_idx: ReplicaIdx) {
        if let Some(slot) = self.votes.get_mut(replica_idx.0)
            && matches!(slot, Some(false))
        {
            *slot = Some(true);
            self.validated_count += 1;
        }
    }

    pub fn remove(&mut self, replica_idx: ReplicaIdx) {
        let Some(slot) = self.votes.get_mut(replica_idx.0) else {
            return;
        };

        let Some(was_validated) = slot.take() else {
            return;
        };

        self.eligible_count -= 1;
        if was_validated {
            self.validated_count -= 1;
        }
    }

    fn insert_pending(&mut self, replica_idx: ReplicaIdx) {
        self.ensure_capacity(replica_idx);

        if self.votes[replica_idx.0].is_none() {
            self.votes[replica_idx.0] = Some(false);
            self.eligible_count += 1;
        }
    }

    fn status(&self, replica_idx: ReplicaIdx) -> Option<bool> {
        self.votes.get(replica_idx.0).copied().flatten()
    }

    fn ensure_capacity(&mut self, replica_idx: ReplicaIdx) {
        if self.votes.len() <= replica_idx.0 {
            self.votes.resize(replica_idx.0 + 1, None);
        }
    }
}
