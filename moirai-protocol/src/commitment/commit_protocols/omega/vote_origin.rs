use std::fmt::Display;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::{clock::version_vector::Version, replica::ReplicaIdx};

/// Id of an entry in the vote log
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct VoteOrigin {
    id: ReplicaIdx,
    seq: usize,
}

impl VoteOrigin {
    pub fn new(id: ReplicaIdx, seq: usize) -> Self {
        Self { id, seq }
    }

    pub fn id(&self) -> ReplicaIdx {
        self.id
    }

    pub fn seq(&self) -> usize {
        self.seq
    }

    pub fn from(version: &Version) -> VoteOrigin {
        VoteOrigin::new(version.origin_idx(), version.origin_seq())
    }
}

impl Display for VoteOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{:?},{}]", self.id, self.seq)
    }
}
