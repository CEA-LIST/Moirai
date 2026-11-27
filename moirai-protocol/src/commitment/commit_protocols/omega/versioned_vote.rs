use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::{
    clock::version_vector::Version, commitment::commit_protocols::omega::vote_origin::VoteOrigin,
    replica::ReplicaIdx,
};

/// An entry in the vote log, with a replica vote, a vector clock and last seen partial leader
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Debug, Clone)]
pub struct VersionedVote {
    vote_origin: VoteOrigin,
    lamport: usize,
    /// Lexicographic rank of the origin's name in the fixed membership.
    origin_order: usize,
    vote: ReplicaIdx,
    version: Version,
    is_partial: bool,
    last_partial: Option<VoteOrigin>,
}

impl VersionedVote {
    /// `origin_order` is the origin's lexicographic name rank in the fixed
    /// membership, identical across replicas even when local indices differ.
    pub fn new(
        vote: ReplicaIdx,
        version: Version,
        is_partial: bool,
        last_partial: Option<VoteOrigin>,
        origin_order: usize,
    ) -> Self {
        Self {
            vote_origin: VoteOrigin::from(&version),
            lamport: version.sum(),
            origin_order,
            vote,
            version,
            is_partial,
            last_partial,
        }
    }

    pub fn vote_origin(&self) -> &VoteOrigin {
        &self.vote_origin
    }

    pub fn origin_idx(&self) -> ReplicaIdx {
        self.version.origin_idx()
    }

    pub fn origin_seq(&self) -> usize {
        self.vote_origin.seq()
    }

    pub fn vote(&self) -> ReplicaIdx {
        self.vote
    }

    pub fn version(&self) -> &Version {
        &self.version
    }

    pub fn is_partial(&self) -> bool {
        self.is_partial
    }

    pub fn last_partial(&self) -> &Option<VoteOrigin> {
        &self.last_partial
    }

    pub fn split(self) -> (VoteOrigin, ReplicaIdx, Version, bool, Option<VoteOrigin>) {
        (
            self.vote_origin,
            self.vote,
            self.version,
            self.is_partial,
            self.last_partial,
        )
    }
}

// Records are keyed by immutable event identity and its causal timestamp.
// Payload fields are metadata of that event, not independent key components.
// Name ranks are computed once at construction, independently of local indices.
impl Ord for VersionedVote {
    fn cmp(&self, other: &Self) -> Ordering {
        self.lamport
            .cmp(&other.lamport)
            .then_with(|| self.origin_order.cmp(&other.origin_order))
            .then_with(|| self.origin_seq().cmp(&other.origin_seq()))
    }
}

impl PartialEq for VersionedVote {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for VersionedVote {}

impl PartialOrd for VersionedVote {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for VersionedVote {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{:?}@{:?}]", self.vote, self.version)
    }
}
