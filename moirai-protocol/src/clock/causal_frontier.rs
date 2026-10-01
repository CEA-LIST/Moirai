use std::{
    cmp::Ordering,
    fmt::{self, Display},
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::{
    broadcast::internalizer::Resolver,
    clock::version_vector::{Seq, Version, VersionEntries},
    event::id::EventId,
    replica::ReplicaIdx,
};

/// A component-wise causal cut with no distinguished event origin.
///
/// Unlike [`Version`], a frontier may be the join or meet of several event
/// versions. It is not the version of an event.
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Debug, Clone, PartialEq)]
pub struct CausalFrontier {
    entries: VersionEntries,
    resolver: Resolver,
}

impl Eq for CausalFrontier {}

impl CausalFrontier {
    pub fn new(resolver: Resolver) -> Self {
        Self {
            entries: VersionEntries(vec![0; resolver.len()]),
            resolver,
        }
    }

    pub fn resolver(&self) -> &Resolver {
        &self.resolver
    }

    pub fn seq_by_idx(&self, idx: ReplicaIdx) -> Seq {
        self.entries.get(idx)
    }

    pub fn iter(&self) -> impl Iterator<Item = (ReplicaIdx, Seq)> + '_ {
        self.entries
            .0
            .iter()
            .copied()
            .enumerate()
            .map(|(i, seq)| (ReplicaIdx(i), seq))
    }

    pub fn set_by_idx(&mut self, idx: ReplicaIdx, value: Seq) {
        *self.entries.get_mut(idx) = value;
    }

    pub fn sum(&self) -> usize {
        self.entries.0.iter().sum()
    }

    pub fn len(&self) -> usize {
        self.resolver.len()
    }

    pub fn is_empty(&self) -> bool {
        self.resolver.is_empty()
    }

    /// Incorporate an event's causal past and the event itself.
    pub fn join_version(&mut self, version: &Version) {
        self.entries.join(&version.entries);
    }

    /// Incorporate everything represented by another frontier.
    pub fn join(&mut self, other: &Self) {
        self.entries.join(&other.entries);
    }

    /// The component-wise intersection of two causal cuts.
    pub fn meet(&mut self, other: &Self) {
        self.entries.meet(&other.entries);
    }

    pub fn contains(&self, event_id: &EventId) -> bool {
        self.seq_by_idx(event_id.idx()) >= event_id.seq()
    }
}

impl PartialOrd for CausalFrontier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        debug_assert!(
            self.resolver == other.resolver,
            "Comparing clocks with different views"
        );
        let mut greater = false;
        let mut less = false;
        for idx in 0..self.entries.0.len().max(other.entries.0.len()) {
            match self
                .seq_by_idx(ReplicaIdx(idx))
                .cmp(&other.seq_by_idx(ReplicaIdx(idx)))
            {
                Ordering::Greater => greater = true,
                Ordering::Less => less = true,
                Ordering::Equal => {}
            }
            if greater && less {
                return None;
            }
        }
        match (greater, less) {
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            (false, false) => Some(Ordering::Equal),
            (true, true) => unreachable!(),
        }
    }
}

impl From<Version> for CausalFrontier {
    fn from(version: Version) -> Self {
        Self {
            entries: version.entries,
            resolver: version.resolver,
        }
    }
}

impl From<&Version> for CausalFrontier {
    fn from(version: &Version) -> Self {
        Self {
            entries: version.entries.clone(),
            resolver: version.resolver.clone(),
        }
    }
}

/// Reverse conversion requires the caller to choose an origin. The resulting
/// `Version` retains the entries but is not thereby an actual event version.
impl From<(CausalFrontier, ReplicaIdx)> for Version {
    fn from((frontier, origin_idx): (CausalFrontier, ReplicaIdx)) -> Self {
        Self {
            entries: frontier.entries,
            origin_idx,
            resolver: frontier.resolver,
        }
    }
}

impl From<(&CausalFrontier, ReplicaIdx)> for Version {
    fn from((frontier, origin_idx): (&CausalFrontier, ReplicaIdx)) -> Self {
        Self {
            entries: frontier.entries.clone(),
            origin_idx,
            resolver: frontier.resolver.clone(),
        }
    }
}

impl Display for CausalFrontier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{{}}}",
            self.iter()
                .map(|(idx, seq)| format!("\"{}\":{}", self.resolver.resolve(idx).unwrap(), seq))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}
