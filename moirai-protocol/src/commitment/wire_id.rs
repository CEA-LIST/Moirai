use std::fmt::{self, Display};

/// Stable, compact member number assigned by the system manager.
/// The same number must name the same replica in every configured log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct StableReplicaId(pub u16);

impl From<u16> for StableReplicaId {
    fn from(value: u16) -> Self { Self(value) }
}

impl Display for StableReplicaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Compact event reference carried in the commitment payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct StableEventId {
    pub origin: StableReplicaId,
    pub seq: u64,
}

impl StableEventId {
    pub fn new(origin: StableReplicaId, seq: u64) -> Self {
        Self { origin, seq }
    }
}
