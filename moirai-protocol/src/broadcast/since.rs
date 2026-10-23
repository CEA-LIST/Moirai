#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use rustc_hash::FxHashSet;

use crate::{clock::version_vector::Version, event::id::EventId, replica::ReplicaId};

/// Request all events since the given version, except for the events in the `except` set.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct Since {
    version: Version,
    /// Except the events in this set
    except: FxHashSet<EventId>,
}

impl Since {
    #[allow(clippy::mutable_key_type)]
    pub fn new(version: Version, except: FxHashSet<EventId>) -> Self {
        Self { version, except }
    }

    pub fn version(&self) -> &Version {
        &self.version
    }

    #[allow(clippy::mutable_key_type)]
    pub fn except(&self) -> &FxHashSet<EventId> {
        &self.except
    }

    pub fn origin_id(&self) -> &ReplicaId {
        self.version.origin_id()
    }
}
