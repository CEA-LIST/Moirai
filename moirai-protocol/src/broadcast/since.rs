#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::{HashSet, clock::version_vector::Version, event::id::EventId, replica::ReplicaId};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct Since {
    version: Version,
    except: HashSet<EventId>,
}

impl Since {
    #[allow(clippy::mutable_key_type)]
    pub fn new(version: Version, except: HashSet<EventId>) -> Self {
        Self { version, except }
    }

    pub fn version(&self) -> &Version {
        &self.version
    }

    #[allow(clippy::mutable_key_type)]
    pub fn except(&self) -> &HashSet<EventId> {
        &self.except
    }

    pub fn origin_id(&self) -> &ReplicaId {
        self.version.origin_id()
    }
}
