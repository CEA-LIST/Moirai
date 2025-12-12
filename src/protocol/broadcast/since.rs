use crate::{
    protocol::{clock::version_vector::Version, event::id::EventId, replica::ReplicaId},
    HashSet,
};

#[derive(Debug, Clone)]
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
