use std::fmt::Display;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::event::{id::EventId, lamport::Lamport};

/// A tag that contains an event ID and a Lamport timestamp
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Tag {
    id: EventId,
    lamport: Lamport,
}

impl Tag {
    pub fn new(id: EventId, lamport: Lamport) -> Self {
        Self { id, lamport }
    }

    pub fn id(&self) -> &EventId {
        &self.id
    }

    pub fn lamport(&self) -> &Lamport {
        &self.lamport
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{},{}]", self.id, self.lamport)
    }
}
