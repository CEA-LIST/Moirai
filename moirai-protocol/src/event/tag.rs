use std::fmt::Display;

use crate::event::{id::EventId, lamport::Lamport};

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
