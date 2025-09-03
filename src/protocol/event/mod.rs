pub mod id;
pub mod lamport;
pub mod tag;
pub mod tagged_op;

use crate::protocol::{
    clock::version_vector::Version,
    event::{id::EventId, lamport::Lamport},
};

#[derive(Clone, Debug)]
pub struct Event<O> {
    id: EventId,
    lamport: Lamport,
    op: O,
    version: Version,
}

impl<O> Event<O> {
    pub fn new(id: EventId, lamport: Lamport, op: O, version: Version) -> Self {
        Self {
            id,
            lamport,
            op,
            version,
        }
    }

    pub fn unfold<N>(self, op: N) -> Event<N> {
        Event::new(self.id, self.lamport, op, self.version)
    }

    pub fn id(&self) -> &EventId {
        &self.id
    }

    pub fn op(&self) -> &O {
        &self.op
    }

    pub fn lamport(&self) -> &Lamport {
        &self.lamport
    }

    pub fn version(&self) -> &Version {
        &self.version
    }
}
