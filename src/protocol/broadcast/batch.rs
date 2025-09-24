use std::fmt::{Debug, Display};

use crate::protocol::{
    clock::version_vector::Version, event::wire_event::WireEvent, membership::ReplicaId,
};

#[derive(Debug)]
pub struct Batch<O> {
    pub events: Vec<WireEvent<O>>,
    pub version: Version,
}

impl<O> Batch<O> {
    pub fn new(events: Vec<WireEvent<O>>, version: Version) -> Self {
        Self { events, version }
    }

    pub fn events(&self) -> &Vec<WireEvent<O>> {
        &self.events
    }

    pub fn version(&self) -> &Version {
        &self.version
    }

    pub fn origin_id(&self) -> &ReplicaId {
        self.version.origin_id()
    }
}

impl<O> Display for Batch<O>
where
    O: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Batch {{ events: [")?;
        let mut first = true;
        for event in &self.events {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{}", event.id.0)?;
            first = false;
        }
        write!(f, "], version: {} }}", self.version)
    }
}
