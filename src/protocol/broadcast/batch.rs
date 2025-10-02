use std::fmt::{Debug, Display};

use crate::{
    protocol::{clock::version_vector::Version, event::Event, membership::ReplicaId},
    utils::intern_str::Resolver,
};

#[derive(Debug)]
pub struct Batch<O> {
    pub events: Vec<Event<O>>,
    pub version: Version,
    pub resolver: Resolver,
}

impl<O> Batch<O> {
    pub fn new(events: Vec<Event<O>>, version: Version, resolver: Resolver) -> Self {
        Self {
            events,
            version,
            resolver,
        }
    }

    pub fn events(self) -> Vec<Event<O>> {
        self.events
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
            write!(f, "{}", event.id())?;
            first = false;
        }
        write!(f, "], version: {} }}", self.version)
    }
}
