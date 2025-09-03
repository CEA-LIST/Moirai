use crate::protocol::{clock::version_vector::Version, event::Event};

pub struct Batch<O> {
    pub events: Vec<Event<O>>,
    pub version: Version,
}

impl<O> Batch<O> {
    pub fn new(events: Vec<Event<O>>, version: Version) -> Self {
        Self { events, version }
    }
}
