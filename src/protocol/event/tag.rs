use std::{cmp::Ordering, fmt::Display};

use crate::protocol::event::{id::EventId, lamport::Lamport};

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

impl PartialOrd for Tag {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// TODO: implements a fairer Ord (https://amturing.acm.org/p558-lamport.pdf)
impl Ord for Tag {
    fn cmp(&self, other: &Self) -> Ordering {
        // First, compare Lamport timestamps
        match self.lamport.cmp(&other.lamport) {
            Ordering::Equal => {
                // Tie-break using origin id
                println!(
                    "{} is {:?} than {}",
                    self.id.origin_id(),
                    self.id.origin_id().cmp(&other.id.origin_id()),
                    other.id.origin_id(),
                );
                self.id.origin_id().cmp(&other.id.origin_id())
            }
            other_order => other_order,
        }
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{},{}]", self.id, self.lamport)
    }
}
