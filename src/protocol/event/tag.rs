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

impl Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{},{}]", self.id, self.lamport)
    }
}

/// Last-Writer-Wins
#[derive(Debug, PartialEq, Eq)]
pub struct Lww<'a>(pub &'a Tag);

impl<'a> Ord for Lww<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // First, compare Lamport timestamps
        match self.0.lamport.cmp(&other.0.lamport) {
            Ordering::Equal => {
                // Tie-break using origin id
                self.0.id.origin_id().cmp(&other.0.id.origin_id())
            }
            other_order => other_order,
        }
    }
}

impl<'a> PartialOrd for Lww<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// First-Writer-Wins
#[derive(Debug, PartialEq, Eq)]
pub struct Fww<'a>(pub &'a Tag);

impl<'a> Ord for Fww<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // First, compare Lamport timestamps
        match self.0.lamport.cmp(&other.0.lamport) {
            Ordering::Equal => {
                // Tie-break using origin id
                other.0.id.origin_id().cmp(&self.0.id.origin_id())
            }
            other_order => other_order,
        }
    }
}

impl<'a> PartialOrd for Fww<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Fair (https://amturing.acm.org/p558-lamport.pdf)
/// Use a round-robin policy to break ties.
/// For example, if C_i(a) = C_j(b) and j < i then we can let a -> b
/// if j < C_i(a) mod N <= i, and b -> a otherwise; where N is the total number of processes.
#[derive(Debug, PartialEq, Eq)]
pub struct Fair<'a>(pub &'a Tag);

impl<'a> Ord for Fair<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let result = match self.0.lamport.cmp(&other.0.lamport) {
            Ordering::Equal => {
                let val = self.0.lamport().val();
                let view = self.0.id().view().borrow();
                let mut members = view.members().map(|(_, id)| id).collect::<Vec<_>>();
                members.sort();
                let n = members.len();
                let round_leader = val % n;
                let self_idx = members
                    .iter()
                    .position(|&r| *r == self.0.id().origin_id())
                    .unwrap();
                let other_idx = members
                    .iter()
                    .position(|&r| *r == other.0.id().origin_id())
                    .unwrap();

                if other_idx < self_idx && other_idx < round_leader && round_leader <= self_idx {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            other_order => other_order,
        };
        result
    }
}

impl PartialOrd for Fair<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
