#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::clocks::vector_clock::VectorClock;

use super::utils::Keyable;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(PartialEq, Eq, Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Metadata {
    pub clock: VectorClock<String, usize>,
    pub origin: String,
    pub view_id: usize,
}

impl Metadata {
    pub fn new(clock: VectorClock<String, usize>, origin: &str, view_id: usize) -> Self {
        Self {
            clock,
            origin: origin.to_string(),
            view_id,
        }
    }

    pub fn bot() -> Self {
        Self {
            clock: VectorClock::bot(),
            origin: String::new(),
            view_id: 0,
        }
    }

    pub fn dot(&self) -> (String, usize) {
        if self.origin.is_empty() {
            (String::new(), 0)
        } else {
            (self.origin.clone(), self.clock.get(&self.origin).unwrap())
        }
    }

    pub fn get_origin_lamport(&self) -> Option<usize> {
        self.clock.get(&self.origin)
    }

    pub fn get_lamport(&self, origin: &str) -> Option<usize> {
        self.clock.get(&String::from(origin))
    }
}

impl PartialOrd for Metadata {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// TODO: garbage
impl Ord for Metadata {
    fn cmp(&self, other: &Self) -> Ordering {
        let clock_cmp: Option<Ordering> = self.clock.partial_cmp(&other.clock);
        assert!(
            !(clock_cmp.is_none() && self.origin == other.origin),
            "Self: {}, Other: {}",
            self,
            other,
        );
        match clock_cmp {
            Some(Ordering::Equal) | None => other.origin.cmp(&self.origin),
            Some(Ordering::Less) => Ordering::Less,
            Some(Ordering::Greater) => Ordering::Greater,
        }
    }
}

impl Display for Metadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let origin = if self.origin.is_empty() {
            "".to_string()
        } else {
            format!("@{}", self.origin)
        };
        write!(f, "{}{}", self.clock, origin)
    }
}
