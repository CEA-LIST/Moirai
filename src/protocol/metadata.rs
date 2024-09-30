use serde::{Deserialize, Serialize};

use crate::clocks::vector_clock::VectorClock;

use super::utils::Keyable;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(PartialEq, Eq, Clone, Debug, Default)]
#[cfg_attr(feature = "utils", derive(Serialize, Deserialize))]
pub struct Metadata {
    pub vc: VectorClock<String, usize>,
    pub origin: String,
}

impl Metadata {
    pub fn new(vc: VectorClock<String, usize>, origin: &str) -> Self {
        Self {
            vc,
            origin: origin.to_string(),
        }
    }

    pub fn bot() -> Self {
        Self {
            vc: VectorClock::bot(),
            origin: String::new(),
        }
    }

    pub fn get_origin_lamport(&self) -> usize {
        self.vc.get(&self.origin).expect("Origin not found")
    }

    pub fn get_lamport(&self, origin: &str) -> usize {
        self.vc
            .get(&String::from(origin))
            .expect("Origin not found")
    }
}

impl PartialOrd for Metadata {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Metadata {
    fn cmp(&self, other: &Self) -> Ordering {
        let clock_cmp: Option<Ordering> = self.vc.partial_cmp(&other.vc);
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
        write!(f, "{}{}", self.vc, origin)
    }
}
