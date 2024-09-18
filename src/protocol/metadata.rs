use crate::clocks::vector_clock::VectorClock;

use super::utils::{Incrementable, Keyable};
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(PartialEq, Eq, Clone, Debug, Default)]
pub struct Metadata {
    pub vc: VectorClock<&'static str, usize>,
    pub origin: &'static str,
}

impl Metadata {
    pub fn new(vc: VectorClock<&'static str, usize>, origin: &'static str) -> Self {
        Self { vc, origin }
    }

    pub fn bot() -> Self {
        Self {
            vc: VectorClock::bot(),
            origin: "",
        }
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
            Some(Ordering::Equal) | None => other.origin.cmp(self.origin),
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
