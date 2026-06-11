use std::fmt::Display;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::clock::version_vector::Version;

/// This is not really a Lamport timestamp.
/// It is just a wrapper around the sum of the version vector, which is used for ordering events in the event log.
/// The sum of vector-clock components is a valid Lamport timestamp in the sense that it satisfies Lamport’s clock condition.
/// But it is not necessarily the same value as the Lamport algorithm would produce.
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Lamport(usize);

impl Lamport {
    pub fn val(&self) -> usize {
        self.0
    }

    pub fn new(val: usize) -> Self {
        Lamport(val)
    }
}

impl From<&Version> for Lamport {
    fn from(version: &Version) -> Self {
        Lamport(version.sum())
    }
}

impl Display for Lamport {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
