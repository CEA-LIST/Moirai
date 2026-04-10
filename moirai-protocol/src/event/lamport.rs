use std::fmt::Display;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::clock::version_vector::Version;

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
