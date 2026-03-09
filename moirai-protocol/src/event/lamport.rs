use std::fmt::Display;

use crate::clock::version_vector::Version;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
