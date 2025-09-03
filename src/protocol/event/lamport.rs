use crate::protocol::clock::version_vector::Version;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lamport(usize);

impl From<&Version> for Lamport {
    fn from(version: &Version) -> Self {
        Lamport(version.sum())
    }
}
