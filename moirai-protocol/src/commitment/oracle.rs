use std::fmt::Debug;

use crate::replica::{ReplicaId, ReplicaIdOwned};

pub trait IsOracle: Debug + Clone {
    fn query(&self) -> &ReplicaId;
}

#[derive(Clone, Debug)]
pub struct Omega {
    leader: ReplicaIdOwned,
}

impl Omega {
    pub fn new() -> Omega {
        Omega {
            leader: ReplicaIdOwned::default(),
        }
    }
}

impl IsOracle for Omega {
    fn query(&self) -> &ReplicaId {
        &self.leader
    }
}
