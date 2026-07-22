use std::fmt::Debug;

use crate::replica::{ReplicaId, ReplicaIdOwned};

pub trait IsOracle: Debug + Clone {
    fn query(&self) -> &ReplicaId;
}

#[derive(Clone, Debug)]
pub struct Omega {
    // TODO: use replica idx
    leader: ReplicaIdOwned,
}

impl Omega {
    pub fn new() -> Omega {
        Omega {
            leader: ReplicaIdOwned::default(),
        }
    }

    pub fn with_leader(leader: impl Into<ReplicaIdOwned>) -> Omega {
        Omega {
            leader: leader.into(),
        }
    }

    pub fn set_leader(&mut self, leader: impl Into<ReplicaIdOwned>) {
        self.leader = leader.into();
    }
}

impl Default for Omega {
    fn default() -> Self {
        Self::new()
    }
}

impl IsOracle for Omega {
    fn query(&self) -> &ReplicaId {
        &self.leader
    }
}
