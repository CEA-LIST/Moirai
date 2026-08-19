use std::fmt::Debug;

use crate::replica::ReplicaIdOwned;

/// An oracle is a component that provides information about the current state of the system, such as the current leader.
pub trait IsOracle: Debug + Clone + Default {
    /// Query the oracle for the current leader.
    fn query(&self) -> Option<ReplicaIdOwned>;
}

/// Eventual leader oracle
#[derive(Clone, Debug)]
pub struct Omega {
    leader: Option<ReplicaIdOwned>,
}

impl Omega {
    pub fn new() -> Omega {
        Omega { leader: None }
    }

    pub fn with_leader(leader: impl Into<ReplicaIdOwned>) -> Omega {
        Omega {
            leader: Some(leader.into()),
        }
    }

    pub fn set_leader(&mut self, leader: impl Into<ReplicaIdOwned>) {
        self.leader = Some(leader.into());
    }
}

impl Default for Omega {
    fn default() -> Self {
        Self::new()
    }
}

impl IsOracle for Omega {
    fn query(&self) -> Option<ReplicaIdOwned> {
        self.leader.clone()
    }
}
