use std::fmt::Debug;

use crate::replica::ReplicaId;

/// An oracle is a component that provides information about the current state of the system, such as the current leader.
pub trait IsOracle: Debug + Clone + Default {
    /// Query the oracle for the current leader.
    fn query(&self) -> Option<ReplicaId>;
}

/// Eventual leader oracle
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct Omega {
    leader: Option<ReplicaId>,
}

impl Omega {
    pub fn new() -> Omega {
        Omega { leader: None }
    }

    pub fn with_leader(leader: impl Into<ReplicaId>) -> Omega {
        Omega {
            leader: Some(leader.into()),
        }
    }

    pub fn set_leader(&mut self, leader: impl Into<ReplicaId>) {
        self.leader = Some(leader.into());
    }
}

impl Default for Omega {
    fn default() -> Self {
        Self::new()
    }
}

impl IsOracle for Omega {
    fn query(&self) -> Option<ReplicaId> {
        self.leader.clone()
    }
}
