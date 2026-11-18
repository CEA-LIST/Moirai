use std::fmt::Debug;

use crate::commitment::wire_id::StableReplicaId;

/// An oracle is a component that provides information about the current state of the system, such as the current leader.
pub trait IsOracle: Debug + Clone + Default {
    /// Query the oracle for the current leader.
    fn query(&self) -> StableReplicaId;

    fn set_leader(&mut self, leader: impl Into<StableReplicaId>);
}

/// Oracle output holder; configure a stable member ID before use.
/// An external oracle must provide eventual agreement and accuracy.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct Omega {
    leader: StableReplicaId,
}

impl Omega {
    pub fn new() -> Omega {
        Omega { leader: StableReplicaId(u16::MIN) }
    }

    pub fn with_leader(leader: impl Into<StableReplicaId>) -> Omega {
        Omega {
            leader: leader.into(),
        }
    }
}

impl Default for Omega {
    fn default() -> Self {
        Self::new()
    }
}

impl IsOracle for Omega {
    fn query(&self) -> StableReplicaId {
        self.leader.clone()
    }
    fn set_leader(&mut self, leader: impl Into<StableReplicaId>) {
        self.leader = leader.into();
    }
}
