use crate::{
    broadcast::internalizer::Resolver,
    clock::version_vector::Version,
    commitment::oracle::{IsOracle, Omega},
    replica::ReplicaIdx,
};

/// Commitment protocol, which is responsible for
/// determining the current leader based on the votes of the replicas.
#[derive(Debug, Clone)]
pub struct CommitmentProtocol<Oracle>
where
    Oracle: IsOracle,
{
    /// The oracle that provides information about the leader to the local replica.
    oracle: Oracle,
    /// The resolver that maps replica indices to replica IDs, but also provides the members of the system.
    /// It is used to determine the quorum of votes for a candidate leader.
    resolver: Resolver,
    /// The last committed anchor, which is either the greatest version
    /// that has been committed by a quorum of replicas
    /// or the last stable version (LSV) from the TCSB
    last_committed: Option<Version>,
}

impl<Oracle> CommitmentProtocol<Oracle>
where
    Oracle: IsOracle,
{
    pub fn new(oracle: Oracle, resolver: Resolver) -> Self {
        Self {
            oracle,
            resolver,
            last_committed: None,
        }
    }

    pub fn oracle(&self) -> &Oracle {
        &self.oracle
    }

    pub fn oracle_mut(&mut self) -> &mut Oracle {
        &mut self.oracle
    }

    pub fn resolver(&self) -> &Resolver {
        &self.resolver
    }

    pub fn update_resolver(&mut self, resolver: Resolver) {
        self.resolver = resolver;
    }

    pub fn last_committed(&self) -> Option<&Version> {
        self.last_committed.as_ref()
    }

    pub fn update_last_committed(&mut self, version: Version) {
        self.last_committed = Some(version);
    }

    /// Simple majority quorum, which is the minimum number of votes required to commit a candidate leader.
    pub fn quorum(&self) -> usize {
        self.resolver.len() / 2 + 1
    }

    pub fn members(&self) -> Vec<ReplicaIdx> {
        let mut members = vec![];
        for i in 0..self.resolver.len() {
            members.push(ReplicaIdx(i));
        }
        members
    }
}

impl Default for CommitmentProtocol<Omega> {
    fn default() -> Self {
        Self {
            oracle: Omega::new(),
            resolver: Resolver::default(),
            last_committed: None,
        }
    }
}
