use crate::{
    broadcast::internalizer::Resolver,
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    commitment::{
        leader_vote::SupportDelta,
        oracle::{IsOracle, Omega},
    },
    event::id::EventId,
    replica::ReplicaIdx,
    utils::hashmap::{HashMap, HashSet},
};

/// Commitment protocol, which is responsible for
/// determining the current leader based on the votes of the replicas.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct CommitmentProtocol<Oracle>
where
    Oracle: IsOracle,
{
    /// The oracle that provides information about the leader to the local replica.
    oracle: Oracle,
    /// The resolver that maps replica indices to replica IDs, but also provides the members of the system.
    /// It is used to determine the quorum of votes for a candidate leader.
    resolver: Resolver,
    /// The committed causal cut, advanced by quorum-backed anchors or the
    /// stable frontier from TCSB.
    last_committed: Option<CausalFrontier>,
    /// Incrementally maintained positive support relationships for each candidate.
    /// Map key = supported event
    support: HashMap<EventId, HashSet<ReplicaIdx>>,
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
            support: HashMap::default(),
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

    pub fn last_committed(&self) -> Option<&CausalFrontier> {
        self.last_committed.as_ref()
    }

    pub fn update_last_committed(&mut self, frontier: CausalFrontier) {
        self.last_committed = Some(frontier);
    }

    /// Applies newly decided support relationships and returns the anchors reached while
    /// processing them. Duplicate relationships are ignored.
    pub fn apply_support_deltas(
        &mut self,
        deltas: impl IntoIterator<Item = SupportDelta>,
    ) -> Vec<EventId> {
        let quorum = self.quorum();
        let mut new_anchors = Vec::new();

        for delta in deltas {
            let candidate = delta.candidate;

            let maybe_new_leader = {
                let supporters = self.support.entry(candidate.clone()).or_default();

                // If candidate reach quorum
                if supporters.insert(delta.supporter) && supporters.len() == quorum {
                    Some(candidate)
                } else {
                    None
                }
            };

            if let Some(new_leader) = maybe_new_leader {
                new_anchors.push(new_leader);
            }
        }

        // TODO: If anchor != None, maybe we can perform some garbage collection of the `support` table?

        new_anchors
    }

    pub fn advance_commitment(&mut self, candidate: &Version) -> bool {
        let candidate_frontier = CausalFrontier::from(candidate);
        let advances = match &self.last_committed {
            Some(last_committed) => {
                candidate_frontier.partial_cmp(last_committed) == Some(std::cmp::Ordering::Greater)
            }
            None => true,
        };

        if advances {
            self.last_committed = Some(candidate_frontier);
            return true;
        }

        false
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
            support: HashMap::default(),
        }
    }
}
