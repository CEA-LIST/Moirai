use crate::{
    broadcast::internalizer::Resolver,
    clock::version_vector::Version,
    commitment::oracle::{IsOracle, Omega},
    event::{id::EventId, tagged_op::TaggedOp},
    replica::ReplicaIdx,
    state::unstable_state::event_history::EventHistory,
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
    pub last_committed: Option<Version>,
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

    /// Simple majority quorum, which is the minimum number of votes required to commit a candidate leader.
    fn quorum(&self) -> usize {
        self.resolver.len() / 2 + 1
    }

    fn members(&self) -> Vec<ReplicaIdx> {
        let mut members = vec![];
        for i in 0..self.resolver.len() {
            members.push(ReplicaIdx(i));
        }
        members
    }

    fn supports(
        &self,
        log: &EventHistory<ReplicaIdx>,
        candidate_version: &Version,
        r: ReplicaIdx,
    ) -> bool {
        // An event becomes ready to be committed if it gathers support from "enough" events around its past and future
        // The causal region of an event e is comprised of its immediate past (the latest events from each replica seen by e),
        // its immediate future (the earliest events from each replica to see e), and vertices "in between".

        let previous = log.previous(candidate_version, r);
        let next = log.next(&EventId::from(candidate_version), r);

        // A vote is support only once both sides of the candidate's causal
        // region have been observed. In particular, a previous vote without a
        // `next` event is still pending: extending the history may reveal that
        // the replica changed its vote before it observed the candidate.
        let (Some(first), Some(last)) = (previous, next) else {
            return false;
        };

        let first = first.id().seq() - 1;
        let last = last.id().seq() - 1;

        log.store[&r][first..=last]
            .iter()
            .all(|(event, _)| *event.op() == candidate_version.origin_idx())
    }

    pub fn leaders<'a>(
        &self,
        log: &'a EventHistory<ReplicaIdx>,
    ) -> Vec<&'a (TaggedOp<ReplicaIdx>, Version)> {
        let members = self.members();
        let quorum = self.quorum();

        log.store
            .values()
            .flat_map(|events| events.iter())
            .filter(|e| {
                members
                    .iter()
                    .filter(|&&r| self.supports(log, &e.1, r))
                    .take(quorum)
                    .count()
                    >= quorum
            })
            .collect()
    }

    pub fn anchor<'a>(
        &self,
        leaders: &[&'a (TaggedOp<ReplicaIdx>, Version)],
    ) -> Option<&'a Version> {
        leaders
            .iter()
            .copied()
            .find(|(_, candidate_version)| {
                leaders
                    .iter()
                    .all(|(leader, _)| leader.id().is_predecessor_of(candidate_version))
            })
            .map(|(_, version)| version)
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
