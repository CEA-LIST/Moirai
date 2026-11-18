use crate::{
    broadcast::internalizer::Resolver,
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    commitment::{commit_protocols::omega::{oracle::IsOracle, support_op::SupportOp, versioned_vote::VersionedVote, vote_log::VoteLog, vote_origin::VoteOrigin}, protocol::AtomicBroadcastProtocol, wire_id::{StableEventId, StableReplicaId}},
    replica::{ReplicaId, ReplicaIdx},
    state::{effect_context::EffectContext, log::IsLog},
    event::Event,
};

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct OmegaCommit<O>
where
    O: IsOracle
{
    oracle: O,
    vote_log: VoteLog,
    committed_frontier: Option<CausalFrontier>,
    system_members: Vec<(StableReplicaId, ReplicaId)>,
    own_id: StableReplicaId,
}

impl <L, O> AtomicBroadcastProtocol<L> for OmegaCommit<O>
where 
    L: IsLog,
    O: IsOracle
{
    type Update = SupportOp<L::Update>;

    fn child_op(op: &Self::Update) -> &L::Update {
        &op.op
    }

    fn is_default(&self) -> bool {
        self.vote_log.is_empty()
    }

    fn ab_prepare(&self, op: L::Update) -> Self::Update {
        self.assert_configured();
        let (is_partial, last_partial) = if let Some(resolver) = self.vote_log.resolver() {
            let members = self.local_members(resolver);
            let own_idx = self.local_id(self.own_id, resolver);
            let (partial, previous) = self.vote_log.is_potential(own_idx, &members);
            let previous = previous.map(|id| {
                let origin = resolver.resolve(id.id()).expect("Unknown checkpoint origin");
                let (stable_id, _) = self.system_members.iter().find(|(_, member)| member == origin)
                    .expect("Checkpoint origin is not a configured member");
                StableEventId::new(*stable_id, u64::try_from(id.seq()).expect("Event sequence exceeds wire ID range"))
            });
            (partial, previous)
        } else {
            (false, None)
        };
        SupportOp::new(op, self.oracle.query(), is_partial, last_partial)
    }

    fn ab_handler(&mut self, event: Event<Self::Update>, _ctx: &mut EffectContext<'_>) {
        self.assert_configured();
        let event = self.oracle.update(event);
        let resolver = event.version().resolver();
        let (_, vote, is_partial, last_partial) = event.op().clone().split();
        let vote = self.local_id(vote, resolver);
        let last_partial = last_partial.map(|id| {
            assert!(id.seq > 0, "Invalid checkpoint ID");
            let origin = self.local_id(id.origin, resolver);
            let seq = usize::try_from(id.seq).expect("Checkpoint sequence exceeds local range");
            let previous = event.version().seq_by_idx(origin);
            assert!(
                seq <= previous && (origin != event.version().origin_idx() || seq < previous),
                "Checkpoint must be in the event's strict causal past"
            );
            VoteOrigin::new(origin, seq)
        });
        let vertex = VersionedVote::new(vote, event.version().clone(), is_partial, last_partial);
        self.vote_log.insert(vertex);
    }

    fn ab_ready_to_commit(&self, resolver: &Resolver) -> Vec<&Version> {
        let members = self.local_members(resolver);
        self.vote_log.ready_to_commit(&members)
    }

    fn ab_prune(&mut self, event_version: &Version) {
        let meta_frontier = self.vote_log.get_causal_frontier(event_version);
        self.vote_log.prune(&meta_frontier);
    }

    fn ab_last_committed(&self) -> Option<&CausalFrontier> {
        self.committed_frontier.as_ref()
    }

    fn ab_set_last_committed(&mut self, event_version: &Version) {
        self.vote_log.set_committed(event_version);
        self.committed_frontier = self.vote_log.last_committed.as_ref()
            .map(|checkpoint| CausalFrontier::from(checkpoint.version()));
    }

    fn get_resolver(&self) -> Option<&Resolver> {
        self.vote_log.resolver()
    }

    fn update_resolver(&mut self, resolver: Resolver) {
        self.vote_log.update_resolver(resolver);
    }
}

impl <O> OmegaCommit<O>
where
    O: IsOracle
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a protocol with an empty vote log and the supplied configuration.
    pub fn from_parts(
        mut oracle: O,
        system_members: Vec<(StableReplicaId, ReplicaId)>,
        own_id: StableReplicaId,
    ) -> Self {
        oracle.configure_members(&system_members);
        Self {
            oracle,
            vote_log: VoteLog::default(),
            committed_frontier: None,
            system_members,
            own_id,
        }
    }

    pub fn get_oracle(&self) -> &O {
        &self.oracle
    }

    pub fn get_mut_oracle(&mut self) -> &mut O {
        &mut self.oracle
    }

    pub fn get_own_id(&self) -> StableReplicaId {
        self.own_id
    }

    pub fn set_oracle(&mut self, mut oracle: O) {
        oracle.configure_members(&self.system_members);
        self.oracle = oracle;
    }

    pub fn set_own_id(&mut self, own_id: StableReplicaId) {
        self.own_id = own_id;
    }

    pub fn get_members(&self) -> &Vec<(StableReplicaId, ReplicaId)> {
        &self.system_members
    }

    pub fn get_mut_members(&mut self) -> &mut Vec<(StableReplicaId, ReplicaId)> {
        &mut self.system_members
    }

    pub fn set_members(&mut self, system_members: Vec<(StableReplicaId, ReplicaId)>) {
        self.oracle.configure_members(&system_members);
        self.system_members = system_members;
    }

    fn assert_configured(&self) {
        assert!(!self.system_members.is_empty(), "Use from_stable/from_parts and Replica::bootstrap_with_state");
        assert!(self.system_members.iter().any(|(id, _)| *id == self.own_id), "Own ID must be a configured member");
        assert!(self.system_members.iter().any(|(id, _)| *id == self.oracle.query()), "Oracle must name a configured member");
    }

    fn local_members(&self, resolver: &Resolver) -> Vec<ReplicaIdx> {
        assert_eq!(resolver.len(), self.system_members.len(), "TCSB and commitment membership differ");
        self.system_members.iter().map(|(_, id)| {
            resolver.get(id).expect("TCSB and commitment membership differ")
        }).collect()
    }

    fn local_id(&self, stable: StableReplicaId, resolver: &Resolver) -> ReplicaIdx {
        let (_, replica) = self.system_members.iter().find(|(id, _)| *id == stable)
            .expect("Unknown stable replica ID");
        resolver.get(replica).expect("TCSB and commitment membership differ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{broadcast::internalizer::Interner,
        commitment::{commit_protocols::omega::oracle::Omega, examples::commit_set::CommitSet},
        state::po_log::VecLog};

    type Log = VecLog<CommitSet<u8>>;

    #[test]
    fn committed_frontier_tracks_the_full_checkpoint() {
        let mut protocol = OmegaCommit::<Omega>::default();
        assert!(<OmegaCommit<Omega> as AtomicBroadcastProtocol<Log>>::ab_last_committed(&protocol).is_none());
        let mut interner = Interner::new();
        let (origin, _) = interner.intern(&ReplicaId::from("A"));
        let mut version = Version::new(origin, interner.resolver().clone());
        version.set_by_idx(origin, 1);
        protocol.vote_log.insert(VersionedVote::new(origin, version.clone(), true, None));
        <OmegaCommit<Omega> as AtomicBroadcastProtocol<Log>>::ab_set_last_committed(&mut protocol, &version);
        assert_eq!(
            <OmegaCommit<Omega> as AtomicBroadcastProtocol<Log>>::ab_last_committed(&protocol),
            Some(&CausalFrontier::from(&version)),
        );
    }
}
