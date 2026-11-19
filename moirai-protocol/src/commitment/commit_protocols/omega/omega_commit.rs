use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    commitment::{
        commit_protocols::omega::{
            oracle::IsOracle, support_op::SupportOp, versioned_vote::VersionedVote,
            vote_log::VoteLog, vote_origin::VoteOrigin,
        },
        protocol::AtomicBroadcastProtocol,
        wire_id::{StableEventId, StableReplicaId},
    },
    event::Event,
    replica::{ReplicaId, ReplicaIdx},
    state::{effect_context::EffectContext, log::IsLog},
};
use rustc_hash::{FxHashMap, FxHashSet};

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct OmegaCommit<O>
where
    O: IsOracle,
{
    oracle: O,
    vote_log: VoteLog,
    committed_frontier: Option<CausalFrontier>,
    system_members: Vec<(StableReplicaId, ReplicaId)>,
    local_indices: FxHashMap<StableReplicaId, ReplicaIdx>,
    member_indices: Vec<ReplicaIdx>,
    name_order: Vec<usize>,
    own_id: StableReplicaId,
}

impl<L, O> AtomicBroadcastProtocol<L> for OmegaCommit<O>
where
    L: IsLog,
    O: IsOracle,
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
        let (is_partial, previous) = self
            .vote_log
            .is_potential(ReplicaIdx(0), &self.member_indices);
        let last_partial = previous.map(|id| {
            StableEventId::new(
                self.system_members[id.id().0].0,
                u64::try_from(id.seq()).expect("Event sequence exceeds wire ID range"),
            )
        });
        SupportOp::new(op, self.oracle.query(), is_partial, last_partial)
    }

    fn ab_handler(&mut self, event: Event<Self::Update>, _ctx: &mut EffectContext<'_>) {
        self.assert_configured();
        assert_eq!(
            event.version().iter().count(),
            self.system_members.len(),
            "Fixed membership changed"
        );
        let event = self.oracle.update(event);
        let (_, vote, is_partial, last_partial) = event.op().clone().split();
        let vote = self.local_id(vote);
        let last_partial = last_partial.map(|id| {
            assert!(id.seq > 0, "Invalid checkpoint ID");
            let origin = self.local_id(id.origin);
            let seq = usize::try_from(id.seq).expect("Checkpoint sequence exceeds local range");
            let previous = event.version().seq_by_idx(origin);
            assert!(
                seq <= previous && (origin != event.version().origin_idx() || seq < previous),
                "Checkpoint must be in the event's strict causal past"
            );
            VoteOrigin::new(origin, seq)
        });
        let origin_order = self.name_order[event.version().origin_idx().0];
        let vertex = VersionedVote::new(
            vote,
            event.version().clone(),
            is_partial,
            last_partial,
            origin_order,
        );
        self.vote_log.insert(vertex);
    }

    fn ab_ready_to_commit(&self) -> Vec<&Version> {
        self.vote_log.ready_to_commit(&self.member_indices)
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
        self.committed_frontier = self
            .vote_log
            .last_committed
            .as_ref()
            .map(|checkpoint| CausalFrontier::from(checkpoint.version()));
    }
}

impl<O> OmegaCommit<O>
where
    O: IsOracle,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure fixed membership once. Pass the same member order to TCSB's
    /// `Replica::bootstrap_with_state`; both put the local member at index zero.
    pub fn from_parts(
        mut oracle: O,
        mut system_members: Vec<(StableReplicaId, ReplicaId)>,
        own_id: StableReplicaId,
    ) -> Self {
        assert!(
            !system_members.is_empty(),
            "Commitment requires nonempty fixed membership"
        );
        let mut stable_ids = FxHashSet::default();
        let mut names = FxHashSet::default();
        for (id, name) in &system_members {
            assert!(stable_ids.insert(*id), "Duplicate stable member ID");
            assert!(names.insert(name), "Duplicate replica ID");
        }
        let own = system_members
            .iter()
            .position(|(id, _)| *id == own_id)
            .expect("Own ID must be a configured member");
        let own = system_members.remove(own);
        system_members.insert(0, own);
        let local_indices: FxHashMap<_, _> = system_members
            .iter()
            .enumerate()
            .map(|(idx, (id, _))| (*id, ReplicaIdx(idx)))
            .collect();
        let member_indices: Vec<_> = (0..system_members.len()).map(ReplicaIdx).collect();
        let mut by_name = member_indices.clone();
        by_name.sort_unstable_by(|a, b| system_members[a.0].1.cmp(&system_members[b.0].1));
        let mut name_order = vec![0; system_members.len()];
        for (rank, idx) in by_name.into_iter().enumerate() {
            name_order[idx.0] = rank;
        }
        oracle.configure_members(&system_members.iter().map(|(id, _)| *id).collect::<Vec<_>>());
        assert!(
            local_indices.contains_key(&oracle.query()),
            "Oracle must name a configured member"
        );
        Self {
            oracle,
            vote_log: VoteLog::default(),
            committed_frontier: None,
            system_members,
            local_indices,
            member_indices,
            name_order,
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
        oracle.configure_members(
            &self
                .system_members
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
        );
        assert!(
            self.local_indices.contains_key(&oracle.query()),
            "Oracle must name a configured member"
        );
        self.oracle = oracle;
    }

    /// Fixed membership in local TCSB index order, with this replica first.
    pub fn get_members(&self) -> &[(StableReplicaId, ReplicaId)] {
        &self.system_members
    }

    fn assert_configured(&self) {
        assert!(
            !self.system_members.is_empty(),
            "Use from_stable/from_parts and Replica::bootstrap_with_state"
        );
        assert!(
            self.local_indices.contains_key(&self.oracle.query()),
            "Oracle must name a configured member"
        );
    }

    fn local_id(&self, stable: StableReplicaId) -> ReplicaIdx {
        *self
            .local_indices
            .get(&stable)
            .expect("Unknown stable replica ID")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        broadcast::internalizer::Interner,
        commitment::{
            commit_log::CommitmentLog, commit_protocols::omega::oracle::Omega,
            examples::commit_set::CommitSet,
        },
        state::po_log::VecLog,
    };

    type Log = VecLog<CommitSet<u8>>;

    #[test]
    fn committed_frontier_tracks_the_full_checkpoint() {
        let mut protocol = OmegaCommit::<Omega>::default();
        assert!(
            <OmegaCommit<Omega> as AtomicBroadcastProtocol<Log>>::ab_last_committed(&protocol)
                .is_none()
        );
        let mut interner = Interner::new();
        let (origin, _) = interner.intern(&ReplicaId::from("A"));
        let mut version = Version::new(origin, interner.resolver().clone());
        version.set_by_idx(origin, 1);
        protocol
            .vote_log
            .insert(VersionedVote::new(origin, version.clone(), true, None, 0));
        <OmegaCommit<Omega> as AtomicBroadcastProtocol<Log>>::ab_set_last_committed(
            &mut protocol,
            &version,
        );
        assert_eq!(
            <OmegaCommit<Omega> as AtomicBroadcastProtocol<Log>>::ab_last_committed(&protocol),
            Some(&CausalFrontier::from(&version)),
        );
    }

    #[test]
    fn fixed_tables_match_tcsb_with_sparse_ids_and_unsorted_names() {
        use crate::{
            broadcast::tcsb::Tcsb,
            replica::{IsReplica, Replica},
        };
        let members = vec![
            (StableReplicaId(42), ReplicaId::from("zulu")),
            (StableReplicaId(7), ReplicaId::from("middle")),
            (StableReplicaId(u16::MAX), ReplicaId::from("alpha")),
        ];
        let ids: Vec<_> = members.iter().map(|(_, id)| id.clone()).collect();
        for (own, name) in &members {
            let protocol = OmegaCommit::from_parts(Omega::with_leader(*own), members.clone(), *own);
            let node = Replica::<CommitmentLog<Log, OmegaCommit<Omega>>, Tcsb<SupportOp<_>>>::bootstrap_with_state(
                name.clone(), &ids, CommitmentLog::new(Log::default(), protocol.clone()));
            let since = node.since();
            for (idx, (stable, name)) in protocol.get_members().iter().enumerate() {
                assert_eq!(protocol.local_id(*stable), ReplicaIdx(idx));
                // Verify the unchanged clock metadata at the TCSB boundary.
                assert_eq!(since.resolver().resolve(ReplicaIdx(idx)), Some(name));
            }
            assert_eq!(protocol.local_id(*own), ReplicaIdx(0));
            assert_eq!(
                protocol.name_order[protocol.local_id(StableReplicaId(u16::MAX)).0],
                0
            );
            assert_eq!(
                protocol.name_order[protocol.local_id(StableReplicaId(7)).0],
                1
            );
            assert_eq!(
                protocol.name_order[protocol.local_id(StableReplicaId(42)).0],
                2
            );
        }
    }

    #[test]
    fn event_oracle_uses_the_fixed_local_order_before_and_after_replacement() {
        use super::super::oracle::EventOracle;
        use crate::{
            broadcast::tcsb::Tcsb,
            replica::{IsReplica, Replica},
        };
        let members = vec![
            (StableReplicaId(9), ReplicaId::from("A")),
            (StableReplicaId(3), ReplicaId::from("B")),
        ];
        let mut protocol = OmegaCommit::from_parts(
            EventOracle::with_leader(StableReplicaId(9)),
            members,
            StableReplicaId(3),
        );
        let mut node = Replica::<CommitmentLog<Log, OmegaCommit<EventOracle>>, Tcsb<SupportOp<_>>>::bootstrap_with_state(
            "B".into(), &["A", "B"], CommitmentLog::new(Log::default(), protocol.clone()));
        let message = node
            .send(crate::commitment::examples::commit_set::SetUpdate::Add(7))
            .unwrap();
        assert_eq!(message.event().op().vote, StableReplicaId(9));
        // Replaying only the immutable event through a freshly configured oracle
        // must observe local index zero as B (stable ID 3), rather than A.
        protocol.get_mut_oracle().update(message.event().clone());
        assert_eq!(protocol.get_oracle().query(), StableReplicaId(3));
        protocol.set_oracle(EventOracle::with_leader(StableReplicaId(9)));
        assert_eq!(protocol.get_oracle().query(), StableReplicaId(9));
        protocol.get_mut_oracle().update(message.event().clone());
        assert_eq!(protocol.get_oracle().query(), StableReplicaId(3));
    }
}
