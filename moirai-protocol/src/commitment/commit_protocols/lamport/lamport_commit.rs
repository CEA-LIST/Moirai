use crate::{
    broadcast::internalizer::Resolver,
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    commitment::{
        commit_protocols::lamport::lamport_tracker::LamportTracker,
        protocol::AtomicBroadcastProtocol, wire_id::StableReplicaId,
    },
    replica::{ReplicaId, ReplicaIdx},
    state::{effect_context::EffectContext, log::IsLog},
};

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct LamportCommit {
    system_members: Vec<(StableReplicaId, ReplicaId)>,
    lamport_tracker: LamportTracker,
}

impl<L> AtomicBroadcastProtocol<L> for LamportCommit
where
    L: IsLog,
{
    type Update = L::Update;

    fn child_op(op: &Self::Update) -> &L::Update {
        op
    }

    fn is_default(&self) -> bool {
        match self.lamport_tracker.get_resolver() {
            Some(_) => false,
            None => true,
        }
    }

    fn ab_prepare(&self, op: L::Update) -> Self::Update {
        self.assert_configured();
        op
    }

    fn ab_handler(
        &mut self,
        event: crate::event::Event<Self::Update>,
        _ctx: &mut EffectContext<'_>,
    ) {
        self.assert_configured();
        self.lamport_tracker.update_timestamp(event.version());
    }

    fn ab_ready_to_commit(&self, resolver: &Resolver) -> Vec<&Version> {
        let members = self.local_members(resolver);
        self.lamport_tracker.ready_to_commit(&members)
    }

    fn ab_prune(&mut self, _: &Version) {}

    fn ab_last_committed(&self) -> Option<&CausalFrontier> {
        self.lamport_tracker.last_committed()
    }

    fn ab_set_last_committed(&mut self, version: &Version) {
        self.lamport_tracker.set_committed(version);
    }

    fn get_resolver(&self) -> Option<&Resolver> {
        self.lamport_tracker.get_resolver()
    }

    fn update_resolver(&mut self, resolver: Resolver) {
        self.lamport_tracker.update_resolver(resolver);
    }
}

impl LamportCommit {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a protocol with the supplied timestamp tracker and fixed membership.
    pub fn from_parts(
        system_members: Vec<(StableReplicaId, ReplicaId)>,
        lamport_tracker: LamportTracker,
    ) -> Self {
        Self {
            system_members,
            lamport_tracker,
        }
    }

    pub fn get_members(&self) -> &Vec<(StableReplicaId, ReplicaId)> {
        &self.system_members
    }

    pub fn get_mut_members(&mut self) -> &mut Vec<(StableReplicaId, ReplicaId)> {
        &mut self.system_members
    }

    pub fn set_members(&mut self, system_members: Vec<(StableReplicaId, ReplicaId)>) {
        self.system_members = system_members;
    }

    fn assert_configured(&self) {
        assert!(
            !self.system_members.is_empty(),
            "Use from_stable/from_parts and Replica::bootstrap_with_state"
        );
    }

    fn local_members(&self, resolver: &Resolver) -> Vec<ReplicaIdx> {
        assert_eq!(
            resolver.len(),
            self.system_members.len(),
            "TCSB and commitment membership differ"
        );
        self.system_members
            .iter()
            .map(|(_, id)| {
                resolver
                    .get(id)
                    .expect("TCSB and commitment membership differ")
            })
            .collect()
    }
}
