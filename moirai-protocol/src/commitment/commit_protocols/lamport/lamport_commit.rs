use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    commitment::{
        commit_protocols::lamport::lamport_tracker::LamportTracker,
        protocol::AtomicBroadcastProtocol,
    },
    state::{effect_context::EffectContext, log::IsLog},
};

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct LamportCommit {
    lamport_tracker: LamportTracker,
}

impl<L: IsLog> AtomicBroadcastProtocol<L> for LamportCommit {
    type Update = L::Update;

    fn child_op(op: &Self::Update) -> &L::Update {
        op
    }

    fn is_default(&self) -> bool {
        self.lamport_tracker.get_stable_frontier().is_none()
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

    fn ab_ready_to_commit(&self) -> Vec<&Version> {
        self.lamport_tracker.ready_to_commit()
    }

    fn ab_prune(&mut self, _: &Version) {}

    fn ab_last_committed(&self) -> Option<&CausalFrontier> {
        self.lamport_tracker.last_committed()
    }

    fn ab_set_last_committed(&mut self, version: &Version) {
        self.lamport_tracker.set_committed(version);
    }
}

impl LamportCommit {
    /// The count must include every member of the fixed TCSB membership.
    pub fn new(member_count: usize) -> Self {
        Self {
            lamport_tracker: LamportTracker::new(member_count),
        }
    }

    fn assert_configured(&self) {
        // The configured tracker has one queue for every immutable local index.
        assert!(
            self.lamport_tracker.member_count() > 0,
            "Use LamportCommit::new(member_count) and Replica::bootstrap_with_state"
        );
    }
}
