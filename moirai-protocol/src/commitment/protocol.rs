use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    event::Event,
    state::{effect_context::EffectContext, log::IsLog},
};

/// Commitment over an immutable, fully configured local member index order.
/// Events must already have been internalized by TCSB before delivery here.
pub trait AtomicBroadcastProtocol<L>: Default
where
    L: IsLog,
{
    /// Replicated operation, including protocol metadata.
    type Update: std::fmt::Debug + Clone;

    /// Annotate an operation prepared by the child log.
    fn ab_prepare(&self, op: L::Update) -> Self::Update;

    /// Access the child operation inside a replicated operation.
    fn child_op(op: &Self::Update) -> &L::Update;

    fn is_default(&self) -> bool;

    /// Commitment protocol handler of events
    fn ab_handler(&mut self, event: Event<Self::Update>, ctx: &mut EffectContext<'_>);

    /// Returns checkpoints ready to commit, in commit sequence order
    fn ab_ready_to_commit(&self) -> Vec<&Version>;

    /// Prune protocol metadata using a committed checkpoint
    fn ab_prune(&mut self, event_version: &Version);

    /// The latest commitment frontier, if commitment has advanced.
    fn ab_last_committed(&self) -> Option<&CausalFrontier>;

    fn ab_set_last_committed(&mut self, event_version: &Version);
}
