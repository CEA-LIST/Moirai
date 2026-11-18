use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version}, commitment::{protocol::AtomicBroadcastProtocol}, crdt::{eval::EvalNested, query::QueryOperation}, event::Event, state::{
        effect_context::EffectContext, log::IsLog,
    },
};

/// Special log that wraps a child log and provides a commitment protocol on top of it.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct CommitmentLog<L, A>
where
    L: IsLog,
    A: AtomicBroadcastProtocol<L>
{
    /// Child log. It can contain any CRDT log, including composite ones.
    child: L,
    /// Commitment protocol state
    ab_protocol: A,
}

impl<L, A> IsLog for CommitmentLog<L, A>
where
    L: IsLog,
    A: AtomicBroadcastProtocol<L>
{
    type Command = L::Command;
    /// The CommitmentLog is not a pure CRDT log.
    /// The child CRDT update is prepared to be annotated with a leader vote.
    type Update = A::Update;
    type Rejection = L::Rejection;

    /// A user command is prepared by the child log, and then annotated with the current leader vote from the oracle.
    fn prepare(&self, command: Self::Command) -> Self::Update {
        self.ab_protocol.ab_prepare(self.child.prepare(command))
    }

    fn is_enabled(&self, op: &Self::Update) -> Result<(), Self::Rejection> {
        self.child.is_enabled(A::child_op(op))
    }

    fn effect(&mut self, event: Event<Self::Update>, ctx: &mut EffectContext<'_>) {
        // We update the resolver in the protocol to ensure that we can resolve
        // replica IDs correctly for the leader votes (in the case new replicas have joined
        // since the last event).
        self.ab_protocol.update_resolver(event.version().resolver().clone());

        let child_op = A::child_op(event.op()).clone();
        let update_event = event.clone().unfold(child_op);

        // Dispatch the child update to its log, and the event to the atomic broadcast protocol.
        self.child.effect(update_event, ctx);
        self.ab_protocol.ab_handler(event, ctx);

        // Check new event candidates for commitment
        let ready: Vec<_> = self.ab_protocol
            .ab_ready_to_commit(self.ab_protocol.get_resolver().unwrap())
            .into_iter().cloned().collect();

        // Iterate through each candidate to commit
        for version in ready {
            let frontier = CausalFrontier::from(&version);
            self.child.stabilize(&frontier);
            self.ab_protocol.ab_prune(&version);
            self.ab_protocol.ab_set_last_committed(&version);
        }
    }

    fn stabilize(&mut self, _: &CausalFrontier) {
        // Causal stability is not compatible with majority commitment, so we don't stabilize the log.
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.child.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.child.is_default() && self.ab_protocol.is_default()
    }
}

impl<L, A> CommitmentLog<L, A>
where
    L: IsLog,
    A: AtomicBroadcastProtocol<L>,
{
    pub fn new(child: L, ab_protocol: A) -> Self {
        Self {
            child,
            ab_protocol
        }
    }

    pub const fn child(&self) -> &L {
        &self.child
    }

    /// The latest commitment frontier, if commitment has advanced.
    pub fn committed_frontier(&self) -> Option<&CausalFrontier> {
        self.ab_protocol.ab_last_committed()
    }

    /// Read-only access to commitment state, including candidate support.
    pub const fn protocol(&self) -> &A {
        &self.ab_protocol
    }

    /// Returns the frontier representing the strict causal past of the given frontier.
    pub fn strict_causal_past(frontier: &CausalFrontier) -> CausalFrontier {
        let mut new_frontier = CausalFrontier::new(frontier.resolver().clone());

        for (idx, value) in frontier.iter() {
            if value > 0 {
                new_frontier.set_by_idx(idx, value - 1);
            }
        }

        new_frontier
    }
}

impl<L, A, Q> EvalNested<Q> for CommitmentLog<L, A>
where
    L: IsLog + EvalNested<Q>,
    A: AtomicBroadcastProtocol<L>,
    Q: QueryOperation,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        self.child.execute_query(q)
    }
}

impl<L, A> Default for CommitmentLog<L, A>
where
    L: IsLog,
    A: AtomicBroadcastProtocol<L>
{
    fn default() -> Self {
        Self {
            child: L::default(),
            ab_protocol: A::default(),
        }
    }
}