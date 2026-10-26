use crate::{
    broadcast::internalizer::Resolver,
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    commitment::{
        commit_op::CommitOp,
        leader_vote::{LeaderVote, NewSupports},
        oracle::{IsOracle, Omega},
        protocol::CommitmentProtocol,
    },
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    state::{
        effect_context::EffectContext,
        log::IsLog,
        po_log::POLog,
        unstable_state::{IsUnstableCausal, event_history::EventHistory},
    },
};

/// Special log that wraps a child log and adds commitment semantics.
/// It maintains a separate log of leader votes, and uses a commitment protocol
#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct CommitmentLog<L> {
    /// Child log. It can contain any CRDT log, including composite ones.
    child: L,
    /// Log of leader votes
    leader_log: POLog<LeaderVote, EventHistory<LeaderVote>>,
    /// Commitment protocol state
    protocol: CommitmentProtocol<Omega>,
}

impl<L> IsLog for CommitmentLog<L>
where
    L: IsLog,
{
    type Command = L::Command;
    type Op = CommitOp<L::Op>;
    type Rejection = L::Rejection;

    /// A user command is prepared by the child log, and then annotated with the current leader vote from the oracle.
    fn prepare(&self, command: Self::Command) -> Self::Op {
        CommitOp::new(
            self.child.prepare(command),
            LeaderVote::Vote(self.protocol.oracle().query().unwrap()),
        )
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        self.child.is_enabled(&op.op)
    }

    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        // We update the resolver in the protocol to ensure that we can resolve
        // replica IDs correctly for the leader votes (in the case new replicas have joined
        // since the last event).
        self.protocol
            .update_resolver(event.version().resolver().clone());

        let (op, vote) = event.op().clone().split();

        let update_event = event.clone().unfold(op);
        let vote_event = event.clone().unfold(vote);
        let vote_event_id = vote_event.id().clone();

        // Dispatch the child update to its log, and the leader vote to the leader log.
        self.child.effect(update_event, ctx);
        self.leader_log.effect(vote_event, ctx);

        // Compute the new supports
        let deltas = self.leader_log.execute_query(&NewSupports::new(
            vote_event_id,
            self.protocol.last_committed().cloned(),
        ));

        // Gather the new anchors
        let candidate_anchors = self.protocol.apply_support_deltas(deltas);

        // Find the greatest candidate anchor, if any.
        let greatest_candidate = candidate_anchors
            .iter()
            .filter_map(|anchor| self.leader_log.unstable().version(anchor))
            .max_by(|a, b| {
                a.partial_cmp(b)
                    .expect("candidate versions must be comparable")
            })
            .cloned();

        // Only advance commitment when a real candidate exists.
        if let Some(candidate) = greatest_candidate
            && self.protocol.advance_commitment(&candidate)
        {
            let frontier = CausalFrontier::from(&candidate);

            self.protocol
                .prune_support(&frontier, self.leader_log.unstable());

            self.leader_log
                .stabilize(&CommitmentLog::<L>::strict_past_frontier(frontier));
        }
    }

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        self.child.stabilize(frontier);
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.child.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.child.is_default() && self.leader_log.is_default()
    }
}

impl<L> CommitmentLog<L>
where
    L: IsLog,
{
    pub fn new(child: L, oracle: Omega, resolver: Resolver) -> Self {
        Self {
            child,
            leader_log: POLog::default(),
            protocol: CommitmentProtocol::new(oracle, resolver),
        }
    }

    pub fn oracle_mut(&mut self) -> &mut Omega {
        self.protocol.oracle_mut()
    }

    pub fn child(&self) -> &L {
        &self.child
    }

    /// The latest majority-backed commitment frontier, if commitment has advanced.
    pub fn committed_frontier(&self) -> Option<&CausalFrontier> {
        self.protocol.last_committed()
    }

    /// Read-only access to the log of leader votes and their causal history.
    pub fn leader_log(&self) -> &POLog<LeaderVote, EventHistory<LeaderVote>> {
        &self.leader_log
    }

    /// Read-only access to commitment state, including candidate support.
    pub fn protocol(&self) -> &CommitmentProtocol<Omega> {
        &self.protocol
    }

    pub fn strict_past_frontier(frontier: CausalFrontier) -> CausalFrontier {
        let mut new_frontier = CausalFrontier::new(frontier.resolver().clone());

        for (idx, value) in frontier.iter() {
            if value > 0 {
                new_frontier.set_by_idx(idx, value - 1);
            }
        }

        new_frontier
    }
}

impl<L, Q> EvalNested<Q> for CommitmentLog<L>
where
    L: IsLog + EvalNested<Q>,
    Q: QueryOperation,
{
    fn execute_query(&self, q: &Q) -> Q::Response {
        self.child.execute_query(q)
    }
}

impl<L> Default for CommitmentLog<L>
where
    L: IsLog,
{
    fn default() -> Self {
        Self {
            child: L::default(),
            leader_log: POLog::default(),
            protocol: CommitmentProtocol::default(),
        }
    }
}
