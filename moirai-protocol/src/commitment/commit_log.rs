use std::cmp::Ordering;

use crate::{
    broadcast::internalizer::Resolver,
    clock::version_vector::Version,
    commitment::{
        commit_op::CommitOp,
        oracle::{IsOracle, Omega},
        protocol::CommitmentProtocol,
    },
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    replica::ReplicaIdx,
    state::{
        effect_context::EffectContext,
        log::IsLog,
        unstable_state::{IsUnstableCore, event_history::EventHistory},
    },
};

/// Special log that wraps a child log and adds commitment semantics.
/// It maintains a separate log of leader votes, and uses a commitment protocol
/// to determine the new version boundary for which the child log can be stabilized.
#[derive(Debug, Clone)]
pub struct CommitmentLog<L> {
    /// Child log. It can contain any CRDT log, including composite ones.
    child: L,
    /// Log of leader votes
    leader_log: EventHistory<ReplicaIdx>,
    /// Commitment protocol state
    protocol: CommitmentProtocol<Omega>,
}

impl<L> IsLog for CommitmentLog<L>
where
    L: IsLog,
{
    type Value = L::Value;
    type Command = L::Command;
    type Op = CommitOp<L::Op>;
    type Rejection = L::Rejection;

    /// A user command is prepared by the child log, and then annotated with the current leader vote from the oracle.
    fn prepare(&self, command: Self::Command) -> Self::Op {
        CommitOp::new(
            self.child.prepare(command),
            self.protocol.oracle().query().unwrap(),
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

        let (op, leader) = event.op().clone().split();
        let leader = self.protocol.resolver().get(&leader).unwrap();

        let update_event = event.clone().unfold(op);
        let leader_event = event.unfold(leader);

        // Dispatch the child update to its log, and the leader vote to the leader log.
        self.child.effect(update_event, ctx);
        self.leader_log.append(leader_event);

        // After processing the event, we check if we can advance the last committed anchor.
        let leaders = self.protocol.leaders(&self.leader_log);

        // If we have a new anchor that is greater than the last committed anchor, we stabilize the child log at that anchor.
        if let Some(anchor) = self.protocol.anchor(&leaders) {
            let advances = match &self.protocol.last_committed {
                Some(last_committed) => {
                    anchor.partial_cmp(last_committed) == Some(Ordering::Greater)
                }
                None => true,
            };

            if advances {
                self.protocol.last_committed = Some(anchor.clone());
                self.child.stabilize(anchor);
            }
        }
    }

    fn stabilize(&mut self, version: &Version) {
        let advances = match &self.protocol.last_committed {
            Some(last_committed) => version.partial_cmp(last_committed) == Some(Ordering::Greater),
            None => true,
        };

        if advances {
            self.protocol.last_committed = Some(version.clone());
            self.child.stabilize(version);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.child.redundant_by_parent(version, conservative);
    }

    fn is_default(&self) -> bool {
        self.child.is_default() && self.leader_log.is_empty()
    }
}

impl<L> CommitmentLog<L>
where
    L: IsLog,
{
    pub fn new(child: L, oracle: Omega, resolver: Resolver) -> Self {
        Self {
            child,
            leader_log: EventHistory::default(),
            protocol: CommitmentProtocol::new(oracle, resolver),
        }
    }

    pub fn oracle_mut(&mut self) -> &mut Omega {
        self.protocol.oracle_mut()
    }

    pub fn child(&self) -> &L {
        &self.child
    }
}

impl<L, Q> EvalNested<Q> for CommitmentLog<L>
where
    L: IsLog + EvalNested<Q>,
    Q: QueryOperation,
{
    fn execute_query(&self, q: Q) -> Q::Response {
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
            leader_log: EventHistory::default(),
            protocol: CommitmentProtocol::default(),
        }
    }
}
