/////////////////////////////////////////////////////////////////////////////
//  Copyright (c) 2025, 2026 CEA LIST.
//
//  This program and the accompanying materials
//  are made available under the terms of the Apache License, Version 2.0
//  which accompanies this distribution, and is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Contributors:
// Léo Olivier -  (CEA LIST) - Initial implementation
//
/////////////////////////////////////////////////////////////////////////////

use crate::{
    broadcast::internalizer::Resolver,
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    commitment::{
        commit_op::CommitOp,
        leader_vote::{LeaderVote, LeaderVoteUpdate, NewSupports},
        oracle::{IsOracle, Omega},
        protocol::CommitmentProtocol,
    },
    data_type::{query::QueryOperation, replicated::ReplicatedDataType},
    event::Event,
    state::{
        effect_context::EffectContext,
        log::{EvalNested, IsLog},
        po_log::POLog,
        unstable_state::{IsUnstableCausal, event_history::EventHistory},
    },
};

/// Special log that wraps a child log and provides a commitment protocol on top of it.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct CommitmentLog<L> {
    /// Child log. It can contain any CRDT log, including composite ones.
    child: L,
    /// Log of leader votes
    leader_log: POLog<LeaderVote, EventHistory<<LeaderVote as ReplicatedDataType>::Update>>,
    /// Commitment protocol state
    protocol: CommitmentProtocol<Omega>,
}

impl<L> IsLog for CommitmentLog<L>
where
    L: IsLog,
{
    type Command = L::Command;
    /// The CommitmentLog is not a pure CRDT log.
    /// The child CRDT update is prepared to be annotated with a leader vote.
    type Update = CommitOp<L::Update>;
    type Rejection = L::Rejection;

    /// A user command is prepared by the child log, and then annotated with the current leader vote from the oracle.
    fn prepare(&self, command: Self::Command) -> Self::Update {
        CommitOp::new(
            self.child.prepare(command),
            LeaderVoteUpdate::Vote(self.protocol.oracle().query().unwrap()),
        )
    }

    fn is_enabled(&self, op: &Self::Update) -> Result<(), Self::Rejection> {
        self.child.is_enabled(&op.op)
    }

    fn effect(&mut self, event: Event<Self::Update>, ctx: &mut EffectContext<'_>) {
        // We update the resolver in the protocol to ensure that we can resolve
        // replica IDs correctly for the leader votes (in the case new replicas have joined
        // since the last event).
        self.protocol
            .update_resolver(event.version().resolver().clone());

        let (op, vote) = event.op().clone().split();

        let update_event = event.clone().unfold(op);
        let vote_event = event.unfold(vote);
        let vote_event_id = vote_event.id().clone();

        // Dispatch the child update to its log, and the leader vote to the leader log.
        self.child.effect(update_event, ctx);
        self.leader_log.effect(vote_event, ctx);

        // Compute the new supports
        let deltas = self.leader_log.execute_query(&NewSupports::new(
            vote_event_id,
            self.protocol.last_committed().cloned(),
        ));

        for anchor in self.protocol.apply_support_deltas(deltas) {
            let anchor_version = self.leader_log.unstable().version(&anchor).unwrap();
            let frontier = CausalFrontier::from(anchor_version);
            // Record the new latest commitment frontier in the protocol state
            self.protocol.advance_commitment(&frontier);

            // Prune the support table
            self.protocol
                .prune_support(&frontier, self.leader_log.unstable());

            // Prune the leader log to the strict causal past of the new commitment frontier
            self.leader_log
                .stabilize(&Self::strict_causal_past(&frontier));

            // Stabilize the child log to the new commitment frontier
            self.child.stabilize(&frontier);
        }
    }

    fn stabilize(&mut self, _: &CausalFrontier) {
        // Causal stability is not compatible with majority commitment, so we don't stabilize the log.
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

    pub const fn oracle_mut(&mut self) -> &mut Omega {
        self.protocol.oracle_mut()
    }

    pub const fn child(&self) -> &L {
        &self.child
    }

    /// The latest commitment frontier, if commitment has advanced.
    pub const fn committed_frontier(&self) -> Option<&CausalFrontier> {
        self.protocol.last_committed()
    }

    /// Read-only access to the log of leader votes and their causal history.
    pub const fn leader_log(
        &self,
    ) -> &POLog<LeaderVote, EventHistory<<LeaderVote as ReplicatedDataType>::Update>> {
        &self.leader_log
    }

    /// Read-only access to commitment state, including candidate support.
    pub const fn protocol(&self) -> &CommitmentProtocol<Omega> {
        &self.protocol
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
