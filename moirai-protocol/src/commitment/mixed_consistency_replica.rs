#![allow(clippy::mutable_key_type)]

use std::fmt::Debug;

use crate::{
    broadcast::{
        internalizer::Interner,
        message::{BatchMessage, EventMessage, SinceMessage},
        tcsb::IsTcsb,
    },
    commitment::{
        commit_log::CommitLog, commit_op::CommitOp, leader_votes::LeaderVotes, oracle::IsOracle,
    },
    crdt::{
        eval::EvalNested, policy::LwwPolicy, query::QueryOperation, sequential::SequentialDataType,
    },
    event::{Event, id::EventId},
    replica::{IsReplica, ReplicaId, ReplicaIdOwned, ReplicaIdx},
    state::{
        effect_context::EffectContext, log::IsLog, sink::SinkCollector,
        unstable_state::IsUnstableCore,
    },
    utils::hashmap::{HashMap, HashSet},
};

pub struct MixedConsistencyReplica<A, O, T>
where
    A: SequentialDataType,
    CommitOp<A::Update>: Debug,
    O: IsOracle,
    T: IsTcsb<CommitOp<A::Update>>,
{
    id: ReplicaIdOwned,
    tcsb: T,
    state: CommitLog<A>,
    oracle: O,
    /// # Commitment variables
    /// Each vertex is accompanied by a set of ids that validated it.
    /// Each id that is present in the map corresponds to a replica supporting the vertex.
    /// If the boolean value is true, the corresponding replica validated that vertex.
    /// If there are less than a quorum of ids in the map, the vertex is no longer a partial leader.
    potential_leaders: HashMap<EventId, LeaderVotes>,
    /// Keeps track of all the partial leaders in a vertex's cut.
    partial_leaders_hist: HashMap<EventId, HashSet<EventId>>,
    pre_committed: HashSet<EventId>,
    latest_committed: Option<EventId>,
}

impl<A, O, T> IsReplica<CommitLog<A>> for MixedConsistencyReplica<A, O, T>
where
    A: SequentialDataType,
    CommitOp<A::Update>: Debug,
    O: IsOracle,
    T: IsTcsb<CommitOp<A::Update>>,
{
    type Command = A::Update;
    type Payload = CommitOp<A::Update>;

    fn id(&self) -> &ReplicaId {
        &self.id
    }

    fn receive(&mut self, message: EventMessage<Self::Payload>) {
        self.tcsb.receive(message);
        while let Some(e) = self.tcsb.next_causally_ready() {
            self.deliver(e);
        }
    }

    fn receive_batch(&mut self, message: BatchMessage<Self::Payload>) {
        self.tcsb.receive_batch(message);
        while let Some(e) = self.tcsb.next_causally_ready() {
            self.deliver(e);
        }
    }

    fn since(&self) -> SinceMessage {
        self.tcsb.since()
    }

    fn send(&mut self, cmd: Self::Command) -> Result<EventMessage<Self::Payload>, A::Rejection> {
        // Prepare the command by adding the current leader
        let op = self.prepare(cmd);
        self.state.is_enabled(&op)?;
        let message = self.tcsb.send(op);
        self.deliver(message.event().clone());
        Ok(message)
    }

    fn pull(&mut self, since: SinceMessage) -> BatchMessage<Self::Payload> {
        self.tcsb.pull(since)
    }

    fn query<Q: QueryOperation>(&self, q: Q) -> Q::Response
    where
        CommitLog<A>: EvalNested<Q>,
    {
        self.state.eval(q)
    }
}

impl<A, O, T> MixedConsistencyReplica<A, O, T>
where
    A: SequentialDataType,
    CommitOp<A::Update>: Debug,
    O: IsOracle,
    T: IsTcsb<CommitOp<A::Update>>,
{
    pub fn bootstrap(id: ReplicaIdOwned, members: &[&ReplicaId], oracle: O) -> Self {
        Self::bootstrap_with_state(id, members, oracle, CommitLog::default())
    }

    pub fn bootstrap_with_state(
        id: ReplicaIdOwned,
        members: &[&ReplicaId],
        oracle: O,
        state: CommitLog<A>,
    ) -> Self {
        assert!(
            members.contains(&&(*id)),
            "Bootstrap replica ID {} must be included in members list {:?}",
            id,
            members
        );
        let mut interner = Interner::new();
        let (idx, _) = interner.intern(&id);
        for member in members {
            interner.intern(member);
        }
        Self {
            id,
            tcsb: T::new(idx, interner),
            state,
            oracle,
            potential_leaders: HashMap::default(),
            partial_leaders_hist: HashMap::default(),
            pre_committed: HashSet::default(),
            latest_committed: None,
        }
    }

    fn deliver(&mut self, event: Event<CommitOp<A::Update>>) {
        // TODO: needed for the log, but we don't use it here.
        // TODO: the ctx is used to track the effects of nested updates on composite CRDTs (creation, deletion of objects).
        let mut sink = SinkCollector::new();
        let mut ctx = EffectContext::root("root", Some(&mut sink));

        let delivered = event.id().clone();
        let leader = event.op().leader.clone();
        self.state.effect(event, &mut ctx);
        let predecessors = self.state.unstable().predecessor_set_by_id(&delivered);

        // Check potential leaders
        if let Some(votes) = self.is_potential_leader(&delivered, &predecessors) {
            let partial_leaders = self.partial_leaders();
            self.partial_leaders_hist.insert(
                delivered.clone(),
                partial_leaders.map(|(id, _)| id.clone()).collect(),
            );
            self.potential_leaders.insert(delivered.clone(), votes);
        }

        // Update the list of partial leaders
        self.update_leaders(&delivered, &leader, &predecessors);
        // Checking for commitment
        self.check_pre_committed();
        // Pruning the unstable state and committing the operations
        self.record();
    }

    /// Prepares a command for delivery by wrapping it in a `CommitOp` with the current oracle query.
    fn prepare(&self, cmd: A::Update) -> CommitOp<A::Update> {
        CommitOp::new(cmd, self.oracle.query().into())
    }

    pub fn oracle_mut(&mut self) -> &mut O {
        &mut self.oracle
    }

    pub fn state(&self) -> &CommitLog<A> {
        &self.state
    }

    fn is_potential_leader(
        &self,
        position: &EventId,
        predecessors: &HashSet<EventId>,
    ) -> Option<LeaderVotes> {
        let candidate = position.origin_id();
        let votes = self.votes(candidate, predecessors);

        votes.is_partial(self.quorum_size()).then_some(votes)
    }

    fn quorum_size(&self) -> usize {
        (self.tcsb.members_len() / 2) + 1
    }

    fn partial_leaders(&self) -> impl Iterator<Item = (&EventId, &LeaderVotes)> {
        let quorum_size = self.quorum_size();
        self.potential_leaders
            .iter()
            .filter(move |(_, votes)| votes.is_partial(quorum_size))
    }

    fn partial_leaders_mut(&mut self) -> impl Iterator<Item = (&EventId, &mut LeaderVotes)> {
        let quorum_size = self.quorum_size();
        self.potential_leaders
            .iter_mut()
            .filter(move |(_, votes)| votes.is_partial(quorum_size))
    }

    fn update_leaders(
        &mut self,
        delivered: &EventId,
        leader: &ReplicaId,
        predecessors: &HashSet<EventId>,
    ) {
        let sender = delivered.idx();

        for (candidate, votes) in self.partial_leaders_mut() {
            // A missing entry means this sender was not part of the candidate's
            // initial quorum. A `true` entry means it already validated it.
            if votes.is_missing_or_validated(sender) {
                continue;
            }

            if leader != candidate.origin_id() {
                // The sender now supports a different leader!
                // It no longer counts toward this candidate's potential quorum.
                votes.remove(sender);
            } else if predecessors.contains(candidate) {
                // The sender still supports this leader, the vote becomes validated.
                votes.validate(sender);
            }
        }
    }

    fn check_pre_committed(&mut self) {
        let quorum_size = self.quorum_size();
        let newly_pre_committed = self
            .partial_leaders()
            .filter(move |(_, votes)| votes.is_pre_committed(quorum_size))
            .map(|(event_id, _)| event_id.clone())
            .collect::<Vec<_>>();

        let mut worklist = Vec::new();
        for event_id in newly_pre_committed {
            if self.pre_committed.insert(event_id.clone()) {
                worklist.push(event_id);
            }
        }

        while let Some(event_id) = worklist.pop() {
            let Some(history) = self.partial_leaders_hist.get(&event_id) else {
                continue;
            };

            for leader in history {
                if self.pre_committed.insert(leader.clone()) {
                    worklist.push(leader.clone());
                }
            }
        }
    }

    fn record(&mut self) {
        let committed = self.state.commit_frontier::<LwwPolicy>(&self.pre_committed);

        if let Some(last_committed) = committed.last() {
            self.latest_committed = Some(last_committed.clone());
        }
        self.clear_committed_metadata(&committed);
    }

    fn clear_committed_metadata(&mut self, committed: &[EventId]) {
        for event_id in committed {
            self.pre_committed.remove(event_id);
            self.partial_leaders_hist.remove(event_id);
            self.potential_leaders.remove(event_id);
        }

        for history in self.partial_leaders_hist.values_mut() {
            for event_id in committed {
                history.remove(event_id);
            }
        }
    }

    fn votes(&self, candidate: &ReplicaId, predecessors: &HashSet<EventId>) -> LeaderVotes {
        let mut replica_vote = Vec::<Option<bool>>::new();

        for event_id in predecessors {
            if let Some(entry) = self.state.unstable().get(event_id) {
                let replica_idx = entry.id().idx();
                if replica_vote.len() <= replica_idx.0 {
                    replica_vote.resize(replica_idx.0 + 1, None);
                }

                let vote = entry.op().leader == candidate;
                replica_vote[replica_idx.0] =
                    Some(replica_vote[replica_idx.0].unwrap_or(true) && vote);
            }
        }

        LeaderVotes::from_pending_voters(
            replica_vote
                .into_iter()
                .enumerate()
                .filter_map(|(idx, voted)| {
                    voted.is_some_and(|voted| voted).then_some(ReplicaIdx(idx))
                }),
        )
    }
}
