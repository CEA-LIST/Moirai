use crate::{
    broadcast::{
        message::{BatchMessage, EventMessage, SinceMessage},
        tcsb::IsTcsb,
    },
    clock::version_vector::Version,
    commitment::{commit_log::CommitLog, commit_op::CommitOp, oracle::IsOracle},
    crdt::{eval::EvalNested, query::QueryOperation, sequential::SequentialDataType},
    event::{Event, id::EventId},
    replica::{IsReplica, ReplicaId, ReplicaIdOwned},
    state::{
        effect_context::EffectContext, log::IsLog, sink::SinkCollector,
        unstable_state::IsUnstableCore,
    },
};
use std::collections::{BTreeMap, BTreeSet};

pub struct MixedConsistencyReplica<A, O, T>
where
    A: SequentialDataType,
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
    potential_leaders: BTreeMap<Version, BTreeMap<usize, bool>>,
    /// Keeps track of all the partial leaders in a vertex's cut.
    p_leaders_hist: BTreeMap<Version, BTreeSet<Version>>,
    pre_committed: BTreeSet<Version>,
    latest_committed: Version,
}

impl<A, O, T> IsReplica<CommitLog<A>> for MixedConsistencyReplica<A, O, T>
where
    A: SequentialDataType,
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

    fn send(
        &mut self,
        cmd: Self::Command,
    ) -> Result<EventMessage<Self::Payload>, <L as IsLog>::Rejection> {
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
        L: EvalNested<Q>,
    {
        self.state.eval(q)
    }
}

impl<A, O, T> MixedConsistencyReplica<A, O, T>
where
    A: SequentialDataType,
    O: IsOracle,
    T: IsTcsb<CommitOp<A::Update>>,
{
    fn deliver(&mut self, event: Event<CommitOp<A::Update>>) {
        // TODO: unecessary here
        let mut sink = SinkCollector::new();
        let mut ctx = EffectContext::root("root", Some(&mut sink));

        self.state.effect(event, &mut ctx);

        // Check potential leader
        // let (is_pot, votes) = self.is_pot_leader(&id_clock);
        // if is_pot {
        //     let partial_leaders = self.get_partial_leaders().map(|v| v.0.clone()).collect();
        //     self.p_leaders_hist
        //         .insert(id_clock.clone(), partial_leaders);

        //     let votes = BTreeMap::from_iter(votes.iter().map(|v| (*v, false)));
        //     self.potential_leaders.insert(id_clock.clone(), votes);
        //     println!("Added Pot Leader!: {}", &id_clock);
        // }

        // // Updating list of partial leaders
        // self.update_leaders(&id_clock, msg_update.leader);

        // // Checking for commitments
        // self.check_pre_committed();

        // // Prunning
        // self.record();
    }

    fn prepare(&self, cmd: A::Update) -> CommitOp<A::Update> {
        CommitOp::new(cmd, self.oracle.query().into())
    }

    fn is_potential_leader(
        &self,
        position: &EventId,
        quorum_size: usize,
    ) -> Option<BTreeSet<ReplicaIdOwned>> {
        let candidate = position.origin_id();
        let votes = self.votes(candidate, position);

        (votes.len() >= quorum_size).then_some(votes)
    }

    fn votes(&self, candidate: &ReplicaId, position: &EventId) -> BTreeSet<ReplicaIdOwned> {
        let mut replica_vote = BTreeMap::<ReplicaIdOwned, bool>::new();

        for position in self.state.unstable().ancestors(position) {
            if let Some(entry) = self.state.unstable().get(&position) {
                let sender = entry.id().origin_id().to_string();
                let vote = entry.op().leader == candidate;
                replica_vote
                    .entry(sender)
                    .and_modify(|current| *current = *current && vote)
                    .or_insert(vote);
            }
        }

        replica_vote
            .into_iter()
            .filter_map(|(replica, voted)| voted.then_some(replica))
            .collect()
    }
}
