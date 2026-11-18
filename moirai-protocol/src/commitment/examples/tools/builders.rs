use std::collections::BTreeSet;

use crate::{
    broadcast::{
        message::{BatchMessage, EventMessage, SinceMessage},
        tcsb::Tcsb,
    },
    commitment::{
        commit_log::CommitmentLog,
        commit_protocols::omega::{
            omega_commit::OmegaCommit,
            oracle::{IsOracle, Omega},
            support_op::SupportOp,
        },
        wire_id::StableReplicaId,
    },
    crdt::{eval::EvalNested, query::QueryOperation},
    replica::{IsReplica, Replica, ReplicaId},
    state::log::IsLog,
};

use crate::commitment::commit_protocols::lamport::{
    lamport_commit::LamportCommit, lamport_tracker::LamportTracker,
};

/// A Lamport stability commitment log over a child log.
pub type LamportCommitLog<L> = CommitmentLog<L, LamportCommit>;

pub struct LamportCommitReplica<L: IsLog> {
    replica: Replica<LamportCommitLog<L>, Tcsb<L::Update>>,
}

impl<L: IsLog> LamportCommitReplica<L> {
    pub fn new(own_id: StableReplicaId, members: Vec<(StableReplicaId, ReplicaId)>) -> Self {
        Self::from_parts(L::default(), members, own_id)
    }

    pub fn from_parts(
        child: L,
        members: Vec<(StableReplicaId, ReplicaId)>,
        own_id: StableReplicaId,
    ) -> Self {
        let mut stable_ids = BTreeSet::new();
        let mut replica_ids = BTreeSet::new();
        for (stable, replica) in &members {
            assert!(stable_ids.insert(*stable), "Duplicate stable member ID");
            assert!(replica_ids.insert(replica), "Duplicate replica ID");
        }
        let id = members
            .iter()
            .find(|(stable, _)| *stable == own_id)
            .expect("Own ID must be a configured member")
            .1
            .clone();
        let ids: Vec<_> = members.iter().map(|(_, id)| id.clone()).collect();
        let protocol = LamportCommit::from_parts(members, LamportTracker::default());
        Self {
            replica: Replica::bootstrap_with_state(id, &ids, CommitmentLog::new(child, protocol)),
        }
    }

    pub fn into_inner(self) -> Replica<LamportCommitLog<L>, Tcsb<L::Update>> {
        self.replica
    }
}

/// An Omega commitment log over a child log, such as `VecLog<R>`.
pub type CommitLog<L> = CommitmentLog<L, OmegaCommit<Omega>>;

/// A replica with fixed membership and an explicitly configured Omega leader.
pub struct CommitReplica<L: IsLog> {
    replica: Replica<CommitLog<L>, Tcsb<SupportOp<L::Update>>>,
}

impl<L: IsLog> CommitReplica<L> {
    /// Bootstrap a default child log. Member IDs must be identical at every replica.
    pub fn new(
        own_id: StableReplicaId,
        members: Vec<(StableReplicaId, ReplicaId)>,
        leader: StableReplicaId,
    ) -> Self {
        Self::from_parts(L::default(), Omega::with_leader(leader), members, own_id)
    }

    /// Bootstrap with an existing child log and a configured oracle.
    pub fn from_parts(
        child: L,
        oracle: Omega,
        members: Vec<(StableReplicaId, ReplicaId)>,
        own_id: StableReplicaId,
    ) -> Self {
        let mut stable_ids = BTreeSet::new();
        let mut replica_ids = BTreeSet::new();
        for (stable, replica) in &members {
            assert!(stable_ids.insert(*stable), "Duplicate stable member ID");
            assert!(replica_ids.insert(replica), "Duplicate replica ID");
        }
        let id = members
            .iter()
            .find(|(stable, _)| *stable == own_id)
            .expect("Own ID must be a configured member")
            .1
            .clone();
        assert!(
            stable_ids.contains(&oracle.query()),
            "Leader must be a configured member"
        );

        let ids: Vec<_> = members.iter().map(|(_, id)| id.clone()).collect();
        let protocol = OmegaCommit::from_parts(oracle, members, own_id);
        Self {
            replica: Replica::bootstrap_with_state(id, &ids, CommitmentLog::new(child, protocol)),
        }
    }

    pub fn into_inner(self) -> Replica<CommitLog<L>, Tcsb<SupportOp<L::Update>>> {
        self.replica
    }
}

impl<L: IsLog> IsReplica<CommitLog<L>> for CommitReplica<L> {
    type Command = L::Command;
    type Payload = SupportOp<L::Update>;

    fn id(&self) -> &ReplicaId {
        self.replica.id()
    }

    fn prepare(&self, cmd: Self::Command) -> Self::Payload {
        self.replica.prepare(cmd)
    }

    fn receive(&mut self, message: EventMessage<Self::Payload>) {
        self.replica.receive(message);
    }

    fn receive_batch(&mut self, message: BatchMessage<Self::Payload>) {
        self.replica.receive_batch(message);
    }

    fn since(&self) -> SinceMessage {
        self.replica.since()
    }

    fn send(&mut self, cmd: Self::Command) -> Result<EventMessage<Self::Payload>, L::Rejection> {
        self.replica.send(cmd)
    }

    fn pull(&mut self, since: SinceMessage) -> BatchMessage<Self::Payload> {
        self.replica.pull(since)
    }

    fn query<Q>(&self, q: &Q) -> Q::Response
    where
        Q: QueryOperation,
        CommitLog<L>: EvalNested<Q>,
    {
        self.replica.query(q)
    }
}

impl<L: IsLog> IsReplica<LamportCommitLog<L>> for LamportCommitReplica<L> {
    type Command = L::Command;
    type Payload = L::Update;

    fn id(&self) -> &ReplicaId {
        self.replica.id()
    }

    fn prepare(&self, cmd: Self::Command) -> Self::Payload {
        self.replica.prepare(cmd)
    }

    fn receive(&mut self, message: EventMessage<Self::Payload>) {
        self.replica.receive(message);
    }

    fn receive_batch(&mut self, message: BatchMessage<Self::Payload>) {
        self.replica.receive_batch(message);
    }

    fn since(&self) -> SinceMessage {
        self.replica.since()
    }

    fn send(&mut self, cmd: Self::Command) -> Result<EventMessage<Self::Payload>, L::Rejection> {
        self.replica.send(cmd)
    }

    fn pull(&mut self, since: SinceMessage) -> BatchMessage<Self::Payload> {
        self.replica.pull(since)
    }

    fn query<Q>(&self, q: &Q) -> Q::Response
    where
        Q: QueryOperation,
        LamportCommitLog<L>: EvalNested<Q>,
    {
        self.replica.query(q)
    }
}

/// Build a fixed-membership system using Lamport timestamp stability.
pub fn lamport_commit_replicas<L: IsLog>(n: usize) -> Vec<LamportCommitReplica<L>> {
    assert!(n > 0, "Commitment requires a nonempty membership");
    assert!(
        n <= usize::from(u16::MAX) + 1,
        "Too many members for stable IDs"
    );
    let members: Vec<_> = (0..n)
        .map(|i| (StableReplicaId(i as u16), ReplicaId(format!("replica-{i}"))))
        .collect();
    (0..n)
        .map(|i| LamportCommitReplica::new(StableReplicaId(i as u16), members.clone()))
        .collect()
}

/// Build `n` replicas with default child logs, named `replica-0`, `replica-1`, etc.
/// All replicas initially follow stable member 0. For example, use
/// `commit_replicas::<VecLog<MyDataType>>(3)` to create a three-member system.
pub fn commit_replicas<L: IsLog>(n: usize) -> Vec<CommitReplica<L>> {
    assert!(n > 0, "Commitment requires a nonempty membership");
    assert!(
        n <= usize::from(u16::MAX) + 1,
        "Too many members for stable IDs"
    );
    let members: Vec<_> = (0..n)
        .map(|i| (StableReplicaId(i as u16), ReplicaId(format!("replica-{i}"))))
        .collect();
    (0..n)
        .map(|i| {
            CommitReplica::new(
                StableReplicaId(i as u16),
                members.clone(),
                StableReplicaId(0),
            )
        })
        .collect()
}
