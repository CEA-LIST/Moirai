use std::fmt::Debug;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

#[cfg(feature = "test_utils")]
use crate::broadcast::tcsb::IsTcsbTest;
use crate::{
    broadcast::{
        message::{BatchMessage, EventMessage, SinceMessage},
        tcsb::IsTcsb,
    },
    crdt::{
        eval::{BorrowedRead, EvalNested},
        query::QueryOperation,
    },
    event::Event,
    state::{effect_context::EffectContext, log::IsLog, sink::SinkCollector},
    utils::intern_str::Interner,
};

pub type ReplicaId = str;
pub type ReplicaIdOwned = String;

#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReplicaIdx(pub usize);

/// A replica in the system, which maintains a local state and communicates with other replicas via messages.
pub trait IsReplica<L>
where
    L: IsLog,
{
    /// Create a new replica with the given ID. The ID must be unique across all replicas in the system.
    fn new(id: ReplicaIdOwned) -> Self;
    /// Return the ID of this replica.
    fn id(&self) -> &ReplicaId;
    /// Receive a message from the network.
    fn receive(&mut self, message: EventMessage<L::Op>);
    /// Receive a batch message from the network.
    fn receive_batch(&mut self, message: BatchMessage<L::Op>);
    /// Return a `since` message representing a request for all events causally after the given version.
    fn since(&self) -> SinceMessage;
    /// Send an operation to the network. Returns the message to be sent, or `None` if the operation is not enabled.
    fn send(&mut self, op: L::Op) -> Result<EventMessage<L::Op>, L::Rejection>;
    /// Return a batch message containing all events causally after the given version.
    fn pull(&mut self, since: SinceMessage) -> BatchMessage<L::Op>;
    /// Query the current state of the replica with the given query operation.
    fn query<Q: QueryOperation>(&self, q: Q) -> Q::Response
    where
        L: EvalNested<Q>;
    /// Borrow the cached materialized value of the replica state.
    fn read_ref(&self) -> &L::Value
    where
        L: BorrowedRead;
    /// Update the state of the replica with the given operation.
    fn update(&mut self, op: L::Op) -> Result<(), L::Rejection> {
        self.send(op)?;
        Ok(())
    }
    /// Bootstrap a new replica with the given ID and list of members. The ID must be included in the members list.
    fn bootstrap(id: ReplicaIdOwned, members: &[&ReplicaId]) -> Self;
}

#[derive(Debug)]
pub struct Replica<L, T> {
    id: ReplicaIdOwned,
    tcsb: T,
    state: L,
}

impl<L, T> IsReplica<L> for Replica<L, T>
where
    L: IsLog,
    T: IsTcsb<L::Op> + Debug,
{
    fn new(id: ReplicaIdOwned) -> Self {
        let mut interner = Interner::new();
        let idx = interner.intern(&id);
        Self {
            id,
            tcsb: T::new(idx.0, interner),
            state: L::new(),
        }
    }

    fn receive(&mut self, message: EventMessage<L::Op>) {
        self.tcsb.receive(message);
        while let Some(e) = self.tcsb.next_causally_ready() {
            self.deliver(e);
        }
    }

    fn receive_batch(&mut self, message: BatchMessage<L::Op>) {
        self.tcsb.receive_batch(message);
        while let Some(e) = self.tcsb.next_causally_ready() {
            self.deliver(e);
        }
    }

    fn send(&mut self, op: L::Op) -> Result<EventMessage<L::Op>, L::Rejection> {
        self.state.is_enabled(&op)?;
        let op = L::prepare(op);
        let message = self.tcsb.send(op);
        self.deliver(message.event().clone());
        Ok(message)
    }

    fn pull(&mut self, since: SinceMessage) -> BatchMessage<L::Op> {
        self.tcsb.pull(since)
    }

    fn query<Q: QueryOperation>(&self, q: Q) -> Q::Response
    where
        L: EvalNested<Q>,
    {
        self.state.eval(q)
    }

    fn read_ref(&self) -> &L::Value
    where
        L: BorrowedRead,
    {
        self.state.read_ref()
    }

    fn since(&self) -> SinceMessage {
        self.tcsb.since()
    }

    fn id(&self) -> &ReplicaId {
        &self.id
    }

    fn bootstrap(id: ReplicaIdOwned, members: &[&ReplicaId]) -> Self {
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
            state: L::new(),
        }
    }
}

impl<L, T> Replica<L, T>
where
    L: IsLog,
    T: IsTcsb<L::Op>,
{
    pub fn bootstrap_with_state(id: ReplicaIdOwned, members: &[&ReplicaId], state: L) -> Self {
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
        }
    }

    fn deliver(&mut self, event: Event<L::Op>) {
        let mut sink = SinkCollector::new();
        let mut ctx = EffectContext::root("root", Some(&mut sink));

        self.state.effect(event, &mut ctx);

        let maybe_version = self.tcsb.is_stable();
        if let Some(version) = maybe_version {
            self.state.stabilize(version);
        }
    }
}

#[cfg(feature = "test_utils")]
impl<L, T> Replica<L, T>
where
    L: IsLog,             // + DeepSizeOf,
    T: IsTcsbTest<L::Op>, // + DeepSizeOf,
{
    pub fn tcsb(&self) -> &T {
        &self.tcsb
    }

    pub fn num_delivered_events(&self) -> usize {
        self.tcsb.matrix_clock().origin_version().sum()
    }

    pub fn state(&self) -> &L {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut L {
        &mut self.state
    }
}
