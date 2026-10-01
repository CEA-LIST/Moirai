use std::fmt::{Debug, Display};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

#[cfg(feature = "test_utils")]
use crate::broadcast::tcsb::Tcsb;
use crate::{
    broadcast::{
        internalizer::Interner,
        message::{BatchMessage, EventMessage, SinceMessage},
        tcsb::IsTcsb,
    },
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    state::{effect_context::EffectContext, log::IsLog, sink::SinkCollector},
};

/// Replica identifier
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ReplicaId(pub String);

/// Local index of the replica. It is an alias for its string ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct ReplicaIdx(pub usize);

/// A replica in the system, which maintains a local state and communicates with other replicas via messages.
pub trait IsReplica<L>
where
    L: IsLog,
{
    /// Inputs of the clients to the replica.
    type Command;
    /// Content of the event messages. In most cases, it will be the same as the input command.
    type Payload;

    /// Return the ID of this replica.
    fn id(&self) -> &ReplicaId;
    /// Prepare a command to be sent to the network.
    fn prepare(&self, cmd: Self::Command) -> Self::Payload;
    /// Receive a message from the network.
    fn receive(&mut self, message: EventMessage<Self::Payload>);
    /// Receive a batch message from the network.
    fn receive_batch(&mut self, message: BatchMessage<Self::Payload>);
    /// Return a `since` message representing a request for all events causally after the given version.
    fn since(&self) -> SinceMessage;
    /// Send a command to the network. Returns the message to be sent, or `None` if the command is not enabled.
    fn send(&mut self, cmd: Self::Command) -> Result<EventMessage<Self::Payload>, L::Rejection>;
    /// Return a batch message containing all events causally after the given version.
    fn pull(&mut self, since: SinceMessage) -> BatchMessage<Self::Payload>;
    // TODO: add a method for state transfer
    /// Query the current state of the replica with the given query operation.
    fn query<Q>(&self, q: &Q) -> Q::Response
    where
        Q: QueryOperation,
        L: EvalNested<Q>;
    /// Update the state of the replica with the given operation.
    fn update(&mut self, cmd: Self::Command) -> Result<(), L::Rejection> {
        self.send(cmd)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Replica<L, T> {
    /// Replica ID (must be unique across all replicas in the system)
    id: ReplicaId,
    /// Communication-layer
    tcsb: T,
    /// Replica state
    state: L,
}

impl<L, T> IsReplica<L> for Replica<L, T>
where
    L: IsLog,
    T: IsTcsb<L::Op> + Debug,
{
    type Command = L::Command;
    type Payload = L::Op;

    fn prepare(&self, cmd: Self::Command) -> Self::Payload {
        self.state.prepare(cmd)
    }

    fn receive(&mut self, message: EventMessage<Self::Payload>) {
        self.tcsb.receive(message);
        while let Some(e) = self.tcsb.next_causally_ready() {
            self.deliver(e);
        }
    }

    fn receive_batch(&mut self, message: BatchMessage<Self::Payload>) {
        let is_empty = message.batch().events().is_empty();

        self.tcsb.receive_batch(message);

        if !is_empty {
            while let Some(e) = self.tcsb.next_causally_ready() {
                self.deliver(e);
            }
        }
    }

    fn send(&mut self, cmd: Self::Command) -> Result<EventMessage<Self::Payload>, L::Rejection> {
        let payload = self.prepare(cmd);
        self.state.is_enabled(&payload)?;
        let message = self.tcsb.send(payload);
        self.deliver(message.event().clone());
        Ok(message)
    }

    fn pull(&mut self, since: SinceMessage) -> BatchMessage<Self::Payload> {
        self.tcsb.pull(since)
    }

    fn query<Q>(&self, q: &Q) -> Q::Response
    where
        Q: QueryOperation,
        L: EvalNested<Q>,
    {
        self.state.eval(q)
    }

    fn since(&self) -> SinceMessage {
        self.tcsb.since()
    }

    fn id(&self) -> &ReplicaId {
        &self.id
    }
}

impl<L, T> Replica<L, T>
where
    L: IsLog,
    T: IsTcsb<L::Op> + Debug,
{
    fn deliver(&mut self, event: Event<L::Op>) {
        // Keep track of the effects of the event on the state (e.g., object creation, deletion, etc.)
        let mut sink = SinkCollector::new();
        let mut ctx = EffectContext::root("root", Some(&mut sink));

        self.state.effect(event, &mut ctx);

        if let Some(frontier) = self.tcsb.is_stable() {
            self.state.stabilize(frontier);
        }
    }
}

impl<L, T> Replica<L, T>
where
    L: IsLog,
    T: IsTcsb<L::Op> + Debug,
{
    pub fn bootstrap<M>(id: ReplicaId, members: &[M]) -> Self
    where
        M: AsRef<str> + Debug,
    {
        assert!(
            members.iter().any(|member| member.as_ref() == id.as_ref()),
            "Bootstrap replica ID {} must be included in members list {:?}",
            id,
            members
        );
        let mut interner = Interner::new();
        let (idx, _) = interner.intern(&id);
        for member in members {
            interner.intern(&ReplicaId::from(member.as_ref()));
        }
        Self {
            id,
            tcsb: T::new(idx, interner),
            state: L::default(),
        }
    }

    pub fn bootstrap_with_state<M>(id: ReplicaId, members: &[M], state: L) -> Self
    where
        M: AsRef<str> + Debug,
    {
        assert!(
            members.iter().any(|member| member.as_ref() == id.as_ref()),
            "Bootstrap replica ID {} must be included in members list {:?}",
            id,
            members
        );
        let mut interner = Interner::new();
        let (idx, _) = interner.intern(&id);
        for member in members {
            interner.intern(&ReplicaId::from(member.as_ref()));
        }
        Self {
            id,
            tcsb: T::new(idx, interner),
            state,
        }
    }
}

#[cfg(feature = "test_utils")]
impl<L> Replica<L, Tcsb<L::Op>>
where
    L: IsLog,
{
    pub fn new(id: ReplicaId, tcsb: Tcsb<L::Op>, state: L) -> Self {
        Self { id, tcsb, state }
    }

    pub fn tcsb(&self) -> &Tcsb<L::Op> {
        &self.tcsb
    }

    pub fn delivered_count(&self) -> usize {
        self.tcsb.matrix_clock().origin_version().sum()
    }

    pub fn into_state(self) -> L {
        self.state
    }

    pub fn state(&self) -> &L {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut L {
        &mut self.state
    }
}

impl Display for ReplicaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for ReplicaId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for ReplicaId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for ReplicaId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}
