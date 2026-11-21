use std::fmt::{Debug, Display};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::broadcast::tcsb::{StateSnapshot, Tcsb};
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
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ReplicaId(pub String);

/// Validate an identity before it enters a network member table.
pub fn validate_replica_id(id: &str) -> Result<(), String> {
    if !id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        Ok(())
    } else {
        Err(format!(
            "Invalid replica id {id:?}: expected nonempty ASCII letters, digits, '.', '_' or '-'"
        ))
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for ReplicaId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let id = <String as serde::Deserialize>::deserialize(deserializer)?;
        if NETWORK_IDS.with(std::cell::Cell::get) {
            validate_replica_id(&id).map_err(serde::de::Error::custom)?;
        }
        Ok(Self(id))
    }
}

thread_local! {
    static NETWORK_IDS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Keep network identity grammar out of the protocol's unrestricted local IDs.
pub fn with_network_id_validation<R>(decode: impl FnOnce() -> R) -> R {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            NETWORK_IDS.with(|enabled| enabled.set(self.0));
        }
    }
    let _restore = Restore(NETWORK_IDS.with(|enabled| enabled.replace(true)));
    decode()
}

/// Local index of the replica. It is an alias for its string ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
    T: IsTcsb<L::Update> + Debug,
{
    type Command = L::Command;
    type Payload = L::Update;

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
    T: IsTcsb<L::Update> + Debug,
{
    fn deliver(&mut self, event: Event<L::Update>) {
        crate::state::trace::begin(event.id(), event.lamport());
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
    T: IsTcsb<L::Update> + Debug,
{
    /// The CRDT log itself, not its read-out.
    pub fn log(&self) -> &L {
        &self.state
    }

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
        let mut state = L::default();
        state.hosted_by(&id);
        Self {
            id,
            tcsb: T::new(idx, interner),
            state,
        }
    }

    pub fn bootstrap_with_state<M>(id: ReplicaId, members: &[M], mut state: L) -> Self
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
        state.hosted_by(&id);
        Self {
            id,
            tcsb: T::new(idx, interner),
            state,
        }
    }
}

impl<L: IsLog> Replica<L, Tcsb<L::Update>> {
    pub fn tcsb(&self) -> &Tcsb<L::Update> {
        &self.tcsb
    }

    pub fn state(&self) -> &L {
        &self.state
    }

    pub fn bootstrap_with_interner(
        id: ReplicaId,
        members: &[&str],
        interner: crate::broadcast::internalizer::SharedInterner,
    ) -> Self {
        assert!(members.contains(&id.as_ref()), "bootstrap member missing");
        let idx = {
            let mut table = crate::broadcast::internalizer::lock_interner(&interner);
            let (idx, _) = table.intern(&id);
            for member in members {
                table.intern(&(*member).into());
            }
            idx
        };
        let mut state = L::default();
        state.hosted_by(&id);
        Self {
            id,
            tcsb: Tcsb::with_interner(idx, interner),
            state,
        }
    }

    /// Capture causal bookkeeping to transfer alongside the log.
    pub fn snapshot(&self) -> StateSnapshot<L::Update> {
        self.tcsb.snapshot()
    }

    /// Replace the log and causal bookkeeping with a donor's state.
    pub fn adopt(&mut self, snapshot: StateSnapshot<L::Update>, mut state: L) {
        let donor = self.tcsb.adopt(snapshot, &self.id);
        state.hosted_by(&self.id);
        state.adopted_from(&donor);
        self.state = state;
    }
}

#[cfg(feature = "test_utils")]
impl<L> Replica<L, Tcsb<L::Update>>
where
    L: IsLog,
{
    pub fn new(id: ReplicaId, tcsb: Tcsb<L::Update>, mut state: L) -> Self {
        state.hosted_by(&id);
        Self { id, tcsb, state }
    }

    pub fn delivered_count(&self) -> usize {
        self.tcsb.matrix_clock().origin_version().sum()
    }

    pub fn into_state(self) -> L {
        self.state
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

impl PartialEq<str> for ReplicaId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
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
