use std::fmt::Debug;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

#[cfg(feature = "test_utils")]
use crate::{broadcast::tcsb::IsTcsbTest, clock::version_vector::Version};
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

pub type ReplicaId = str;
pub type ReplicaIdOwned = String;

/// Local index of the replica. It is an alias for its string ID.
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    fn query<Q: QueryOperation>(&self, q: Q) -> Q::Response
    where
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
    id: ReplicaIdOwned,
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
        self.tcsb.receive_batch(message);
        while let Some(e) = self.tcsb.next_causally_ready() {
            self.deliver(e);
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

    fn query<Q: QueryOperation>(&self, q: Q) -> Q::Response
    where
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

        let maybe_version = self.tcsb.is_stable();
        if let Some(version) = maybe_version {
            self.state.stabilize(version);
        }
    }

    pub fn bootstrap(id: ReplicaIdOwned, members: &[&ReplicaId]) -> Self {
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
            state: L::default(),
        }
    }

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

    /// Versions at which the communication layer advanced causal stability.
    /// The initial zero version is not included.
    pub fn stable_version_history(&self) -> &[Version] {
        self.tcsb.lsv_history()
    }

    pub fn state(&self) -> &L {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut L {
        &mut self.state
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use crate::{
        broadcast::tcsb::Tcsb,
        clock::version_vector::Version,
        event::Event,
        state::{effect_context::EffectContext, log::IsLog},
    };

    use super::{IsReplica, Replica, ReplicaId};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Prepared(u8);

    #[derive(Debug, Default)]
    struct PreparedLog;

    impl IsLog for PreparedLog {
        type Value = ();
        type Command = u8;
        type Op = Prepared;
        type Rejection = Infallible;

        fn prepare(&self, command: Self::Command) -> Self::Op {
            Prepared(command + 1)
        }

        fn effect(&mut self, _event: Event<Self::Op>, _ctx: &mut EffectContext<'_>) {}

        fn stabilize(&mut self, _version: &Version) {}

        fn redundant_by_parent(&mut self, _version: &Version, _conservative: bool) {}

        fn is_default(&self) -> bool {
            true
        }
    }

    #[test]
    fn prepare_maps_a_command_to_the_wire_payload() {
        let members: [&ReplicaId; 1] = ["A"];
        let mut replica = Replica::<PreparedLog, Tcsb<Prepared>>::bootstrap("A".into(), &members);

        let message = replica.send(41).unwrap();

        assert_eq!(message.event().op(), &Prepared(42));
    }
}
