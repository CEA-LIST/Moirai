use std::fmt::Debug;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

#[cfg(feature = "serde")]
use crate::broadcast::tcsb::StateSnapshot;
#[cfg(feature = "test_utils")]
use crate::{broadcast::tcsb::IsTcsbTest, clock::version_vector::Version};
use crate::{
    broadcast::{
        internalizer::{Interner, SharedInterner, lock_interner},
        message::{BatchMessage, EventMessage, SinceMessage},
        tcsb::{IsTcsb, StabilitySnapshot},
    },
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    log_id::LogId,
    state::{effect_context::EffectContext, log::IsLog, sink::SinkCollector},
};

pub type ReplicaId = str;
pub type ReplicaIdOwned = String;

/// Local index of the replica. It is an alias for its string ID.
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    /// Send a command to the network. Returns the message to be sent, or the log's rejection.
    fn send(&mut self, cmd: Self::Command) -> Result<EventMessage<Self::Payload>, L::Rejection>;
    /// Return a batch message containing all events causally after the given version.
    fn pull(&mut self, since: SinceMessage) -> BatchMessage<Self::Payload>;
    // TODO: add a method for state transfer
    /// Query the current state of the replica with the given query operation.
    fn query<Q: QueryOperation>(&self, q: &Q) -> Q::Response
    where
        L: EvalNested<Q>;
    /// Update the state of the replica with the given operation.
    fn update(&mut self, cmd: Self::Command) -> Result<(), L::Rejection> {
        self.send(cmd)?;
        Ok(())
    }
    /// Bootstrap a replica hosting a *fresh* log.
    ///
    /// Mints an id, which is right for the replica that creates a log and
    /// wrong for every replica meant to join one that already exists — two
    /// replicas that mint separately host two different logs and refuse each
    /// other's events. Use [`bootstrap_with_log_id`] wherever the log is
    /// shared.
    ///
    /// [`bootstrap_with_log_id`]: IsReplica::bootstrap_with_log_id
    fn bootstrap(id: ReplicaIdOwned, members: &[&ReplicaId]) -> Self
    where
        Self: Sized,
    {
        Self::bootstrap_with_log_id(id, members, LogId::generate())
    }
    /// Bootstrap a replica hosting the log named by `log_id`.
    fn bootstrap_with_log_id(id: ReplicaIdOwned, members: &[&ReplicaId], log_id: LogId) -> Self;
    /// [`bootstrap_with_log_id`], except the member table is `interner`,
    /// shared with every other log the caller hosts.
    ///
    /// The table describes the session's members, not any one log's: a node
    /// hosting several logs holds one and hands each of them the handle, so
    /// that sixteen logs do not mean sixteen copies of the same peer list.
    /// `id` and `members` are interned into it when not already there.
    ///
    /// [`bootstrap_with_log_id`]: IsReplica::bootstrap_with_log_id
    fn bootstrap_with_log_id_and_interner(
        id: ReplicaIdOwned,
        members: &[&ReplicaId],
        log_id: LogId,
        interner: SharedInterner,
    ) -> Self;
}

#[derive(Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
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

    fn query<Q: QueryOperation>(&self, q: &Q) -> Q::Response
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

    fn bootstrap_with_log_id(id: ReplicaIdOwned, members: &[&ReplicaId], log_id: LogId) -> Self {
        Self::bootstrap_with_log_id_and_interner(id, members, log_id, Interner::new().into_shared())
    }

    fn bootstrap_with_log_id_and_interner(
        id: ReplicaIdOwned,
        members: &[&ReplicaId],
        log_id: LogId,
        interner: SharedInterner,
    ) -> Self {
        assert!(
            members.contains(&&(*id)),
            "Bootstrap replica ID {} must be included in members list {:?}",
            id,
            members
        );
        let idx = {
            let mut table = lock_interner(&interner);
            let (idx, _) = table.intern(&id);
            for member in members {
                table.intern(member);
            }
            idx
        };
        Self {
            id,
            tcsb: T::new(idx, interner, log_id),
            state: L::new(),
        }
    }
}

impl<L, T> Replica<L, T>
where
    L: IsLog,
    T: IsTcsb<L::Op>,
{
    /// Current causal-stability bookkeeping. See [`StabilitySnapshot`].
    ///
    /// Available without `test_utils` on purpose: this is what a running
    /// replica has to expose for causal stability to be measurable at all.
    pub fn stability(&self) -> StabilitySnapshot {
        self.tcsb.stability()
    }

    /// The log this replica hosts.
    pub fn log_id(&self) -> &LogId {
        self.tcsb.log_id()
    }

    /// Messages refused for belonging to another log. See
    /// [`IsTcsb::foreign_log_refusals`].
    pub fn foreign_log_refusals(&self) -> u64 {
        self.tcsb.foreign_log_refusals()
    }

    /// The CRDT log itself, so that a donor can serialize it for a joiner.
    ///
    /// Not the *value*: `query` renders the state for reading, which cannot be
    /// replayed onto. State transfer moves the log.
    pub fn log(&self) -> &L {
        &self.state
    }

    /// `true` when this replica has already delivered an operation originated
    /// by `id`. The donor-side guard for state transfer; see
    /// [`IsTcsb::has_history_for`].
    pub fn has_history_for(&self, id: &ReplicaId) -> bool {
        self.tcsb.has_history_for(id)
    }

    /// Everything a fresh replica needs to catch up with this one.
    #[cfg(feature = "serde")]
    pub fn snapshot(&self) -> StateSnapshot<L::Op> {
        self.tcsb.snapshot()
    }

    /// Take over a donor's state wholesale.
    ///
    /// The log and the causal bookkeeping have to move together: a matrix clock
    /// claiming operations the log does not contain would make the joiner
    /// silently reject them as duplicates forever.
    #[cfg(feature = "serde")]
    pub fn adopt(&mut self, snapshot: StateSnapshot<L::Op>, state: L) {
        self.tcsb.adopt(snapshot);
        self.state = state;
    }

    fn deliver(&mut self, event: Event<L::Op>) {
        // The single point every delivery passes through, local and remote
        // alike, and therefore the only honest place to open a trace record.
        // A no-op unless someone has called `trace::set_enabled` — see the
        // module header for why this costs nothing when nobody is watching.
        crate::state::trace::begin(event.id(), event.lamport());
        // Keep track of the effects of the event on the state (e.g., object creation, deletion, etc.)
        let mut sink = SinkCollector::new();
        let mut ctx = EffectContext::root("root", Some(&mut sink));

        self.state.effect(event, &mut ctx);

        let maybe_version = self.tcsb.is_stable();
        if let Some(version) = maybe_version {
            self.state.stabilize(version);
        }
    }

    /// Bootstrap a replica whose log already carries `state`.
    ///
    /// Mints a log id, so every replica meant to share the state must be built
    /// with [`bootstrap_with_state_and_log_id`] instead.
    ///
    /// [`bootstrap_with_state_and_log_id`]: Replica::bootstrap_with_state_and_log_id
    pub fn bootstrap_with_state(id: ReplicaIdOwned, members: &[&ReplicaId], state: L) -> Self {
        Self::bootstrap_with_state_and_log_id(id, members, state, LogId::generate())
    }

    /// [`bootstrap_with_state`], hosting the log named by `log_id`.
    ///
    /// [`bootstrap_with_state`]: Replica::bootstrap_with_state
    pub fn bootstrap_with_state_and_log_id(
        id: ReplicaIdOwned,
        members: &[&ReplicaId],
        state: L,
        log_id: LogId,
    ) -> Self {
        assert!(
            members.contains(&&(*id)),
            "Bootstrap replica ID {} must be included in members list {:?}",
            id,
            members
        );
        let interner = Interner::new().into_shared();
        let idx = {
            let mut table = lock_interner(&interner);
            let (idx, _) = table.intern(&id);
            for member in members {
                table.intern(member);
            }
            idx
        };
        Self {
            id,
            tcsb: T::new(idx, interner, log_id),
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
