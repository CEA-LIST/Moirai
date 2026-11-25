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

use std::fmt::{Debug, Display};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

#[cfg(any(feature = "test_utils", feature = "serde"))]
use crate::broadcast::tcsb::Tcsb;
#[cfg(feature = "serde")]
use crate::broadcast::{
    internalizer::{Resolver, with_shared_resolver},
    tcsb::StateSnapshot,
};
use crate::{
    broadcast::{
        internalizer::Interner,
        message::{BatchMessage, EventMessage, SinceMessage},
        tcsb::IsTcsb,
    },
    data_type::query::QueryOperation,
    event::Event,
    state::{
        effect_context::EffectContext,
        log::{EvalNested, IsLog},
        sink::SinkCollector,
    },
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
        // Keep track of the effects of the event on the state (e.g., object creation, deletion, etc.)
        let mut sink = SinkCollector::new();
        let mut ctx = EffectContext::root("root", Some(&mut sink));

        self.state.effect(event, &mut ctx);

        if let Some(frontier) = self.tcsb.is_stable() {
            self.state.stabilize(frontier);
        }
    }
}

/// A replica's log and causal bookkeeping, as a state transfer sends them.
#[cfg(feature = "serde")]
#[derive(serde::Serialize)]
pub struct Snapshot<'a, L, O> {
    state: &'a L,
    tcsb: StateSnapshot<O>,
}

/// A state transfer as received: only deserialization makes one, so the
/// member table its parts share is the adopter's alone.
#[cfg(feature = "serde")]
pub struct ReplicaSnapshot<L, O> {
    state: L,
    tcsb: StateSnapshot<O>,
    table: Resolver,
}

#[cfg(feature = "serde")]
impl<'de, L, O> serde::Deserialize<'de> for ReplicaSnapshot<L, O>
where
    L: serde::Deserialize<'de>,
    O: serde::Deserialize<'de>,
{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Parts<L, O> {
            state: L,
            tcsb: StateSnapshot<O>,
        }
        let (parts, table) = with_shared_resolver(|| Parts::<L, O>::deserialize(deserializer));
        let Parts { state, tcsb } = parts?;
        let table = table.ok_or_else(|| serde::de::Error::custom("a snapshot with no members"))?;
        tcsb.check(&table).map_err(serde::de::Error::custom)?;
        Ok(Self { state, tcsb, table })
    }
}

#[cfg(feature = "serde")]
impl<L> Replica<L, Tcsb<L::Update>>
where
    L: IsLog,
    L::Update: Clone,
{
    /// This replica's log and causal bookkeeping, for a state transfer.
    pub fn snapshot(&self) -> Snapshot<'_, L, L::Update> {
        Snapshot {
            state: &self.state,
            tcsb: self.tcsb.snapshot(),
        }
    }

    /// Replace this replica's log and causal bookkeeping with a donor's,
    /// keeping this replica's own id. Only for a replica that has sent
    /// nothing yet.
    pub fn adopt(&mut self, snapshot: ReplicaSnapshot<L, L::Update>) {
        self.tcsb.adopt(snapshot.tcsb, snapshot.table, &self.id);
        self.state = snapshot.state;
    }
}

impl<L, T> Replica<L, T>
where
    L: IsLog,
    T: IsTcsb<L::Update>,
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
impl<L> Replica<L, Tcsb<L::Update>>
where
    L: IsLog,
{
    pub const fn new(id: ReplicaId, tcsb: Tcsb<L::Update>, state: L) -> Self {
        Self { id, tcsb, state }
    }

    pub const fn tcsb(&self) -> &Tcsb<L::Update> {
        &self.tcsb
    }

    pub fn delivered_count(&self) -> usize {
        self.tcsb.matrix_clock().origin_version().sum()
    }

    pub fn into_state(self) -> L {
        self.state
    }

    pub const fn state(&self) -> &L {
        &self.state
    }

    pub const fn state_mut(&mut self) -> &mut L {
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
