use crate::log_id::LogId;
use std::marker::PhantomData;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use moirai_protocol::{
    broadcast::{batch::Batch, internalizer::Resolver, since::Since},
    event::Event,
};

pub mod kind {
    #[cfg(feature = "test_utils")]
    use deepsize::DeepSizeOf;

    #[derive(Debug, Clone, Copy, Default)]
    #[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
    pub struct Any;
    #[derive(Debug, Clone, Copy, Default)]
    #[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
    pub struct Event;
    #[derive(Debug, Clone, Copy, Default)]
    #[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
    pub struct Batch;
    #[derive(Debug, Clone, Copy, Default)]
    #[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
    pub struct Since;
}

pub type EventMessage<P> = Message<P, kind::Event>;
pub type BatchMessage<P> = Message<P, kind::Batch>;
pub type SinceMessage = Message<(), kind::Since>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct Message<P, K = kind::Any> {
    payload: Payload<P>,
    resolver: Resolver,
    /// Which log this message belongs to.
    ///
    /// Hoisted beside the resolver rather than carried per event, for the same
    /// reason the resolver is: one message is one log's traffic, so one copy
    /// says everything a receiver needs in order to decide whether the payload
    /// is addressed to the log it hosts.
    log_id: LogId,
    #[serde(skip)]
    _kind: PhantomData<K>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum Payload<P> {
    Event(Event<P>),
    Batch(Batch<P>),
    Since(Since),
}

impl<P, K> Message<P, K> {
    pub fn resolver(&self) -> &Resolver {
        &self.resolver
    }

    /// The log this message came from. A receiver hosting a different log
    /// refuses it; see [`crate::generic::LogReplica::receive`].
    pub fn log_id(&self) -> &LogId {
        &self.log_id
    }
}

impl<P> Message<P> {
    pub fn new(payload: Payload<P>, resolver: Resolver, log_id: LogId) -> Self {
        Self {
            payload,
            resolver,
            log_id,
            _kind: PhantomData,
        }
    }

    pub fn payload(&self) -> &Payload<P> {
        &self.payload
    }
}

impl<P> Message<P, kind::Event> {
    pub fn new(event: Event<P>, resolver: Resolver, log_id: LogId) -> Self {
        Self {
            payload: Payload::Event(event),
            resolver,
            log_id,
            _kind: PhantomData,
        }
    }

    pub fn event(&self) -> &Event<P> {
        match &self.payload {
            Payload::Event(event) => event,
            _ => unreachable!("EventMessage is expected to hold an event payload"),
        }
    }
}

impl<P> Message<P, kind::Batch> {
    pub fn new(batch: Batch<P>, resolver: Resolver, log_id: LogId) -> Self {
        Self {
            payload: Payload::Batch(batch),
            resolver,
            log_id,
            _kind: PhantomData,
        }
    }

    pub fn batch(&self) -> &Batch<P> {
        match &self.payload {
            Payload::Batch(batch) => batch,
            _ => unreachable!("BatchMessage is expected to hold a batch payload"),
        }
    }

    pub fn into_batch(self) -> Batch<P> {
        match self.payload {
            Payload::Batch(batch) => batch,
            _ => unreachable!("BatchMessage is expected to hold a batch payload"),
        }
    }

    pub fn into_parts(self) -> (Batch<P>, Resolver) {
        match self.payload {
            Payload::Batch(batch) => (batch, self.resolver),
            _ => unreachable!("BatchMessage is expected to hold a batch payload"),
        }
    }
}

impl<P> Message<P, kind::Since> {
    pub fn new(since: Since, resolver: Resolver, log_id: LogId) -> Self {
        Self {
            payload: Payload::Since(since),
            resolver,
            log_id,
            _kind: PhantomData,
        }
    }

    pub fn since(&self) -> &Since {
        match &self.payload {
            Payload::Since(since) => since,
            _ => unreachable!("SinceMessage is expected to hold a since payload"),
        }
    }
}

pub(crate) fn deserialize_replica_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let id = String::deserialize(deserializer)?;
    moirai_protocol::replica::validate_replica_id(&id).map_err(serde::de::Error::custom)?;
    Ok(id)
}
