use std::marker::PhantomData;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::{
    broadcast::{batch::Batch, since::Since},
    event::Event,
    log_id::LogId,
    utils::intern_str::Resolver,
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

pub type EventMessage<O> = Message<O, kind::Event>;
pub type BatchMessage<O> = Message<O, kind::Batch>;
pub type SinceMessage = Message<(), kind::Since>;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct Message<O, K = kind::Any> {
    payload: Payload<O>,
    resolver: Resolver,
    /// Which log this message belongs to.
    ///
    /// Hoisted beside the resolver rather than carried per event, for the same
    /// reason the resolver is: one message is one log's traffic, so one copy
    /// says everything a receiver needs in order to decide whether the payload
    /// is addressed to the log it hosts.
    log_id: LogId,
    #[cfg_attr(feature = "serde", serde(skip))]
    _kind: PhantomData<K>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub enum Payload<O> {
    Event(Event<O>),
    Batch(Batch<O>),
    Since(Since),
}

impl<O, K> Message<O, K> {
    pub fn resolver(&self) -> &Resolver {
        &self.resolver
    }

    /// The log this message came from. A receiver hosting a different log
    /// refuses it; see [`crate::broadcast::tcsb::Tcsb::receive`].
    pub fn log_id(&self) -> &LogId {
        &self.log_id
    }
}

impl<O> Message<O> {
    pub fn new(payload: Payload<O>, resolver: Resolver, log_id: LogId) -> Self {
        Self {
            payload,
            resolver,
            log_id,
            _kind: PhantomData,
        }
    }

    pub fn payload(&self) -> &Payload<O> {
        &self.payload
    }
}

impl<O> Message<O, kind::Event> {
    pub fn new(event: Event<O>, resolver: Resolver, log_id: LogId) -> Self {
        Self {
            payload: Payload::Event(event),
            resolver,
            log_id,
            _kind: PhantomData,
        }
    }

    pub fn event(&self) -> &Event<O> {
        match &self.payload {
            Payload::Event(event) => event,
            _ => unreachable!("EventMessage is expected to hold an event payload"),
        }
    }
}

impl<O> Message<O, kind::Batch> {
    pub fn new(batch: Batch<O>, resolver: Resolver, log_id: LogId) -> Self {
        Self {
            payload: Payload::Batch(batch),
            resolver,
            log_id,
            _kind: PhantomData,
        }
    }

    pub fn batch(&self) -> &Batch<O> {
        match &self.payload {
            Payload::Batch(batch) => batch,
            _ => unreachable!("BatchMessage is expected to hold a batch payload"),
        }
    }

    pub fn into_batch(self) -> Batch<O> {
        match self.payload {
            Payload::Batch(batch) => batch,
            _ => unreachable!("BatchMessage is expected to hold a batch payload"),
        }
    }

    pub fn into_parts(self) -> (Batch<O>, Resolver) {
        match self.payload {
            Payload::Batch(batch) => (batch, self.resolver),
            _ => unreachable!("BatchMessage is expected to hold a batch payload"),
        }
    }
}

impl<O> Message<O, kind::Since> {
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
