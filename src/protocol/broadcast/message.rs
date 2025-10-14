use std::marker::PhantomData;

use crate::{
    protocol::{
        broadcast::{batch::Batch, since::Since},
        event::Event,
    },
    utils::intern_str::Resolver,
};

pub mod kind {
    #[derive(Debug, Clone, Copy, Default)]
    pub struct Any;
    #[derive(Debug, Clone, Copy, Default)]
    pub struct Event;
    #[derive(Debug, Clone, Copy, Default)]
    pub struct Batch;
    #[derive(Debug, Clone, Copy, Default)]
    pub struct Since;
}

pub type EventMessage<O> = Message<O, kind::Event>;
pub type BatchMessage<O> = Message<O, kind::Batch>;
pub type SinceMessage = Message<(), kind::Since>;

#[derive(Debug, Clone)]
pub struct Message<O, K = kind::Any> {
    payload: Payload<O>,
    resolver: Resolver,
    _kind: PhantomData<K>,
}

#[derive(Debug, Clone)]
pub enum Payload<O> {
    Event(Event<O>),
    Batch(Batch<O>),
    Since(Since),
}

impl<O, K> Message<O, K> {
    pub fn resolver(&self) -> &Resolver {
        &self.resolver
    }
}

impl<O> Message<O> {
    pub fn new(payload: Payload<O>, resolver: Resolver) -> Self {
        Self {
            payload,
            resolver,
            _kind: PhantomData,
        }
    }

    pub fn payload(&self) -> &Payload<O> {
        &self.payload
    }
}

impl<O> Message<O, kind::Event> {
    pub fn new(event: Event<O>, resolver: Resolver) -> Self {
        Self {
            payload: Payload::Event(event),
            resolver,
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
    pub fn new(batch: Batch<O>, resolver: Resolver) -> Self {
        Self {
            payload: Payload::Batch(batch),
            resolver,
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
    pub fn new(since: Since, resolver: Resolver) -> Self {
        Self {
            payload: Payload::Since(since),
            resolver,
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
