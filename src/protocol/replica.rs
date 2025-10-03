use std::{fmt::Debug, str::FromStr};

use tinystr::TinyAsciiStr;
use tracing::{info, instrument};

use crate::{
    protocol::{
        broadcast::{batch::Batch, since::Since, tcsb::IsTcsb},
        event::Event,
        membership::{ReplicaId, ReplicaIdOwned},
        state::log::IsLog,
    },
    utils::intern_str::Interner,
};

pub trait IsReplica<L>
where
    L: IsLog,
{
    fn new(id: ReplicaIdOwned) -> Self;
    fn id(&self) -> &ReplicaId;
    fn receive(&mut self, event: Event<L::Op>);
    fn receive_batch(&mut self, batch: Batch<L::Op>);
    fn since(&self) -> Since;
    fn send(&mut self, op: L::Op) -> Option<Event<L::Op>>;
    fn pull(&mut self, since: Since) -> Batch<L::Op>;
    // TODO: Add support for custom queries
    fn query(&self) -> L::Value;
    fn update(&mut self, op: L::Op);
    fn bootstrap(id: String, members: &[&ReplicaId]) -> Self;
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

    fn receive(&mut self, event: Event<L::Op>) {
        // TODO: check if the event comes from a known replica
        // The event should not come from an unknown replica
        // if self.resolver.get(event.id().origin_id()).is_none() {
        //     error!("Event from unknown replica");
        //     return false;
        // }

        self.tcsb.receive(event);
        while let Some(event) = self.tcsb.next_causally_ready() {
            self.deliver(event);
        }
    }

    fn send(&mut self, op: L::Op) -> Option<Event<L::Op>> {
        if !self.state.is_enabled(&op) {
            info!("Operation is not enabled: {op:?}");
            return None;
        }
        let op = L::prepare(op);
        let event = self.tcsb.send(op);
        self.deliver(event.clone());
        Some(event)
    }

    fn pull(&mut self, since: Since) -> Batch<L::Op> {
        // assert_ne!(since.version().origin_id(), self.id);
        self.tcsb.pull(since)
    }

    fn query(&self) -> L::Value {
        self.state.eval()
    }

    fn update(&mut self, op: L::Op) {
        self.send(op).unwrap();
    }

    fn since(&self) -> Since {
        self.tcsb.since()
    }

    // #[instrument(skip(self, batch), fields(id = self.id))]
    fn receive_batch(&mut self, batch: Batch<<L as IsLog>::Op>) {
        info!("Receiving batch with {} events", batch.events.len());
        for event in batch.events() {
            self.receive(event);
        }
        // TODO: is it correct?
        // self.tcsb.update_version(batch.version());
    }

    fn id(&self) -> &ReplicaId {
        &self.id
    }

    fn bootstrap(id: String, members: &[&ReplicaId]) -> Self {
        let mut interner = Interner::new();
        let (idx, _) = interner.intern(&id);
        for member in members {
            interner.intern(member);
        }
        Self {
            id: TinyAsciiStr::from_str(&id).unwrap(),
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
    #[instrument(skip(self, event))]
    fn deliver(&mut self, event: Event<L::Op>) {
        info!("Delivering event: {event}");
        self.state.effect(event);
        let maybe_version = self.tcsb.is_stable();
        if let Some(version) = maybe_version {
            self.state.stabilize(version);
        }
    }

    #[cfg(test)]
    pub fn state(&self) -> &L {
        &self.state
    }

    #[cfg(feature = "fuzz")]
    pub fn num_delivered_events(&self) -> usize {
        self.tcsb.matrix_clock().origin_version().sum()
    }
}
