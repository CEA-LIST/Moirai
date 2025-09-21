use std::fmt::Debug;

use tracing::{info, instrument};

use crate::protocol::{
    broadcast::{batch::Batch, since::Since, tcsb::IsTcsb},
    event::Event,
    membership::{Membership, ReplicaId},
    state::log::IsLog,
};

pub trait IsReplica<L>
where
    L: IsLog,
{
    fn new(id: ReplicaId) -> Self;
    fn id(&self) -> &ReplicaId;
    fn bootstrap(id: ReplicaId, membership: Membership) -> Self;
    fn receive(&mut self, event: Event<L::Op>);
    fn receive_batch(&mut self, batch: Batch<L::Op>);
    fn since(&self) -> Since;
    fn send(&mut self, op: L::Op) -> Event<L::Op>;
    fn pull(&mut self, since: Since) -> Batch<L::Op>;
    // TODO: Add support for custom queries
    fn query(&self) -> L::Value;
    fn update(&mut self, op: L::Op);
}

#[derive(Debug)]
pub struct Replica<L, T> {
    id: ReplicaId,
    tcsb: T,
    state: L,
    #[allow(dead_code)]
    membership: Membership,
}

impl<L, T> IsReplica<L> for Replica<L, T>
where
    L: IsLog,
    T: IsTcsb<L::Op> + Debug,
{
    fn new(id: ReplicaId) -> Self {
        let membership = Membership::new(&id);
        let view = membership.get_reader(&id).unwrap();
        let replica_idx = view.borrow().get_idx(&id).unwrap();
        Self {
            id: id.to_string(),
            tcsb: T::new(&view, replica_idx),
            state: L::new(),
            membership,
        }
    }

    #[instrument(skip(self, event), fields(id = self.id))]
    fn receive(&mut self, event: Event<L::Op>) {
        self.tcsb.receive(event);
        while let Some(event) = self.tcsb.next_causally_ready() {
            self.deliver(event);
        }
    }

    #[instrument(skip(self, op), fields(id = self.id))]
    fn send(&mut self, op: L::Op) -> Event<L::Op> {
        let op = L::prepare(op);
        let event = self.tcsb.send(op);
        self.deliver(event.clone());
        event
    }

    fn pull(&mut self, since: Since) -> Batch<L::Op> {
        assert_ne!(since.version().origin_id(), self.id);
        self.tcsb.pull(since)
    }

    fn query(&self) -> L::Value {
        self.state.eval()
    }

    fn update(&mut self, op: L::Op) {
        self.send(op);
    }

    fn bootstrap(id: ReplicaId, membership: Membership) -> Self {
        let view = membership.get_reader(&id).unwrap();
        let replica_idx = view.borrow().get_idx(&id).unwrap();
        Self {
            id: id.to_string(),
            tcsb: T::new(&view, replica_idx),
            state: L::new(),
            membership,
        }
    }

    fn since(&self) -> Since {
        self.tcsb.since()
    }

    #[instrument(skip(self, batch), fields(id = self.id))]
    fn receive_batch(&mut self, batch: Batch<<L as IsLog>::Op>) {
        info!("Receiving batch with {} events", batch.events.len());
        for event in batch.events() {
            self.receive(event.clone());
        }
        // TODO: is it correct?
        // self.tcsb.update_version(batch.version());
    }

    fn id(&self) -> &ReplicaId {
        &self.id
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
