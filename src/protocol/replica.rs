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
    fn new(id: &ReplicaId) -> Self;
    fn receive(&mut self, event: Event<L::Op>);
    fn send(&mut self, op: L::Op) -> Event<L::Op>;
    fn pull(&mut self, since: Since) -> Batch<L::Op>;
    // TODO: Add support for custom queries
    fn query(&self) -> L::Value;
    fn update(&mut self, op: L::Op);
}

#[allow(dead_code)]
pub struct Replica<L, T> {
    id: ReplicaId,
    tcsb: T,
    state: L,
    membership: Membership,
}

impl<L, T> IsReplica<L> for Replica<L, T>
where
    L: IsLog,
    T: IsTcsb<L::Op>,
{
    fn new(id: &ReplicaId) -> Self {
        let membership = Membership::new(id);
        let view = membership.get_reader(id).unwrap();
        let replica_idx = view.borrow().get_idx(id).unwrap();
        Self {
            id: id.to_string(),
            tcsb: T::new(&view, replica_idx),
            state: L::new(),
            membership,
        }
    }

    fn receive(&mut self, event: Event<L::Op>) {
        self.tcsb.receive(event);
        while let Some(event) = self.tcsb.next_causally_ready() {
            self.deliver(event);
        }
    }

    fn send(&mut self, op: L::Op) -> Event<L::Op> {
        let op = L::prepare(op);
        let event = self.tcsb.send(op);
        self.deliver(event.clone());
        event
    }

    fn pull(&mut self, _since: Since) -> Batch<L::Op> {
        todo!()
    }

    fn query(&self) -> L::Value {
        self.state.eval()
    }

    fn update(&mut self, op: L::Op) {
        self.send(op);
    }
}

impl<L, T> Replica<L, T>
where
    L: IsLog,
    T: IsTcsb<L::Op>,
{
    fn deliver(&mut self, event: Event<L::Op>) {
        self.state.effect(event);
        let maybe_version = self.tcsb.is_stable();
        if let Some(version) = maybe_version {
            self.state.stabilize(version);
        }
    }
}
