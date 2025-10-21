use std::fmt::Debug;

use crate::{
    protocol::{
        broadcast::{
            message::{BatchMessage, EventMessage, SinceMessage},
            tcsb::{IsTcsb, IsTcsbTest},
        },
        crdt::pure_crdt::QueryOperation,
        event::Event,
        membership::{ReplicaId, ReplicaIdOwned},
        state::log::{EvalNested, IsLog, IsLogTest},
    },
    utils::intern_str::Interner,
};

pub trait IsReplica<L>
where
    L: IsLog,
{
    fn new(id: ReplicaIdOwned) -> Self;
    fn id(&self) -> &ReplicaId;
    fn receive(&mut self, message: EventMessage<L::Op>);
    fn receive_batch(&mut self, message: BatchMessage<L::Op>);
    fn since(&self) -> SinceMessage;
    fn send(&mut self, op: L::Op) -> Option<EventMessage<L::Op>>;
    fn pull(&mut self, since: SinceMessage) -> BatchMessage<L::Op>;
    // TODO: Add support for custom queries
    fn query<Q: QueryOperation>(&self, q: Q) -> Q::Response
    where
        L: EvalNested<Q>;
    fn update(&mut self, op: L::Op);
    fn bootstrap(id: ReplicaIdOwned, members: &[&ReplicaId]) -> Self;
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

    fn receive(&mut self, message: EventMessage<L::Op>) {
        self.tcsb.receive(message);
        while let Some(e) = self.tcsb.next_causally_ready() {
            self.deliver(e);
        }
    }

    fn receive_batch(&mut self, message: BatchMessage<L::Op>) {
        self.tcsb.receive_batch(message);
        while let Some(e) = self.tcsb.next_causally_ready() {
            self.deliver(e);
        }
    }

    fn send(&mut self, op: L::Op) -> Option<EventMessage<L::Op>> {
        if !self.state.is_enabled(&op) {
            return None;
        }
        let op = L::prepare(op);
        let message = self.tcsb.send(op);
        self.deliver(message.event().clone());
        Some(message)
    }

    fn pull(&mut self, since: SinceMessage) -> BatchMessage<L::Op> {
        self.tcsb.pull(since)
    }

    fn query<Q: QueryOperation>(&self, q: Q) -> Q::Response
    where
        L: EvalNested<Q>,
    {
        self.state.eval(q)
    }

    fn update(&mut self, op: L::Op) {
        self.send(op).unwrap();
    }

    fn since(&self) -> SinceMessage {
        self.tcsb.since()
    }

    fn id(&self) -> &ReplicaId {
        &self.id
    }

    fn bootstrap(id: ReplicaIdOwned, members: &[&ReplicaId]) -> Self {
        let mut interner = Interner::new();
        let (idx, _) = interner.intern(&id);
        for member in members {
            interner.intern(member);
        }
        Self {
            id,
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
    fn deliver(&mut self, event: Event<L::Op>) {
        self.state.effect(event);
        let maybe_version = self.tcsb.is_stable();
        if let Some(version) = maybe_version {
            self.state.stabilize(version);
        }
    }
}

impl<L, T> Replica<L, T>
where
    L: IsLogTest,
    T: IsTcsb<L::Op>,
{
    #[cfg(feature = "test_utils")]
    pub fn state(&self) -> &L {
        &self.state
    }
}

impl<L, T> Replica<L, T>
where
    L: IsLog,
    T: IsTcsbTest<L::Op>,
{
    #[cfg(feature = "test_utils")]
    pub fn tcsb(&self) -> &T {
        &self.tcsb
    }

    #[cfg(feature = "test_utils")]
    pub fn num_delivered_events(&self) -> usize {
        self.tcsb.matrix_clock().origin_version().sum()
    }
}
