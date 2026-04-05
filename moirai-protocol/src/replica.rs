use std::fmt::Debug;

#[cfg(feature = "test_utils")]
use crate::broadcast::tcsb::IsTcsbTest;
#[cfg(feature = "sink")]
use crate::state::{
    sink::SinkOwnership,
    object_path::ObjectPath,
    sink::SinkCollector,
};
use crate::{
    broadcast::{
        message::{BatchMessage, EventMessage, SinceMessage},
        tcsb::IsTcsb,
    },
    crdt::{eval::EvalNested, query::QueryOperation},
    event::Event,
    state::log::IsLog,
    utils::intern_str::Interner,
};

pub type ReplicaId = str;
pub type ReplicaIdOwned = String;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReplicaIdx(pub usize);

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
        #[cfg(feature = "sink")]
        let mut sink = SinkCollector::new();
        #[cfg(feature = "sink")]
        let object_path = ObjectPath::new("root"); // TODO: pass actual path
        self.state.effect(
            event,
            #[cfg(feature = "sink")]
            object_path.clone(),
            #[cfg(feature = "sink")]
            &mut sink,
            #[cfg(feature = "sink")]
            SinkOwnership::Owned,
        );
        let maybe_version = self.tcsb.is_stable();
        if let Some(version) = maybe_version {
            self.state.stabilize(version);
        }
    }
}

#[cfg(feature = "test_utils")]
impl<L, T> Replica<L, T>
where
    L: IsLog,
    T: IsTcsbTest<L::Op>,
{
    pub fn tcsb(&self) -> &T {
        &self.tcsb
    }

    pub fn num_delivered_events(&self) -> usize {
        self.tcsb.matrix_clock().origin_version().sum()
    }

    pub fn state(&self) -> &L {
        &self.state
    }
}
