use crate::{
    protocol::{
        clock::{matrix_clock::MatrixClock, version_vector::Version},
        event::{id::EventId, lamport::Lamport, Event},
        membership::{ReplicaIdx, View},
    },
    utils::mut_owner::Reader,
};

pub trait IsTcsb<O> {
    fn new(view: &Reader<View>, replica_idx: ReplicaIdx) -> Self;
    fn receive(&mut self, event: Event<O>);
    fn send(&mut self, op: O) -> Event<O>;
    fn next_causally_ready(&mut self) -> Option<Event<O>>;
    fn is_stable(&mut self) -> Option<&Version>;
}

pub struct Tcsb<O> {
    /// Received events not yet causally ready.
    /// It contains only events from other replicas than the local one.
    inbox: Vec<Event<O>>,
    /// Events waiting to be broadcast.
    /// It contains events from all replicas, including the local one.
    outbox: Vec<Event<O>>,
    matrix_clock: MatrixClock,
    last_stable_version: Version,
    view: Reader<View>,
    replica_idx: ReplicaIdx,
}

impl<O> IsTcsb<O> for Tcsb<O>
where
    O: Clone,
{
    fn new(view: &Reader<View>, replica_idx: ReplicaIdx) -> Self {
        Self {
            inbox: Vec::new(),
            outbox: Vec::new(),
            matrix_clock: MatrixClock::new(view, replica_idx),
            last_stable_version: Version::new(view, replica_idx),
            view: Reader::clone(view),
            replica_idx,
        }
    }

    fn receive(&mut self, event: Event<O>) {
        if self.is_valid(&event) {
            self.inbox.push(event.clone());
            self.outbox.push(event);
        }
    }

    fn send(&mut self, op: O) -> Event<O> {
        let seq = self.matrix_clock.origin_version_mut().increment();
        let version = self.matrix_clock.origin_version();
        let lamport = Lamport::from(version);
        let event_id = EventId::new(self.replica_idx, seq, self.view.clone());
        Event::new(event_id, lamport, op, version.clone())
    }

    fn next_causally_ready(&mut self) -> Option<Event<O>> {
        let idx = self
            .inbox
            .iter()
            .position(|event| self.is_causally_ready(event));
        if let Some(idx) = idx {
            let event = self.inbox.remove(idx);
            self.matrix_clock
                .origin_version_mut()
                .merge(event.version());
            return Some(event);
        }
        None
    }

    fn is_stable(&mut self) -> Option<&Version> {
        let lsv = self.matrix_clock.column_wise_min();
        if lsv == self.last_stable_version {
            None
        } else {
            self.prune_outbox(&lsv);
            self.last_stable_version = lsv;
            Some(&self.last_stable_version)
        }
    }
}

impl<O> Tcsb<O> {
    fn is_valid(&self, event: &Event<O>) -> bool {
        // The event should not come from the local replica
        if event.id().origin_idx() == self.replica_idx {
            return false;
        }
        // The event should not come from an unknown replica
        if !self.view.borrow().is_known(&event.id().origin_id()) {
            return false;
        }
        // The event should not be a duplicate, i.e. an event already received
        if self.is_duplicate(event) {
            return false;
        }

        true
    }

    fn is_causally_ready(&self, event: &Event<O>) -> bool {
        let version = self
            .matrix_clock
            .get_by_idx(event.id().origin_idx())
            .unwrap();
        if version.origin_seq() + 1 != event.id().seq() {
            return false;
        }

        true
    }

    fn is_duplicate(&self, event: &Event<O>) -> bool {
        let version = self
            .matrix_clock
            .get_by_idx(event.id().origin_idx())
            .unwrap();
        version.origin_seq() >= event.id().seq()
    }

    fn prune_outbox(&mut self, lsv: &Version) {
        self.outbox
            .retain(|event| !event.id().is_predecessor_of(lsv));
    }
}
