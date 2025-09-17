use std::fmt::Debug;

use tracing::{error, info};

use crate::{
    protocol::{
        clock::{matrix_clock::MatrixClock, version_vector::Version},
        event::{id::EventId, lamport::Lamport, Event},
        membership::{view::View, ReplicaIdx},
    },
    utils::mut_owner::Reader,
};

pub trait IsTcsb<O> {
    fn new(view: &Reader<View>, replica_idx: ReplicaIdx) -> Self;
    fn receive(&mut self, event: Event<O>);
    fn send(&mut self, op: O) -> Event<O>;
    fn next_causally_ready(&mut self) -> Option<Event<O>>;
    fn is_stable(&mut self) -> Option<&Version>;
    fn change_view(&mut self, new_view: &Reader<View>);
}

#[derive(Debug)]
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
    O: Clone + Debug,
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
        info!(
            "Checking for next causally ready event. Inbox: {:?}",
            self.inbox
                .iter()
                .map(|e| format!("{e}"))
                .collect::<Vec<_>>()
        );
        let idx = self
            .inbox
            .iter()
            .position(|event| self.is_causally_ready(event));
        if let Some(idx) = idx {
            let event = self.inbox.remove(idx);
            self.matrix_clock
                .origin_version_mut()
                .merge(event.version());
            self.matrix_clock
                .set_by_id(&event.id().origin_id(), event.version());
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

    /// Change the view of the TCSB.
    /// # Invariant
    /// The previous indices must be preserved.
    fn change_view(&mut self, new_view: &Reader<View>) {
        self.view = Reader::clone(new_view);
        self.matrix_clock.change_view(new_view);
    }
}

impl<O> Tcsb<O>
where
    O: Debug,
{
    fn is_valid(&self, event: &Event<O>) -> bool {
        // The event should not come from the local replica
        // TODO: improve the code
        if &event.id().origin_id() == self.view.borrow().get_id(self.replica_idx).unwrap() {
            error!("Event from local replica");
            return false;
        }
        // The event should not come from an unknown replica
        if !self.view.borrow().is_known(&event.id().origin_id()) {
            error!("Event from unknown replica");
            return false;
        }
        // The event should not be a duplicate, i.e. an event already received
        if self.is_duplicate(event) {
            error!("Event is a duplicate {event}");
            return false;
        }

        true
    }

    /// Check that an event is causally ready to be delivered.
    /// It checks that all the event's dependencies have already been delivered.
    /// # Performance
    /// `O(n)`
    fn is_causally_ready(&self, event: &Event<O>) -> bool {
        let version = self.matrix_clock.origin_version();
        info!(
            "Checking if event {} is causally ready. Current version: {}, event version: {}",
            event,
            version,
            event.version()
        );

        for (_, id) in self.view.borrow().members() {
            let local_seq = version.seq_by_id(id).unwrap_or(0);
            let event_seq = event.version().seq_by_id(id).unwrap_or(0);
            if id == &event.id().origin_id() {
                if local_seq + 1 != event_seq {
                    info!("Event {} is not causally ready", event);
                    return false;
                }
            } else if local_seq < event_seq {
                info!("Event {} is not causally ready", event);
                return false;
            }
        }

        true
    }

    fn is_duplicate(&self, event: &Event<O>) -> bool {
        let version = self
            .matrix_clock
            .version_by_id(&event.id().origin_id())
            .unwrap();
        version.origin_seq() >= event.id().seq()
    }

    /// Remove events from the outbox that have been delivered by every replica.
    fn prune_outbox(&mut self, lsv: &Version) {
        // Retain only the events that are not predecessors (including equal) to the last stable version
        self.outbox
            .retain(|event| !event.id().is_predecessor_of(lsv));
    }
}
