use std::{cmp::Ordering, fmt::Debug};

#[cfg(feature = "test_utils")]
use crate::protocol::membership::ReplicaIdOwned;
use crate::{
    protocol::{
        broadcast::{
            batch::Batch,
            message::{BatchMessage, EventMessage, SinceMessage},
            since::Since,
        },
        clock::{matrix_clock::MatrixClock, version_vector::Version},
        event::{id::EventId, lamport::Lamport, Event},
    },
    utils::intern_str::{Interner, ReplicaIdx},
    HashMap, HashSet,
};

pub trait IsTcsb<O> {
    fn new(replica_idx: ReplicaIdx, interner: Interner) -> Self;
    fn receive(&mut self, message: EventMessage<O>);
    fn receive_batch(&mut self, message: BatchMessage<O>);
    fn send(&mut self, op: O) -> EventMessage<O>;
    fn since(&self) -> SinceMessage;
    fn pull(&mut self, since: SinceMessage) -> BatchMessage<O>;
    fn next_causally_ready(&mut self) -> Option<Event<O>>;
    fn is_stable(&mut self) -> Option<&Version>;
    fn update_version(&mut self, version: &Version);
}

#[derive(Debug)]
pub struct Tcsb<O> {
    /// Received events not yet causally ready.
    /// It contains only events from other replicas than the local one.
    inbox: HashMap<EventId, Event<O>>,
    /// Events waiting to be broadcast.
    /// It contains events from all replicas, including the local one.
    outbox: HashMap<EventId, Event<O>>,
    matrix_clock: MatrixClock,
    last_stable_version: Version,
    replica_idx: ReplicaIdx,
    interner: Interner,
}

impl<O> IsTcsb<O> for Tcsb<O>
where
    O: Clone + Debug,
{
    fn new(replica_idx: ReplicaIdx, interner: Interner) -> Self {
        let resolver = interner.resolver();
        Self {
            inbox: HashMap::default(),
            outbox: HashMap::default(),
            matrix_clock: MatrixClock::new(replica_idx, resolver.clone()),
            last_stable_version: Version::new(replica_idx, resolver.clone()),
            interner,
            replica_idx,
        }
    }

    fn receive(&mut self, message: EventMessage<O>) {
        // TODO: do the checks before internalizing (i.e, before adding new replicas to the matrix clock)
        let event = self.internalize_event(message);
        if self.is_valid(&event) {
            self.inbox.insert(event.id().clone(), event.clone());
            self.outbox.insert(event.id().clone(), event);
        }
    }

    fn receive_batch(&mut self, message: BatchMessage<O>) {
        let batch = self.internalize_batch(message);
        for event in batch.into_events() {
            if self.is_valid(&event) {
                self.inbox.insert(event.id().clone(), event.clone());
                self.outbox.insert(event.id().clone(), event);
            }
        }
    }

    fn send(&mut self, op: O) -> EventMessage<O> {
        let seq = self.matrix_clock.origin_version_mut().increment();
        let version = self.matrix_clock.origin_version();
        let lamport = Lamport::from(version);
        let event_id = EventId::new(self.replica_idx, seq, self.interner.resolver().clone());
        let event = Event::new(event_id, lamport, op, version.clone());
        self.outbox.insert(event.id().clone(), event.clone());
        EventMessage::new(event, self.interner.resolver().clone())
    }

    fn next_causally_ready(&mut self) -> Option<Event<O>> {
        let maybe_event = self
            .inbox
            .values()
            .find(|e| self.is_causally_ready(e))
            .cloned();
        if let Some(event) = maybe_event {
            self.inbox.remove(event.id()).unwrap();
            self.matrix_clock.origin_version_mut().join(event.version());
            self.matrix_clock
                .set_by_idx(event.id().idx(), event.version().clone());
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

    fn update_version(&mut self, version: &Version) {
        self.matrix_clock.origin_version_mut().join(version);
        self.matrix_clock
            .set_by_idx(version.origin_idx(), version.clone());
    }

    /// # Performance
    /// `Θ(n)` where `n` is the number of events in the outbox.
    fn pull(&mut self, since: SinceMessage) -> BatchMessage<O> {
        let since = self.internalize_since(since);
        let events: Vec<Event<O>> = self
            .outbox
            .iter()
            .filter(|(id, e)| {
                !since.except().contains(id)
                    && *id.origin_id() != *since.version().origin_id()
                    && match e.version().partial_cmp(since.version()) {
                        Some(Ordering::Greater) | Some(Ordering::Equal) | None => true,
                        Some(Ordering::Less) => false,
                    }
            })
            .map(|(_, e)| e)
            .cloned()
            .collect();
        let batch = Batch::new(events, self.matrix_clock.origin_version().clone());
        BatchMessage::new(batch, self.interner.resolver().clone())
    }

    fn since(&self) -> SinceMessage {
        #[allow(clippy::mutable_key_type)]
        let except: HashSet<EventId> = self.inbox.keys().cloned().collect();
        let version = self.matrix_clock.origin_version().clone();
        let since = Since::new(version, except);
        SinceMessage::new(since, self.interner.resolver().clone())
    }
}

impl<O> Tcsb<O>
where
    O: Debug + Clone,
{
    fn is_valid(&self, event: &Event<O>) -> bool {
        // TODO: reject events from unknown replicas (?)

        // The event should not come from the local replica
        if event.id().idx() == self.replica_idx {
            // println!("Event from local replica");
            // return false;
            panic!("Received event from local replica");
        }

        // The event should not be a duplicate, i.e. an event already received
        if self.is_duplicate(event) {
            // println!("Event is a duplicate {event}");
            return false;
        }

        // The event should not be stale, i.e. an event that is not greater than the last stable version
        if self.is_stale(event) {
            // println!("Event is stale {event}");
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
        let event_version = event.version();

        for (idx, event_seq) in event_version.iter() {
            let local_seq = version.seq_by_idx(idx);
            if idx == event.id().idx() {
                if local_seq + 1 != event_seq {
                    return false;
                }
            } else if local_seq < event_seq {
                return false;
            }
        }

        true
    }

    fn is_duplicate(&self, event: &Event<O>) -> bool {
        let version = self.matrix_clock.version_by_idx(event.id().idx()).unwrap();
        event.id().seq() <= version.origin_seq()
    }

    /// Return `true` if the event is not greater than the last stable version
    /// i.e., if the sending replica needs a state transfer.
    fn is_stale(&self, event: &Event<O>) -> bool {
        match event.version().partial_cmp(&self.last_stable_version) {
            Some(Ordering::Less) | Some(Ordering::Equal) | None => true,
            Some(Ordering::Greater) => false,
        }
    }

    /// Remove events from the outbox that have been delivered by every replica.
    fn prune_outbox(&mut self, lsv: &Version) {
        // Retain only the events that are not predecessors (including equal) to the last stable version
        self.outbox.retain(|id, _| !id.is_predecessor_of(lsv));
    }

    /// Internalize an event by mapping its replica IDs to local indices.
    /// If a replica ID is unknown, it is added to the interner and the matrix clock.
    fn internalize_event(&mut self, message: EventMessage<O>) -> Event<O> {
        let (from, is_new) = self.interner.intern(message.event().id().origin_id());

        if is_new {
            self.matrix_clock.add_replica(from);
        }

        let new_indices = self.interner.update_translation(from, message.resolver());

        for idx in new_indices {
            self.matrix_clock.add_replica(idx);
        }

        let event_id = EventId::new(
            from,
            message.event().id().seq(),
            self.interner.resolver().clone(),
        );
        let mut version = Version::new(from, self.interner.resolver().clone());

        for (remote_idx, seq) in message.event().version().iter() {
            let idx = self.interner.translate(from, remote_idx);
            version.set_by_idx(idx, seq);
        }

        let event = message.event();

        Event::new(
            event_id,
            event.lamport().clone(),
            event.op().clone(),
            version,
        )
    }

    fn internalize_since(&mut self, message: SinceMessage) -> Since {
        let since = message.since();

        let (from, is_new) = self.interner.intern(since.origin_id());

        if is_new {
            self.matrix_clock.add_replica(from);
        }

        let new_indices = self.interner.update_translation(from, message.resolver());

        for idx in new_indices {
            self.matrix_clock.add_replica(idx);
        }

        let mut version = Version::new(from, self.interner.resolver().clone());

        for (remote_idx, seq) in since.version().iter() {
            let idx = self.interner.translate(from, remote_idx);
            version.set_by_idx(idx, seq);
        }

        #[allow(clippy::mutable_key_type)]
        let except: HashSet<EventId> = since
            .except()
            .iter()
            .map(|e_id| {
                let idx = self.interner.translate(from, e_id.idx());
                EventId::new(idx, e_id.seq(), self.interner.resolver().clone())
            })
            .collect();

        Since::new(version, except)
    }

    fn internalize_batch(&mut self, message: BatchMessage<O>) -> Batch<O> {
        let (batch, resolver) = message.into_parts();
        // Intern the batch origin ID
        let (from, is_new) = self.interner.intern(batch.origin_id());

        // If a new replica ID was added, update the matrix clock
        if is_new {
            self.matrix_clock.add_replica(from);
        }

        // Update the translation between our resolver and the batch resolver
        let new_indices = self.interner.update_translation(from, &resolver);

        // If new replica IDs were discovered during translation update, update the matrix clock
        for idx in new_indices {
            self.matrix_clock.add_replica(idx);
        }

        // Rebuild the batch version with local indices
        let mut version = Version::new(from, self.interner.resolver().clone());
        for (remote_idx, seq) in batch.version().iter() {
            let idx = self.interner.translate(from, remote_idx);
            version.set_by_idx(idx, seq);
        }

        let mut events = Vec::with_capacity(batch.events().len());
        // For each event, translate its event ID and version to our local indices
        for event in batch.into_events() {
            // Event origin idx in our mapping
            let event_origin_idx = self.interner.translate(from, event.id().idx());
            let event_id = EventId::new(
                event_origin_idx,
                event.id().seq(),
                self.interner.resolver().clone(),
            );
            let mut version = Version::new(event_origin_idx, self.interner.resolver().clone());
            for (remote_idx, seq) in event.version().iter() {
                let idx = self.interner.translate(from, remote_idx);
                version.set_by_idx(idx, seq);
            }

            let e = Event::new(
                event_id,
                event.lamport().clone(),
                event.op().clone(),
                version,
            );
            events.push(e);
        }

        Batch::new(events, version)
    }
}

#[cfg(feature = "test_utils")]
pub trait IsTcsbTest<O>: IsTcsb<O> {
    fn matrix_clock(&self) -> &MatrixClock;
    fn members(&self) -> Vec<ReplicaIdOwned>;
    fn inbox<'a>(&'a self) -> impl Iterator<Item = &'a Event<O>>
    where
        O: 'a;
    fn inbox_len(&self) -> usize;
    fn outbox<'a>(&'a self) -> impl Iterator<Item = &'a Event<O>>
    where
        O: 'a;
    fn outbox_len(&self) -> usize;
}

impl<O> IsTcsbTest<O> for Tcsb<O>
where
    O: Debug + Clone,
{
    fn matrix_clock(&self) -> &MatrixClock {
        &self.matrix_clock
    }

    fn members(&self) -> Vec<ReplicaIdOwned> {
        self.interner.resolver().into_vec()
    }

    fn inbox<'a>(&'a self) -> impl Iterator<Item = &'a Event<O>>
    where
        O: 'a,
    {
        self.inbox.values()
    }

    fn inbox_len(&self) -> usize {
        self.inbox.len()
    }

    fn outbox<'a>(&'a self) -> impl Iterator<Item = &'a Event<O>>
    where
        O: 'a,
    {
        self.outbox.values()
    }

    fn outbox_len(&self) -> usize {
        self.outbox.len()
    }
}
