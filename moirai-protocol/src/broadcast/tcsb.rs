use std::{collections::BTreeMap, fmt::Debug};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    broadcast::{
        batch::Batch,
        internalizer::{Interner, Resolver, SharedInterner, lock_interner},
        message::{BatchMessage, EventMessage, SinceMessage},
        since::Since,
    },
    clock::{causal_frontier::CausalFrontier, matrix_clock::MatrixClock, version_vector::Version},
    event::{Event, id::EventId, lamport::Lamport},
    replica::{ReplicaId, ReplicaIdx},
};

/// Services of the Tagged Causal Stable Broadcast communication protocol.
pub trait IsTcsb<O> {
    /// Create a new TCSB instance for a replica with the given id and interner.
    fn new(replica_idx: ReplicaIdx, interner: Interner) -> Self;
    /// Receive a message containing a single event from another replica.
    fn receive(&mut self, message: EventMessage<O>);
    /// Receive a message containing a batch of events from another replica.
    fn receive_batch(&mut self, message: BatchMessage<O>);
    /// Transform an operation into a message containing an event to be broadcast to other replicas.
    fn send(&mut self, op: O) -> EventMessage<O>;
    /// Return a message containing the version vector and except set to pull events from other replicas.
    fn since(&self) -> SinceMessage;
    /// Return the batch of events since the given version and are not in the except set.
    fn pull(&mut self, since: SinceMessage) -> BatchMessage<O>;
    /// Return the next causally ready event to be delivered, if any.
    fn next_causally_ready(&mut self) -> Option<Event<O>>;
    /// Return the new stable causal frontier if it has advanced.
    fn is_stable(&mut self) -> Option<&CausalFrontier>;
    /// Return the replica IDs of all members in the system
    fn members(&self) -> Vec<ReplicaId>;
    /// Return the number of members in the system.
    fn members_len(&self) -> usize;
}

#[derive(Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct Tcsb<O> {
    replica_idx: ReplicaIdx,
    /// Received events not yet causally ready.
    /// It contains only events from other replicas than the local one.
    inbox: FxHashMap<EventId, Event<O>>,
    /// Events waiting to be broadcast.
    /// It contains events from all replicas, including the local one.
    /// Organized by replica index and then by sequence number for efficient range queries.
    outbox: FxHashMap<ReplicaIdx, BTreeMap<usize, Event<O>>>,
    /// The matrix clock tracks the latest known version of each replica, including the local one.
    matrix_clock: MatrixClock,
    /// Component-wise cut containing every event delivered by all replicas.
    last_stable_version: CausalFrontier,
    /// Local mapping between remote replica IDs and local replica indices, and resolver for translating between them.
    interner: SharedInterner,
    /// The indices of the columns that were updated in the last matrix clock update, used for efficient stable version computation.
    last_updated_columns: Vec<ReplicaIdx>,
}

/// Causal bookkeeping transferred alongside a log, using the donor's member indices.
/// Pending events arrive through the recipient's stream, rather than being delivered by adoption.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StateSnapshot<O> {
    delivered: Version,
    rows: Vec<Version>,
    stable: CausalFrontier,
    outbox: Vec<Event<O>>,
}

impl<O> IsTcsb<O> for Tcsb<O>
where
    O: Clone + Debug,
{
    fn new(replica_idx: ReplicaIdx, interner: Interner) -> Self {
        Self::with_interner(replica_idx, interner.into_shared())
    }

    fn receive(&mut self, message: EventMessage<O>) {
        self.sync_members();
        // TODO: do the checks before internalizing (i.e, before adding new replicas to the matrix clock)
        let event = self.internalize_event(message);
        self.record(event);
    }

    fn receive_batch(&mut self, message: BatchMessage<O>) {
        self.sync_members();
        let batch = self.internalize_batch(message);
        for event in batch.into_events() {
            self.record(event);
        }
    }

    fn send(&mut self, op: O) -> EventMessage<O> {
        self.sync_members();
        let seq = self.matrix_clock.origin_version_mut().increment();
        let version = self.matrix_clock.origin_version();
        let lamport = Lamport::from(version);
        let event_id = EventId::new(
            self.replica_idx,
            seq,
            self.matrix_clock.origin_version().resolver().clone(),
        );
        let event = Event::new(event_id, lamport, op, version.clone());
        self.outbox
            .entry(event.id().idx())
            .or_default()
            .insert(event.id().seq(), event.clone());
        EventMessage::new(event, self.matrix_clock.origin_version().resolver().clone())
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
            self.last_updated_columns = self
                .matrix_clock
                .set_by_idx_incremental(event.id().idx(), event.version().clone());
            return Some(event);
        }
        None
    }

    fn is_stable(&mut self) -> Option<&CausalFrontier> {
        let lsv = self
            .matrix_clock
            .column_wise_min_incremental(&self.last_stable_version, &self.last_updated_columns);
        if lsv == self.last_stable_version {
            None
        } else {
            self.prune_outbox(&lsv);
            self.last_stable_version = lsv;

            Some(&self.last_stable_version)
        }
    }

    /// # Complexity
    /// `O(n log n + k log k)` where `n` is the number of replicas and `k` is the number of events returned.
    fn pull(&mut self, since: SinceMessage) -> BatchMessage<O> {
        self.sync_members();
        let since = self.internalize_since(since);
        let mut events = Vec::new();

        // Iterate over replicas in the version vector and only fetch needed ranges
        for (replica_idx, req_seq) in since.version().iter() {
            // Skip events originating from the requesting replica itself
            if replica_idx == since.version().origin_idx() {
                continue;
            }

            if let Some(events_by_seq) = self.outbox.get(&replica_idx) {
                // Range query: get all events with sequence > req_seq
                for (_, event) in events_by_seq.range((req_seq + 1)..) {
                    if !since.except().contains(event.id()) {
                        events.push(event.clone());
                    }
                }
            }
        }

        let batch = Batch::new(events, self.matrix_clock.origin_version().clone());
        BatchMessage::new(batch, self.matrix_clock.origin_version().resolver().clone())
    }

    fn since(&self) -> SinceMessage {
        #[allow(clippy::mutable_key_type)]
        let except: FxHashSet<EventId> = self.inbox.keys().cloned().collect();
        let version = self.matrix_clock.origin_version().clone();
        let since = Since::new(version, except);
        SinceMessage::new(since, self.matrix_clock.origin_version().resolver().clone())
    }

    fn members(&self) -> Vec<ReplicaId> {
        self.matrix_clock.origin_version().resolver().into_vec()
    }

    fn members_len(&self) -> usize {
        self.matrix_clock.origin_version().resolver().len()
    }
}

impl<O> Tcsb<O>
where
    O: Debug + Clone,
{
    pub fn snapshot(&self) -> StateSnapshot<O> {
        StateSnapshot {
            delivered: self.matrix_clock.origin_version().clone(),
            rows: (0..self.matrix_clock.origin_version().resolver().len())
                .map(|idx| {
                    self.matrix_clock
                        .version_by_idx(ReplicaIdx(idx))
                        .cloned()
                        .unwrap_or_else(|| {
                            Version::new(
                                ReplicaIdx(idx),
                                self.matrix_clock.origin_version().resolver().clone(),
                            )
                        })
                })
                .collect(),
            stable: self.last_stable_version.clone(),
            outbox: self
                .outbox
                .values()
                .flat_map(|row| row.values().cloned())
                .collect(),
        }
    }

    pub(crate) fn adopt(&mut self, snapshot: StateSnapshot<O>, id: &ReplicaId) -> Version {
        // Keep donor indices so IDs embedded in the log remain meaningful.
        let mut interner = Interner::new();
        for member in snapshot.delivered.resolver().into_vec() {
            interner.intern(&member);
        }
        for member in self.members() {
            interner.intern(&member);
        }
        interner.intern(id);
        {
            let mut table = lock_interner(&self.interner);
            if table.resolver().agrees_with(&interner.resolver()) {
                for member in interner.resolver().into_vec() {
                    table.intern(&member);
                }
            } else {
                *table = interner;
            }
        }
        let idx = lock_interner(&self.interner).get(id).unwrap();
        let mut adopted = Self::with_interner(idx, self.interner.clone());
        let resolver = lock_interner(&adopted.interner).resolver().clone();
        let donor = snapshot.delivered.rebind(&resolver);
        for row in snapshot.rows {
            let row = row.rebind(&resolver);
            adopted.matrix_clock.set_by_idx(row.origin_idx(), row);
        }
        let mut local = Version::new(idx, resolver.clone());
        for (column, seq) in donor.iter() {
            local.set_by_idx(column, seq);
        }
        adopted.matrix_clock.set_by_idx(idx, local);
        adopted.last_stable_version = snapshot.stable;
        adopted.last_stable_version.rebind(&resolver);
        for event in snapshot.outbox {
            let mut event_id = event.id().clone();
            event_id.rebind(&resolver);
            let version = event.version().rebind(&resolver);
            let event = Event::new(event_id, *event.lamport(), event.op().clone(), version);
            adopted
                .outbox
                .entry(event.id().idx())
                .or_default()
                .insert(event.id().seq(), event);
        }
        *self = adopted;
        donor
    }

    /// Record a received event in the inbox and outbox if it is valid.
    fn record(&mut self, event: Event<O>) {
        if self.is_valid(&event) {
            self.inbox.insert(event.id().clone(), event.clone());
            self.outbox
                .entry(event.id().idx())
                .or_default()
                .insert(event.id().seq(), event);
        }
    }

    fn is_valid(&self, event: &Event<O>) -> bool {
        // TODO: reject events from unknown replicas (?)

        // The event should not come from the local replica
        if event.id().idx() == self.replica_idx {
            panic!("Received event from local replica");
        }

        // The event should not be a duplicate, i.e. an event already received
        if self.is_duplicate(event) {
            return false;
        }

        // The event should not be stale, i.e. an event that is not greater than the last stable version
        if self.is_stale(event) {
            return false;
        }

        true
    }

    /// Check that an event is causally ready to be delivered.
    /// It checks that all the event's dependencies have already been delivered.
    /// # Complexity
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
        self.last_stable_version.contains(event.id())
            || !event.version().includes_frontier(&self.last_stable_version)
    }

    /// Remove events from the outbox that have been delivered by every replica.
    fn prune_outbox(&mut self, lsv: &CausalFrontier) {
        // For each replica, retain only events with sequence greater than the replica's last stable seq
        for (replica_idx, events_by_seq) in self.outbox.iter_mut() {
            let lsv_seq = lsv.seq_by_idx(*replica_idx);
            events_by_seq.retain(|seq, _| *seq > lsv_seq);
        }
        // Remove empty replica entries
        self.outbox
            .retain(|_, events_by_seq| !events_by_seq.is_empty());
    }

    /// Internalize an event by mapping its replica IDs to local indices.
    /// If a replica ID is unknown, it is added to the interner and the matrix clock.
    fn internalize_event(&mut self, message: EventMessage<O>) -> Event<O> {
        let event = message.event();

        let (from, version) =
            self.internalize(event.version(), event.id().origin_id(), message.resolver());

        let op = event.op().clone();

        let event_id = EventId::new(
            from,
            event.id().seq(),
            self.matrix_clock.origin_version().resolver().clone(),
        );

        Event::new(event_id, *event.lamport(), op, version)
    }

    fn internalize_since(&mut self, message: SinceMessage) -> Since {
        let since = message.since();

        let (from, version) = self.internalize(
            since.version(),
            since.version().origin_id(),
            message.resolver(),
        );

        #[allow(clippy::mutable_key_type)]
        let except: FxHashSet<EventId> = since
            .except()
            .iter()
            .map(|e_id| {
                let idx = lock_interner(&self.interner).translate(from, e_id.idx());
                EventId::new(
                    idx,
                    e_id.seq(),
                    self.matrix_clock.origin_version().resolver().clone(),
                )
            })
            .collect();

        Since::new(version, except)
    }

    /// Internalize a batch of events by mapping their replica IDs to local indices.
    fn internalize_batch(&mut self, message: BatchMessage<O>) -> Batch<O> {
        let (batch, resolver) = message.into_parts();

        let (from, version) = self.internalize(batch.version(), batch.origin_id(), &resolver);

        let mut events = Vec::with_capacity(batch.events().len());
        // For each event, translate its event ID and version to our local indices
        for event in batch.into_events() {
            // Event origin idx in our mapping
            let event_origin_idx = lock_interner(&self.interner).translate(from, event.id().idx());
            let event_id = EventId::new(
                event_origin_idx,
                event.id().seq(),
                self.matrix_clock.origin_version().resolver().clone(),
            );
            let version = self.internalize_version_from(event_origin_idx, from, event.version());
            let op = event.op().clone();
            let e = Event::new(event_id, *event.lamport(), op, version);
            events.push(e);
        }

        Batch::new(events, version)
    }

    fn internalize(
        &mut self,
        foreign_version: &Version,
        origin_id: &ReplicaId,
        foreign_resolver: &Resolver,
    ) -> (ReplicaIdx, Version) {
        let from = {
            let mut table = lock_interner(&self.interner);
            let (from, _) = table.intern(origin_id);
            table.update_translation(from, foreign_resolver);
            from
        };
        self.sync_members();

        let version = self.internalize_version(from, foreign_version);

        (from, version)
    }

    fn internalize_version(&self, from: ReplicaIdx, foreign_version: &Version) -> Version {
        self.internalize_version_from(from, from, foreign_version)
    }

    fn internalize_version_from(
        &self,
        origin_idx: ReplicaIdx,
        translation_from: ReplicaIdx,
        foreign_version: &Version,
    ) -> Version {
        // Rebuild the batch version with local indices
        let mut version = Version::new(
            origin_idx,
            self.matrix_clock.origin_version().resolver().clone(),
        );

        for (remote_idx, seq) in foreign_version.iter() {
            let idx = lock_interner(&self.interner).translate(translation_from, remote_idx);
            version.set_by_idx(idx, seq);
        }

        version
    }
}

#[cfg(feature = "test_utils")]
impl<O> Tcsb<O>
where
    O: Debug + Clone,
{
    pub fn matrix_clock(&self) -> &MatrixClock {
        &self.matrix_clock
    }

    pub fn last_stable_version(&self) -> &CausalFrontier {
        &self.last_stable_version
    }

    /// The number of causally stable events is the number of events that have been delivered and are causally stable.
    /// It is the sum of the components of the LSV.
    pub fn causally_stable_events_count(&self) -> usize {
        self.last_stable_version.sum()
    }

    pub fn inbox<'a>(&'a self) -> impl Iterator<Item = &'a Event<O>>
    where
        Self: 'a,
    {
        self.inbox.values()
    }

    pub fn inbox_len(&self) -> usize {
        self.inbox.len()
    }

    pub fn outbox<'a>(&'a self) -> impl Iterator<Item = &'a Event<O>>
    where
        Self: 'a,
    {
        self.outbox
            .values()
            .flat_map(|events_by_seq| events_by_seq.values())
    }

    pub fn outbox_len(&self) -> usize {
        self.outbox
            .values()
            .map(|events_by_seq| events_by_seq.len())
            .sum()
    }

    pub fn interner(&self) -> std::sync::MutexGuard<'_, Interner> {
        lock_interner(&self.interner)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StabilitySnapshot {
    /// Per-replica stable sequence number, keyed by replica id. Everything at
    /// or below it has been delivered by *every* known replica, so it can be
    /// compacted out of the PO-Log.
    pub stable_version: Vec<(String, usize)>,
    /// `stable_version` summed: the length of the causally stable prefix. This
    /// is the number that stops advancing when a member goes silent.
    pub stable_prefix: usize,
    /// Operations this replica has delivered locally, stable or not.
    pub delivered: usize,
    /// Replicas this replica has ever heard of. Never shrinks today — there is
    /// no eviction, which is precisely the problem phase 2 addresses.
    pub known_replicas: usize,
    /// Events received but not yet causally deliverable.
    pub pending: usize,
    /// Events retained for replay because they are not yet stable everywhere.
    ///
    /// The outbox is pruned only when the stable version advances
    /// (`prune_outbox`), so this is the quantity that grows without bound while
    /// one member is silent. It is the closest directly observable proxy for
    /// PO-Log growth: the composite log types generated by Arachne do not
    /// expose their unstable length through `IsLog`, and both are compacted by
    /// the same stable version.
    pub retained: usize,
}

impl<O: Clone + Debug> Tcsb<O> {
    pub fn with_interner(replica_idx: ReplicaIdx, interner: SharedInterner) -> Self {
        let resolver = lock_interner(&interner).resolver().clone();
        Self {
            replica_idx,
            inbox: FxHashMap::default(),
            outbox: FxHashMap::default(),
            matrix_clock: MatrixClock::new(replica_idx, resolver.clone()),
            last_stable_version: CausalFrontier::new(resolver),
            interner,
            last_updated_columns: Vec::new(),
        }
    }

    fn sync_members(&mut self) {
        let resolver = lock_interner(&self.interner).resolver().clone();
        if *self.matrix_clock.origin_version().resolver() != resolver {
            assert!(
                self.inbox.is_empty()
                    && self.outbox.is_empty()
                    && self.matrix_clock.origin_version().sum() == 0,
                "cannot reorder a member table with history"
            );
            let id = self.matrix_clock.origin_version().origin_id();
            self.replica_idx = resolver.get(id).expect("hosted member");
            self.matrix_clock = MatrixClock::new(self.replica_idx, resolver.clone());
            self.last_stable_version = CausalFrontier::new(resolver.clone());
        }
        for idx in 0..resolver.len() {
            if self.matrix_clock.version_by_idx(ReplicaIdx(idx)).is_none() {
                self.matrix_clock.add_replica(ReplicaIdx(idx));
            }
        }
    }

    pub fn shared_interner(&self) -> &SharedInterner {
        &self.interner
    }

    pub fn stability(&self) -> StabilitySnapshot {
        let resolver = lock_interner(&self.interner).resolver().clone();
        let stable_version: Vec<_> = resolver
            .into_vec()
            .into_iter()
            .enumerate()
            .map(|(idx, id)| (id.0, self.last_stable_version.seq_by_idx(ReplicaIdx(idx))))
            .collect();
        StabilitySnapshot {
            stable_prefix: stable_version.iter().map(|(_, seq)| seq).sum(),
            stable_version,
            delivered: self.matrix_clock.origin_version().sum(),
            known_replicas: resolver.len(),
            pending: self.inbox.len(),
            retained: self.outbox.values().map(BTreeMap::len).sum(),
        }
    }

    pub fn has_history_for(&self, id: &str) -> bool {
        lock_interner(&self.interner)
            .get(&id.into())
            .is_some_and(|idx| self.matrix_clock.origin_version().seq_by_idx(idx) > 0)
    }
}

impl<O> StateSnapshot<O> {
    pub fn resolver(&self) -> &Resolver {
        self.delivered.resolver()
    }

    pub fn validate_members(&self) -> Result<(), ReplicaId> {
        let table = self.resolver();
        let check = |resolver: &Resolver| {
            for member in resolver.into_vec() {
                if table.get(&member).is_none() {
                    return Err(member);
                }
            }
            Ok(())
        };
        for row in &self.rows {
            check(row.resolver())?;
        }
        check(self.stable.resolver())?;
        for event in &self.outbox {
            check(event.id().resolver())?;
            check(event.version().resolver())?;
        }
        Ok(())
    }
}
