use camino::Utf8PathBuf;
use colored::*;
use log::{debug, error, info, log_enabled, Level};
use radix_trie::TrieCommon;
#[cfg(feature = "wasm")]
use web_sys::console;

use super::pathbuf_key::PathBufKey;
use super::po_log::POLog;
use super::{event::Event, metadata::Metadata, pure_crdt::PureCRDT};
use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};
use crate::crdt::duet::Duet;
use crate::crdt::membership_set::MSet;
#[cfg(feature = "utils")]
use crate::utils::tracer::Tracer;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

pub type RedundantRelation<O> = fn(&Event<O>, &Event<O>) -> bool;

pub enum DeliveryError {
    UnknownPeer,
    DuplicatedEvent,
    OutOfOrderEvent,
}

/// Extended Reliable Causal Broadcast (RCB) middleware API
///
/// A Tagged Causal Stable Broadcast (TCSB) is an extended Reliable Causal Broadcast (RCB)
/// middleware API designed to offer additional information about causality during message delivery.
/// It also notifies recipients when delivered messages achieve causal stability,
/// facilitating subsequent compaction within the Partially Ordered Log of operations (PO-Log)
pub struct Tcsb<O>
where
    O: PureCRDT + Debug,
{
    pub id: String,
    pub state: POLog<O>,
    /// Buffer of operations to be delivered
    pub pending: VecDeque<Event<Duet<MSet<String>, O>>>,
    /// A peer might stabilize a remove operation ahead of others if it hasn't yet broadcasted any operations.
    /// Consequently, its first message after the remove should include the lamport clock of the evicted peer.
    timestamp_extension: BTreeMap<Metadata, VectorClock<String, usize>>,
    /// Group Membership Service
    pub group_membership: POLog<MSet<String>>,
    /// Last Timestamp Matrix (LTM) is a matrix clock that keeps track of the vector clocks of all peers.
    pub ltm: MatrixClock<String, usize>,
    /// Last Stable Vector (LSV)
    pub lsv: VectorClock<String, usize>,
    /// Trace of events for debugging purposes
    #[cfg(feature = "utils")]
    pub tracer: Tracer,
}

impl<O> Tcsb<O>
where
    O: PureCRDT + Debug,
{
    /// Create a new TCSB instance.
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            state: POLog::default(),
            group_membership: Self::create_group_membership(id),
            timestamp_extension: BTreeMap::new(),
            ltm: MatrixClock::new(&[id.to_string()]),
            lsv: VectorClock::new(id.to_string()),
            pending: VecDeque::new(),
            #[cfg(feature = "utils")]
            tracer: Tracer::new(String::from(id)),
        }
    }

    #[cfg(feature = "utils")]
    /// Create a new TCSB instance with a tracer for debugging purposes.
    pub fn new_with_trace(id: &str) -> Self {
        let mut tcsb = Self::new(id);
        tcsb.tracer = Tracer::new(String::from(id));
        tcsb
    }

    /// Broadcast a new operation to all peers and deliver it to the local state.
    pub fn tc_bcast_op(&mut self, op: O) -> Event<O> {
        let metadata = self.generate_metadata_for_new_event();
        let event = Event::new(Duet::Second(op.clone()), metadata.clone());
        self.tc_deliver(event.clone());
        #[cfg(feature = "utils")]
        self.tracer.append(event.clone());
        Event::new(op, metadata)
    }

    /// Broadcast a new operation to all peers and deliver it to the local state.
    pub fn tc_bcast_membership(&mut self, op: MSet<String>) -> Event<MSet<String>> {
        let metadata = self.generate_metadata_for_new_event();
        let event = Event::new(Duet::First(op.clone()), metadata.clone());
        self.tc_deliver(event.clone());
        #[cfg(feature = "utils")]
        self.tracer.append(event.clone());
        Event::new(op, metadata)
    }

    pub fn tc_deliver_op(&mut self, event: Event<O>) {
        #[cfg(feature = "wasm")]
        console::log_1(
            &format!(
                "{} - Delivering op event {}",
                self.id,
                format!("{:?}", event.op)
            )
            .into(),
        );
        let event = Event::new(Duet::Second(event.op.clone()), event.metadata.clone());
        self.check_delivery(event);
    }

    pub fn tc_deliver_membership(&mut self, event: Event<MSet<String>>) {
        #[cfg(feature = "wasm")]
        console::log_1(
            &format!(
                "{} - Delivering membership event {}",
                self.id,
                format!("{:?}", event.op)
            )
            .into(),
        );
        let event = Event::new(Duet::First(event.op.clone()), event.metadata.clone());
        self.check_delivery(event);
    }

    fn check_delivery(&mut self, mut event: Event<Duet<MSet<String>, O>>) {
        // Check for timestamp inconsistencies
        if event.metadata.clock.keys().len() != self.eval_group_membership().len() {
            debug!(
                "[{}] - Timestamp inconsistency: MSet members: {}",
                self.id.blue().bold(),
                format!("{:?}", self.eval_group_membership()).green(),
            );
            self.fix_timestamp_inconsistencies(&mut event, &self.ltm.keys());
        }
        match self.guard(&event.metadata) {
            Err(DeliveryError::UnknownPeer) | Err(DeliveryError::DuplicatedEvent) => return,
            _ => {}
        }
        #[cfg(feature = "wasm")]
        console::log_1(&"Storing the event in the `pending` buffer".into());
        // Store the new event at the end of the causal buffer
        self.pending.push_back(event.clone());
        // Oldest event first
        self.pending
            .make_contiguous()
            .sort_by(|a, b| a.metadata.cmp(&b.metadata));
        let mut still_pending = VecDeque::new();
        while let Some(event) = self.pending.pop_front() {
            // If the event is causally ready...
            if !Self::guard_against_out_of_order(self, &event.metadata)
                && !self.guard_against_concurrent_to_remove(&event)
            {
                // ...deliver it
                self.tc_deliver(event);
            } else {
                // ...otherwise, keep it in the buffer
                still_pending.push_back(event);
            }
        }
        self.pending = still_pending;
    }

    /// Deliver an event to the local state.
    fn tc_deliver(&mut self, event: Event<Duet<MSet<String>, O>>) {
        info!(
            "[{}] - Delivering event {} from {} with timestamp {}",
            self.id.blue().bold(),
            format!("{:?}", event.op).green(),
            event.metadata.origin.blue(),
            format!("{}", event.metadata.clock).red()
        );
        // If the event is not from the local replica
        if self.id != event.metadata.origin {
            // Update the vector clock of the sender in the LTM
            // Increment the new peer vector clock with its actual value
            self.ltm
                .update(&event.metadata.origin, &event.metadata.clock);
            // Update our own vector clock
            self.my_clock_mut().merge(&event.metadata.clock);
            #[cfg(feature = "utils")]
            self.tracer.append(event.clone());
        }

        match event.op {
            // Group Membership event
            Duet::First(op) => {
                let mut event = Event::new(op, event.metadata);
                let (keep, stable, unstable) = MSet::effect(&event, &self.group_membership);
                self.group_membership
                    .remove_redundant_ops(&self.id, stable, unstable);

                if keep {
                    let ext = event.metadata.ext.clone();
                    for key in ext {
                        event.metadata.clock.remove(&key);
                    }
                    self.group_membership.new_event(&event);
                    info!(
                        "[{}] - Op {} is added to the log",
                        self.id.blue().bold(),
                        format!("{:?}", event.op).green()
                    );
                }

                if log_enabled!(Level::Debug) {
                    let trie_size = self.group_membership.path_trie.values().flatten().count();
                    let state_size =
                        self.group_membership.stable.len() + self.group_membership.unstable.len();
                    debug!(
                        "[{}] - `path_trie`: {}/{} weak pointers waiting to be cleaned",
                        self.id.blue().bold(),
                        trie_size - state_size,
                        trie_size,
                    );
                }

                self.group_membership.garbage_collect_trie();
            }
            // Domain-specific CRDT event
            Duet::Second(op) => {
                let mut event = Event::new(op, event.metadata);
                let (keep, stable, unstable) = O::effect(&event, &self.state);

                #[cfg(feature = "wasm")]
                console::log_1(
                    &format!(
                        "{} - Effect of the event: keep: {}, stable: {:?}, unstable: {:?}",
                        self.id, keep, stable, unstable
                    )
                    .into(),
                );

                self.state.remove_redundant_ops(&self.id, stable, unstable);

                if keep {
                    let ext = event.metadata.ext.clone();
                    for key in ext {
                        event.metadata.clock.remove(&key);
                    }
                    self.state.new_event(&event);
                    info!(
                        "[{}] - Op {} is added to the log",
                        self.id.blue().bold(),
                        format!("{:?}", event.op).green()
                    );
                    #[cfg(feature = "wasm")]
                    console::log_1(
                        &format!(
                            "Add to log. State size: {}",
                            self.state.stable.len() + self.state.unstable.len()
                        )
                        .into(),
                    );
                }
                if log_enabled!(Level::Debug) {
                    let trie_size = self.state.path_trie.values().flatten().count();
                    let state_size = self.state.stable.len() + self.state.unstable.len();
                    debug!(
                        "[{}] - `path_trie`: {}/{} weak pointers waiting to be cleaned",
                        self.id.blue().bold(),
                        trie_size - state_size,
                        trie_size,
                    );
                }

                self.state.garbage_collect_trie();
            }
        }

        #[cfg(feature = "wasm")]
        console::log_1(
            &format!(
                "State size: {}",
                self.state.stable.len() + self.state.unstable.len()
            )
            .into(),
        );

        // Check if some operations are ready to be stabilized
        self.tc_stable();
    }

    /// The TCSB middleware can offer this causal stability information through extending its API with tcstablei(τ),
    /// which informs the upper layers that message with timestamp τ is now known to be causally stable
    fn tc_stable(&mut self) {
        let ignore = self.peers_to_ignore_for_stability();
        let lower_bound = Metadata::new(self.ltm.svv(&ignore), "");
        let ready_to_stabilize = self.collect_stabilizable_events(&lower_bound);
        if !ready_to_stabilize.is_empty() {
            self.lsv = self.ltm.svv(&ignore);
        }

        for metadata in ready_to_stabilize.iter() {
            if self.state.unstable.contains_key(metadata) {
                info!(
                    "[{}] - {} is causally stable (op: {}) ; {:?} were ignored",
                    self.id.blue().bold(),
                    format!("{}", metadata.clock).red(),
                    format!("{:?}", self.state.unstable.get(metadata).unwrap()).green(),
                    ignore,
                );
                O::stable(metadata, &mut self.state);
            } else if self.group_membership.unstable.contains_key(metadata) {
                info!(
                    "[{}] - {} is causally stable (op: {}) ; {:?} were ignored",
                    self.id.blue().bold(),
                    format!("{}", metadata.clock).red(),
                    format!(
                        "{:?}",
                        self.group_membership.unstable.get(metadata).unwrap()
                    )
                    .green(),
                    ignore,
                );
                let to_remove = self
                    .group_membership
                    .unstable
                    .get(metadata)
                    .map(|arc| arc.as_ref())
                    .and_then(|op| {
                        if let MSet::Remove(v) = op {
                            Some(v.clone())
                        } else {
                            None
                        }
                    });

                if let Some(id) = to_remove {
                    self.store_lamport_of_removed_peer(&id);
                    // Remove every pending events from the removed peer
                    self.pending.retain(|e| e.metadata.origin != *id);
                }
                MSet::stable(metadata, &mut self.group_membership);
            }
        }

        self.update_ltm_membership();
    }

    pub fn state_transfer(&mut self, other: &Tcsb<O>) {
        self.state = other.state.clone();
        self.group_membership = other.group_membership.clone();
        self.ltm = other.ltm.clone();
        self.ltm.most_update(&self.id);
        self.lsv = other.lsv.clone();
    }

    /// Utilitary function to evaluate the current state of the whole CRDT.
    pub fn eval(&self) -> O::Value {
        O::eval(&self.state, &Utf8PathBuf::default())
    }

    pub fn eval_group_membership(&self) -> HashSet<String> {
        MSet::eval(&self.group_membership, &Utf8PathBuf::default())
    }

    /// Return the mutable vector clock of the local replica
    pub(crate) fn my_clock_mut(&mut self) -> &mut VectorClock<String, usize> {
        self.ltm
            .get_mut(&self.id)
            .expect("Local vector clock not found")
    }

    /// Return the vector clock of the local replica
    pub(crate) fn my_clock(&self) -> &VectorClock<String, usize> {
        self.ltm
            .get(&self.id)
            .expect("Local vector clock not found")
    }

    fn guard(&self, metadata: &Metadata) -> Result<(), DeliveryError> {
        if self.guard_against_unknow_peer(metadata) {
            error!(
                "[{}] - Unknown peer detected: {}",
                self.id.blue().bold(),
                metadata.origin.red()
            );
            #[cfg(feature = "wasm")]
            console::error_1(
                &format!(
                    "[{}] - Unknown peer detected: {}",
                    self.id.blue().bold(),
                    metadata.origin.red()
                )
                .into(),
            );
            return Err(DeliveryError::UnknownPeer);
        }
        if self.guard_against_duplicates(metadata) {
            error!(
                "[{}] - Duplicated event detected: {}",
                self.id.blue().bold(),
                format!("{}", metadata.clock).red()
            );
            #[cfg(feature = "wasm")]
            console::error_1(
                &format!(
                    "[{}] - Duplicated event detected: {}",
                    self.id.blue().bold(),
                    format!("{}", metadata.clock).red()
                )
                .into(),
            );
            return Err(DeliveryError::DuplicatedEvent);
        }
        if self.guard_against_out_of_order(metadata) {
            error!(
                "[{}] - Out-of-order event detected: {} from {}. Current local clock is: {}",
                self.id.blue().bold(),
                format!("{}", metadata.clock).red(),
                metadata.origin.blue(),
                format!("{}", self.my_clock()).red()
            );
            #[cfg(feature = "wasm")]
            console::error_1(
                &format!(
                    "[{}] - Out-of-order event detected: {} from {}. Current local clock is: {}",
                    self.id.blue().bold(),
                    format!("{}", metadata.clock).red(),
                    metadata.origin.blue(),
                    format!("{}", self.my_clock()).red()
                )
                .into(),
            );
            return Err(DeliveryError::OutOfOrderEvent);
        }
        Ok(())
    }

    /// Check that the event has not already been delivered
    fn guard_against_duplicates(&self, metadata: &Metadata) -> bool {
        self.id != metadata.origin
            && self
                .ltm
                .get(&metadata.origin)
                .map(|ltm_clock| metadata.clock <= *ltm_clock)
                .unwrap_or(false)
    }

    /// Check that the event is the causal successor of the last event delivered by this same replica
    /// Returns true if the event is out of order
    fn guard_against_out_of_order(&self, metadata: &Metadata) -> bool {
        // We assume that the LTM and the event clock have the same number of entries
        assert_eq!(self.ltm.len(), metadata.clock.len());
        // We assume that the event clock has an entry for its origin
        let event_lamport_clock = metadata.clock.get(&metadata.origin).unwrap();
        // We assume we know this origin
        let ltm_origin_clock = self.ltm.get(&metadata.origin).unwrap();
        // We assume that the clock we have for this origin has an entry for this origin
        let ltm_lamport_lock = ltm_origin_clock.get(&metadata.origin).unwrap();
        // Either the event is the next in the sequence or the event is causally superior to the origin eviction
        let is_origin_out_of_order = event_lamport_clock != ltm_lamport_lock + 1;
        let are_other_entries_out_of_order = metadata
            .clock
            .iter()
            .filter(|(k, _)| *k != &metadata.origin)
            .any(|(k, v)| {
                let ltm_clock = self.ltm.get(k).unwrap();
                let ltm_value = ltm_clock.get(k).unwrap();
                *v > ltm_value && *v != usize::MAX
            });
        is_origin_out_of_order || are_other_entries_out_of_order
    }

    /// Check that the event is not from an unknown peer
    fn guard_against_unknow_peer(&self, metadata: &Metadata) -> bool {
        self.ltm.get(&metadata.origin).is_none()
    }

    /// Check that the event is not coming from a peer that is going to be removed from the group
    /// Returns true if the event is not ready to be delivered
    fn guard_against_concurrent_to_remove(&self, event: &Event<Duet<MSet<String>, O>>) -> bool {
        // Do not deliver the event if the origin is going to be removed from the group...
        let will_be_removed = self
            .group_membership
            .unstable
            .iter()
            .any(|(_, o)| matches!(o.as_ref(), MSet::Remove(v) if v == &event.metadata.origin));
        // ...unless the event is necessary to deliver other peers events
        let necessary = self.pending.iter().any(|e| {
            e.metadata.get_lamport(&event.metadata.origin) >= event.metadata.get_origin_lamport()
                && e.metadata.origin != event.metadata.origin
        });
        will_be_removed && !necessary
    }

    /// Returns the update clock new event of this [`Tcsb<O>`].
    fn generate_metadata_for_new_event(&mut self) -> Metadata {
        let my_id = self.id.clone();
        let mut clock = {
            let my_clock = self.my_clock_mut();
            my_clock.increment(&my_id);
            my_clock.clone()
        };
        let ext = self.add_timestamp_extension(&mut clock);
        Metadata::new_with_ext(clock, &self.id, ext)
    }

    /// Add the timestamp extension to the vector clock of the new event.
    ///
    /// A peer may stabilize a membership event before the other peers because
    /// it hasn't yet broadcasted any operations. Consequently, its first message after the remove
    /// should include the lamport clock of the evicted peer.
    fn add_timestamp_extension(&mut self, clock: &mut VectorClock<String, usize>) -> Vec<String> {
        let ext_list: Vec<(Metadata, VectorClock<String>)> = self
            .timestamp_extension
            .range((
                Bound::Unbounded,
                Bound::Included(&Metadata::new(self.lsv.clone(), "")),
            ))
            .map(|(m, v)| (m.clone(), v.clone()))
            .collect();
        let ext_list_len = ext_list.len();
        if ext_list_len > 0 {
            debug!(
                "[{}] - Adding timestamp extension for {}",
                self.id.blue().bold(),
                format!("{}", clock).red()
            );
        }
        let mut ext_tracker = Vec::<String>::new();
        for (m, ext) in ext_list {
            ext.left_difference(clock).keys().iter().for_each(|k| {
                if !ext_tracker.contains(k) {
                    ext_tracker.push(k.clone());
                }
            });
            clock.merge(&ext);
            self.timestamp_extension.remove(&m);
        }
        if ext_list_len > 0 {
            debug!(
                "[{}] - Timestamp extension added: {}",
                self.id.blue().bold(),
                format!("{}", clock).red()
            );
        }
        ext_tracker
    }

    /// Store the lamport clock of the removed peer in the timestamp extension list.
    fn store_lamport_of_removed_peer(&mut self, id: &String) {
        let ext_list = self
            .timestamp_extension
            .entry(Metadata::new(self.lsv.clone(), ""))
            .or_default();
        let removed_clock = self.ltm.get(&self.id).unwrap().get(id).unwrap();
        ext_list.insert(id.clone(), removed_clock);
    }

    /// Correct the inconsistencies in the vector clocks of two events
    /// by adding the missing keys and setting the missing values to 0 or usize::MAX
    /// Timestamp inconsistencies can occur when a peer has stablized a membership event before the other peers.
    fn fix_timestamp_inconsistencies(
        &self,
        new: &mut Event<Duet<MSet<String>, O>>,
        ltm_keys: &[String],
    ) {
        // Missing keys in the new event
        for key in ltm_keys.iter() {
            if !new.metadata.clock.contains(key) {
                // If a remove is waiting to be stabilized, set the value to usize::MAX
                // because the incoming message may be the first one after the remove
                let value = if self
                    .group_membership
                    .unstable
                    .values()
                    .any(|o| matches!(o.as_ref(), MSet::Remove(v) if v == key))
                {
                    usize::MAX
                } else {
                    0
                };
                new.metadata.clock.insert(key.clone(), value);
            }
        }
        // TODO: Verify if the following code is correct
        // Missing keys in the LTM
        for key in new.metadata.clock.keys() {
            if !ltm_keys.contains(&key) {
                new.metadata.clock.remove(&key);
            }
        }
        assert_eq!(
            self.ltm.len(),
            new.metadata.clock.len(),
            "Timestamp inconsistency: LTM keys: {:?}, Event keys: {:?}",
            ltm_keys,
            new.metadata.clock.keys()
        );
    }

    /// Returns a subset of peers that can be safely ignored when checking for causal stability.
    fn peers_to_ignore_for_stability(&self) -> Vec<String> {
        let ignore: Vec<String> = self
            .group_membership
            .unstable
            .iter()
            .filter_map(|(_, o)| match o.as_ref() {
                MSet::Add(_) => None,
                MSet::Remove(v) => Some(v.clone()),
            })
            .collect();
        let ignore = if ignore.contains(&self.id) {
            self.ltm
                .keys()
                .iter()
                .filter(|k| **k != self.id)
                .cloned()
                .collect()
        } else {
            ignore
        };
        ignore
    }

    /// Returns a list of operations that are ready to be stabilized.
    fn collect_stabilizable_events(&self, lower_bound: &Metadata) -> Vec<Metadata> {
        let mut state = self
            .state
            .unstable
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .map(|(m, _)| m.clone())
            .collect::<Vec<Metadata>>();
        let group_membership = self
            .group_membership
            .unstable
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .map(|(m, _)| m.clone())
            .collect::<Vec<Metadata>>();
        state.extend(group_membership);
        state.sort();
        state
    }

    /// Synchronize the Last Timestamp Matrix (LTM) with the latest group membership information.
    fn update_ltm_membership(&mut self) {
        let gms_members = self.eval_group_membership().into_iter().collect::<Vec<_>>();
        // Add missing keys
        for member in &gms_members {
            if self.ltm.get(member).is_none() {
                self.ltm.add_key(member.clone());
            }
        }
        // Remove keys that are not in the group membership
        for member in self.ltm.keys() {
            if !gms_members.contains(&member) {
                if member != self.id {
                    self.ltm.remove_key(&member);
                } else {
                    // Remove all keys except the local one
                    // if the local peer is removed from the group
                    for key in self.ltm.keys() {
                        if key != self.id {
                            self.ltm.remove_key(&key);
                        }
                    }
                    // Re-init the group membership
                    self.group_membership = Self::create_group_membership(&self.id);
                }
            }
        }
    }

    /// Create a new group membership log.
    fn create_group_membership(id: &str) -> POLog<MSet<String>> {
        let mut group_membership = POLog::default();
        let op = Arc::new(MSet::Add(id.to_string()));
        group_membership.stable.push(Arc::clone(&op));
        group_membership
            .path_trie
            .insert(PathBufKey::default(), vec![Arc::downgrade(&op)]);
        group_membership
    }
}

#[cfg(test)]
mod tests {
    use crate::crdt::{
        counter::Counter,
        test_util::{triplet, twins},
    };

    #[test_log::test]
    fn causal_delivery() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Counter<i32>>();

        let event_a_1 = tcsb_a.tc_bcast_op(Counter::Inc(1));
        let event_a_2 = tcsb_a.tc_bcast_op(Counter::Inc(1));

        tcsb_b.tc_deliver_op(event_a_2);
        tcsb_b.tc_deliver_op(event_a_1);

        assert_eq!(tcsb_b.eval(), 2);
        assert_eq!(tcsb_a.eval(), 2);

        let event_b_1 = tcsb_b.tc_bcast_op(Counter::Inc(1));
        let event_b_2 = tcsb_b.tc_bcast_op(Counter::Inc(1));
        let event_b_3 = tcsb_b.tc_bcast_op(Counter::Inc(1));
        let event_b_4 = tcsb_b.tc_bcast_op(Counter::Inc(1));

        tcsb_a.tc_deliver_op(event_b_4);
        tcsb_a.tc_deliver_op(event_b_3);
        tcsb_a.tc_deliver_op(event_b_1);
        tcsb_a.tc_deliver_op(event_b_2);

        assert_eq!(tcsb_a.eval(), 6);
        assert_eq!(tcsb_b.eval(), 6);
    }

    #[test_log::test]
    fn causal_delivery_triplet() {
        let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<Counter<i32>>();

        let event_b = tcsb_b.tc_bcast_op(Counter::Inc(2));

        tcsb_a.tc_deliver_op(event_b.clone());
        let event_a = tcsb_a.tc_bcast_op(Counter::Dec(7));

        tcsb_b.tc_deliver_op(event_a.clone());
        tcsb_c.tc_deliver_op(event_a.clone());
        tcsb_c.tc_deliver_op(event_b.clone());

        assert_eq!(tcsb_a.eval(), -5);
        assert_eq!(tcsb_b.eval(), -5);
        assert_eq!(tcsb_c.eval(), -5);
    }
}
