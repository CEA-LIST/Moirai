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
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::ops::Bound;
use std::rc::Rc;

pub type RedundantRelation<O> = fn(&Event<O>, &Event<O>) -> bool;
pub type AnyOp<O> = Duet<MSet<String>, O>;
pub type Converging = HashMap<String, Vec<(String, VectorClock<String, usize>)>>;

#[derive(Debug)]
pub enum DeliveryError {
    UnknownPeer,
    DuplicatedEvent,
    OutOfOrderEvent,
    EvictedPeer,
}

/// # Extended Reliable Causal Broadcast (RCB) middleware API
///
/// A **Tagged Causal Stable Broadcast (TCSB)** is an extended Reliable Causal Broadcast (RCB)
/// middleware API designed to offer additional information about causality during message delivery.
/// It also notifies recipients when delivered messages achieve causal stability,
/// facilitating subsequent compaction within the Partially Ordered Log of operations (PO-Log)
#[derive(Clone)]
pub struct Tcsb<O>
where
    O: PureCRDT + Debug,
{
    pub id: String,
    pub state: POLog<O>,
    /// Buffer of operations to be delivered
    pub pending: VecDeque<Event<AnyOp<O>>>,
    /// A peer might stabilize a remove operation ahead of others if it hasn't yet broadcasted any operations.
    /// Consequently, its first message after the remove should include the lamport clock of the evicted peer.
    pub timestamp_extension: BTreeMap<Metadata, VectorClock<String, usize>>,
    /// Members whose convergence to the network state is unknown.
    /// The key is the welcoming peer, value is the list of converging members.
    pub converging_members: Converging,
    /// Group Membership Service
    pub group_membership: POLog<MSet<String>>,
    /// Peers that have been evicted from the group
    pub evicted: HashSet<String>,
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
            converging_members: HashMap::new(),
            timestamp_extension: BTreeMap::new(),
            ltm: MatrixClock::new(&[id.to_string()]),
            lsv: VectorClock::new(id.to_string()),
            pending: VecDeque::new(),
            evicted: HashSet::new(),
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

    /// Broadcast a new domain-specific operation to all peers and deliver it to the local state.
    pub fn tc_bcast_op(&mut self, op: O) -> Event<O> {
        let metadata = self.generate_metadata_for_new_event();
        let event = Event::new(Duet::Second(op.clone()), metadata.clone());
        self.tc_deliver(event.clone());
        #[cfg(feature = "utils")]
        self.tracer.append(event.clone());
        Event::new(op, metadata)
    }

    /// Broadcast a new membership operation to all peers and deliver it to the local state.
    pub fn tc_bcast_membership(&mut self, op: MSet<String>) -> Event<MSet<String>> {
        let metadata = self.generate_metadata_for_new_event();
        let event = Event::new(Duet::First(op.clone()), metadata.clone());
        self.tc_deliver(event.clone());
        #[cfg(feature = "utils")]
        self.tracer.append(event.clone());
        Event::new(op, metadata)
    }

    /// Deliver a domain-specific operation to the local state.
    pub fn tc_deliver_op(&mut self, event: Event<O>) {
        let event = Event::new(Duet::Second(event.op.clone()), event.metadata.clone());
        self.check_delivery(event);
    }

    /// Deliver a membership operation to the local state.
    pub fn tc_deliver_membership(&mut self, event: Event<MSet<String>>) {
        let event = Event::new(Duet::First(event.op.clone()), event.metadata.clone());
        self.check_delivery(event);
    }

    /// Reliable Causal Broadcast (RCB) functionality.
    /// Store a new event in the buffer and check if it is ready to be delivered.
    /// Check if other pending events are made ready to be delivered by the new event.
    fn check_delivery(&mut self, mut event: Event<AnyOp<O>>) {
        // An event from the local peer should not be the origin of the event
        // Because the local peer should not call this function for its own events
        assert_ne!(
            self.id, event.metadata.origin,
            "Local peer {} should not be the origin {} of the event",
            self.id, event.metadata.origin
        );
        // If from evicted peer, unknown peer, duplicated event, ignore it
        if self.guard(&event.metadata).is_err() {
            return;
        }
        // The LTM should be synchronized with the group membership
        assert_eq!(self.eval_group_membership().len(), self.ltm_keys().len());
        // If the event comes from a converging member, it has converged
        self.check_still_converging_members(&event);
        // Check for timestamp inconsistencies
        if HashSet::from_iter(event.metadata.clock.keys()) != self.eval_group_membership() {
            debug!(
                "[{}] - Timestamp inconsistency, group members are: {}",
                self.id.blue().bold(),
                format!("{:?}", self.eval_group_membership()).green(),
            );
            self.fix_timestamp_inconsistencies_incoming_event(&mut event.metadata);
        }
        if self.guard_against_out_of_order(&event.metadata) {
            error!(
                "[{}] - Out-of-order event from {} detected with timestsamp {}. Operation: {}",
                self.id.blue().bold(),
                event.metadata.origin.blue(),
                format!("{}", event.metadata.clock).red(),
                format!("{:?}", event.op).green(),
            );
            #[cfg(feature = "wasm")]
            console::error_1(
                &format!(
                    "[{}] - Out-of-order event from {} detected with timestsamp {}. Operation: {}",
                    self.id.blue().bold(),
                    format!("{}", event.metadata.clock).red(),
                    event.metadata.origin,
                    format!("{:?}", event.op).green(),
                )
                .into(),
            );
        }
        // Store the new event at the end of the causal buffer
        self.pending.push_back(event.clone());
        self.check_pending();
    }

    /// Deliver an event to the local state.
    fn tc_deliver(&mut self, event: Event<AnyOp<O>>) {
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

            // Is the event coming from a new peer from whom we are waiting for proof of convergence to the network state?
            let is_from_converging = self.converging_members.iter().find_map(|(w, c)| {
                if c.iter().any(|i| i.0 == event.metadata.origin) {
                    Some(w.clone())
                } else {
                    None
                }
            });
            if let Some(w) = is_from_converging {
                debug!(
                    "[{}] - Removing {} from converging members welcomed by {}",
                    self.id.blue().bold(),
                    event.metadata.origin.red(),
                    w.blue()
                );
                // The new peer has converged to the network state
                self.converging_members
                    .get_mut(&w)
                    .unwrap()
                    .retain(|c| c.0 != event.metadata.origin);
            }

            // Is the event from the peer that must transfer its state to a converging members?
            let is_from_welcoming = self
                .converging_members
                .iter_mut()
                .find(|(w, _)| *w == &event.metadata.origin);
            if let Some((w, c)) = is_from_welcoming {
                let mut to_remove = Vec::new();
                let w_clock = self.ltm.get(w).unwrap().clone();
                // We still don't have a proof that the new peer has conv"erged to the network state
                // We are updating the vector clock of the new peer with the welcoming peer's vector clock
                for (id, clock) in c.iter() {
                    // if the event is greater than the clock of the converging member
                    // it means that the converging member has converged to the network state
                    debug!(
                        "[{}] - Updating vector clock of converging member {} with the one of {}",
                        self.id.blue().bold(),
                        id.blue(),
                        w.blue()
                    );
                    self.ltm.update(id, &w_clock);

                    // TODO: not precise enough, could cause issues
                    if &event.metadata.clock >= clock {
                        debug!(
                            "[{}] - removing {} from the converging member",
                            self.id.blue().bold(),
                            id.blue(),
                        );
                        to_remove.push(id.clone());
                    }
                }
                to_remove.iter().for_each(|id| {
                    c.retain(|(i, _)| i != id);
                });
            }

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
                    assert!(!self.state.unstable.contains_key(&event.metadata));
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
                let event = Event::new(op, event.metadata);
                let (keep, stable, unstable) = O::effect(&event, &self.state);

                self.state.remove_redundant_ops(&self.id, stable, unstable);

                if keep {
                    self.state.new_event(&event);
                    info!(
                        "[{}] - Op {} is added to the log",
                        self.id.blue().bold(),
                        format!("{:?}", event.op).green()
                    );
                    assert!(!self.group_membership.unstable.contains_key(&event.metadata));
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

        // Check if some operations are ready to be stabilized
        self.tc_stable();
    }

    /// The TCSB middleware can offer this causal stability information through extending its API with tcstablei(τ),
    /// which informs the upper layers that message with timestamp τ is now known to be causally stable
    pub fn tc_stable(&mut self) {
        let ignore = self.peers_to_ignore_for_stability();
        let svv = self.ltm.svv(&ignore);
        let lower_bound = Metadata::new(svv.clone(), "");
        let mut ready_to_stabilize = self.collect_stabilizable_events(&lower_bound);
        if !ready_to_stabilize.is_empty() {
            self.lsv = self.ltm.svv(&ignore);
        }

        // Events from a peer that is going to be removed must be stabilized before the remove operation is stable.
        ready_to_stabilize.sort_by(|a, b| {
            // Membership ops are always causally stable after domain-specific ops
            if self.group_membership.unstable.contains_key(a)
                && a.clock.partial_cmp(&b.clock).is_none()
            {
                Ordering::Greater
            } else {
                a.cmp(b)
            }
        });

        for m in &ready_to_stabilize {
            assert!(m.clock <= svv, "SVV: {:?}, Clock: {:?}", svv, m.clock);
        }

        // Timestamp rewwriting during stabilization: necessary for early rejoin.

        for metadata in ready_to_stabilize.iter_mut() {
            // must modify metadata to remove the keys that are not in the group membership
            // for key in metadata.clock.keys() {
            //     if !self.eval_group_membership().contains(&key) {
            //         assert_ne!(key, self.id);
            //         metadata.clock.remove(&key);
            //     }
            // }
            if self.state.unstable.contains_key(metadata) {
                info!(
                    "[{}] - Op {} with timestamp {} is causally stable",
                    self.id.blue().bold(),
                    format!("{:?}", self.state.unstable.get(metadata).unwrap()).green(),
                    format!("{}", metadata.clock).red(),
                );
                O::stable(metadata, &mut self.state);
            } else if self.group_membership.unstable.contains_key(metadata) {
                info!(
                    "[{}] - Op {} with timestamp {} is causally stable",
                    self.id.blue().bold(),
                    format!(
                        "{:?}",
                        self.group_membership.unstable.get(metadata).unwrap()
                    )
                    .green(),
                    format!("{}", metadata.clock).red(),
                );

                let event = self
                    .group_membership
                    .unstable
                    .get(metadata)
                    .unwrap()
                    .as_ref()
                    .clone();

                // We cannot remove a peer from the LTM before all its events have been stabilized
                // Otherwise, we would process events from a peer we don't know and it's forbidden
                // To avoir that, we just skip a stabilization opportunity until all events from the peer can be stabilized
                if let MSet::Remove(i) = &event {
                    if let Some(i_clock) = self.ltm.get(i) {
                        let mut remove_exec_clock = svv.clone();
                        remove_exec_clock.merge(i_clock);
                        if svv.partial_cmp(&remove_exec_clock).is_none() || svv < remove_exec_clock
                        {
                            debug!(
                            "[{}] - Skipping stabilization of remove event for peer {}. Remove will not be stabilized before {}. Current SVV is {}",
                            self.id.blue().bold(),
                            i.blue(),
                            format!("{}", remove_exec_clock).red(),
                            format!("{}", svv).red()
                        );
                            continue;
                        }
                    }
                }

                MSet::stable(metadata, &mut self.group_membership);

                if let MSet::Add(id) = event {
                    // Keep the version vector of the new member + who is welcoming it (i.e. the origin of the event)
                    // After stabilizing the event, the new member should have the same vector clock as the welcoming peer
                    // And we should keep updating the vector clock of the new member with the welcoming peer's vector clock
                    // when we receive new events from the welcoming peer, until we know that the welcoming peer has stabilized
                    // the new member's `add` event.
                    if metadata.origin != self.id {
                        debug!(
                            "[{}] - Adding {} to converging members welcomed by {}",
                            self.id.blue().bold(),
                            id.blue(),
                            metadata.origin.blue()
                        );
                        self.converging_members
                            .entry(metadata.origin.clone())
                            .and_modify(|c| c.push((id.clone(), metadata.clock.clone())))
                            .or_insert_with(|| vec![(id.clone(), metadata.clock.clone())]);
                    }
                }
            } else {
                panic!(
                    "[{}] - Event with metadata {} not found in the log",
                    self.id, metadata
                );
            }
        }

        self.update_ltm_membership();
    }

    /// Transfer the state of a replica to another replica.
    /// The peer giving the state should be the one that have welcomed the other peer in its group membership.
    pub fn state_transfer(&mut self, other: &mut Tcsb<O>) {
        assert!(
            self.id != other.id && other.eval_group_membership().contains(&self.id),
            "Peer {} is not in the group membership of peer {}",
            self.id,
            other.id
        );
        self.state = other.state.clone();
        self.group_membership = other.group_membership.clone();
        self.ltm = other.ltm.clone();
        self.ltm.most_update(&self.id);
        self.lsv = other.lsv.clone();
        self.timestamp_extension = other.timestamp_extension.clone();
        self.converging_members = other.converging_members.clone();
        self.evicted = other.evicted.clone();
        // The peer will have its clock at least as high as the one of the other peer
        let other_clock = other.my_clock().clone();
        other.ltm.get_mut(&self.id).unwrap().merge(&other_clock);
        assert_eq!(self.my_clock(), other.my_clock());
        assert_eq!(self.my_clock(), self.ltm.get(&other.id).unwrap());
        assert_eq!(other.my_clock(), other.ltm.get(&self.id).unwrap());
    }

    /// Utilitary function to evaluate the current state of the whole CRDT.
    pub fn eval(&self) -> O::Value {
        O::eval(&self.state, &Utf8PathBuf::default())
    }

    /// Utilitary function to evaluate the current state of the group membership.
    pub fn eval_group_membership(&self) -> HashSet<String> {
        MSet::eval(&self.group_membership, &Utf8PathBuf::default())
    }

    /// Return the mutable vector clock of the local replica
    pub fn my_clock_mut(&mut self) -> &mut VectorClock<String, usize> {
        self.ltm
            .get_mut(&self.id)
            .expect("Local vector clock not found")
    }

    /// Return the vector clock of the local replica
    pub fn my_clock(&self) -> &VectorClock<String, usize> {
        self.ltm
            .get(&self.id)
            .expect("Local vector clock not found")
    }

    pub fn events_since(
        &self,
        lsv: &VectorClock<String, usize>,
        since: &VectorClock<String, usize>,
    ) -> Vec<Event<AnyOp<O>>> {
        assert!(
            lsv <= since || lsv.partial_cmp(since).is_none(),
            "LSV should be inferior, equal or even concurrent to the since clock."
        );
        let mut metadata_lsv = Metadata::new(lsv.clone(), "");
        let mut metadata_since = Metadata::new(since.clone(), "");

        if self.eval_group_membership() != HashSet::from_iter(metadata_lsv.clock.keys()) {
            self.fix_timestamp_inconsistencies_incoming_event(&mut metadata_lsv);
        }

        if self.eval_group_membership() != HashSet::from_iter(metadata_since.clock.keys()) {
            self.fix_timestamp_inconsistencies_incoming_event(&mut metadata_since);
        }

        assert_eq!(
            self.ltm_keys(),
            metadata_since.clock.keys(),
            "Since: {:?}",
            metadata_since.clock
        );
        assert_eq!(
            self.ltm_keys(),
            metadata_lsv.clock.keys(),
            "LSV: {:?}",
            metadata_lsv.clock
        );
        // If the LSV is strictly greater than the since vector clock, it means the peer needs a state transfer
        // However, it should not happen because every peer should wait that everyone gets the ops before stabilizing

        // TODO: Rather than just `since`, the requesting peer should precise if it has received other events in its pending buffer.
        let events: Vec<Event<AnyOp<O>>> = self
            .group_membership
            .unstable
            .range((Bound::Excluded(&metadata_lsv), Bound::Unbounded))
            .filter_map(|(m, o)| {
                // If the dot is greater than the one in the since vector clock, then we have not delivered the event
                if m.clock.get(&m.origin).unwrap() > metadata_since.clock.get(&m.origin).unwrap() {
                    Some(Event::new(Duet::First(o.as_ref().clone()), m.clone()))
                } else {
                    None
                }
            })
            // .iter()
            // .map(|(m, o)| Event::new(Duet::First(o.as_ref().clone()), m.clone()))
            .collect::<Vec<_>>();
        let domain_events: Vec<Event<AnyOp<O>>> = self
            .state
            .unstable
            .range((Bound::Excluded(&metadata_lsv), Bound::Unbounded))
            .filter_map(|(m, o)| {
                // If the dot is greater than the one in the since vector clock, then we have not delivered the event
                if m.clock.get(&m.origin).unwrap() > metadata_since.clock.get(&m.origin).unwrap() {
                    Some(Event::new(Duet::Second(o.as_ref().clone()), m.clone()))
                } else {
                    None
                }
            })
            // .iter()
            // .map(|(m, o)| Event::new(Duet::Second(o.as_ref().clone()), m.clone()))
            .collect::<Vec<_>>();
        let events = [events, domain_events].concat();
        #[cfg(feature = "wasm")]
        console::log_1(
            &format!(
                "Events since LSV {} with clock {}: {:?}",
                lsv, since, events
            )
            .into(),
        );
        events
    }

    /// Returns the list of peers whose local peer is waiting for messages to deliver those previously received.
    pub fn waiting_from(&self) -> HashSet<String> {
        // Let consider a distributed systems where nodes exchange messages with a vector clock where
        // process id are maped to integers. Each peer have a pending array of received messages that are
        // not causally ready to be delivered. Give me the algorithm that returns the list of peers whose
        // local peer is waiting for messages to deliver those previously received.
        let mut waiting_from = HashSet::<String>::new();
        for event in self.pending.iter() {
            assert!(
                event.metadata.origin != self.id,
                "Local peer should not be in the pending list. Event: {:?}",
                event
            );
            let sending_peer_clock = self.ltm.get(&event.metadata.origin).unwrap();
            let sending_peer_lamport = sending_peer_clock.get(&event.metadata.origin).unwrap();
            if event.metadata.get_lamport(&event.metadata.origin) > sending_peer_lamport {
                waiting_from.insert(event.metadata.origin.clone());
            }
        }
        waiting_from
    }

    /// Guard against unknown peers, duplicated events, and out-of-order events.
    fn guard(&self, metadata: &Metadata) -> Result<(), DeliveryError> {
        if self.guard_against_unknow_peer(metadata) {
            error!(
                "[{}] - Event from an unknown peer {} detected with timestsamp {}",
                self.id.blue().bold(),
                metadata.origin.blue(),
                format!("{}", metadata.clock).red()
            );
            #[cfg(feature = "wasm")]
            console::error_1(
                &format!(
                    "[{}] - Unknown peer {} detected with timestsamp: {}",
                    self.id.blue().bold(),
                    metadata.origin.blue(),
                    format!("{}", metadata.clock).red()
                )
                .into(),
            );
            return Err(DeliveryError::UnknownPeer);
        }
        if self.guard_against_duplicates(metadata) {
            error!(
                "[{}] - Duplicated event detected from {} with timestsamp {}",
                self.id.blue().bold(),
                metadata.origin.red(),
                format!("{}", metadata.clock).red()
            );
            #[cfg(feature = "wasm")]
            console::error_1(
                &format!(
                    "[{}] - Duplicated event detected from {} with timestsamp {}",
                    self.id.blue().bold(),
                    metadata.origin.red(),
                    format!("{}", metadata.clock).red()
                )
                .into(),
            );
            return Err(DeliveryError::DuplicatedEvent);
        }
        if self.guard_against_evicted(metadata) {
            error!(
                "[{}] - Event from an evicted peer {} detected with timestsamp {}",
                self.id.blue().bold(),
                metadata.origin.red(),
                format!("{}", metadata.clock).red()
            );
            #[cfg(feature = "wasm")]
            console::error_1(
                &format!(
                    "[{}] - Event from an evicted peer {} detected with timestsamp {}",
                    self.id.blue().bold(),
                    metadata.origin.red(),
                    format!("{}", metadata.clock).red()
                )
                .into(),
            );
            return Err(DeliveryError::DuplicatedEvent);
        }
        Ok(())
    }

    fn guard_against_evicted(&self, metadata: &Metadata) -> bool {
        self.evicted.contains(&metadata.origin)
    }

    /// Check that the event has not already been delivered
    fn guard_against_duplicates(&self, metadata: &Metadata) -> bool {
        self.ltm
            .get(&metadata.origin)
            .map(|other_clock| metadata.clock <= *other_clock)
            .unwrap_or(false)
    }

    /// Check that the event is the causal successor of the last event delivered by this same replica
    /// Returns true if the event is out of order
    fn guard_against_out_of_order(&self, metadata: &Metadata) -> bool {
        // We assume that the LTM and the event clock have the same number of entries
        assert_eq!(
            self.ltm_keys(),
            metadata.clock.keys(),
            "LTM Keys ({}): {:?}, Event Keys ({}): {:?}",
            self.ltm_keys().len(),
            self.ltm_keys(),
            metadata.clock.len(),
            metadata.clock.keys()
        );
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
                *v > ltm_value
            });
        is_origin_out_of_order || are_other_entries_out_of_order
    }

    /// Check that the event is not from an unknown peer
    /// The peer is unknown if it is not in the LTM and there is no unstable `add`
    /// operation for it in the group membership
    fn guard_against_unknow_peer(&self, metadata: &Metadata) -> bool {
        self.ltm.get(&metadata.origin).is_none()
            && !self
                .group_membership
                .unstable
                .iter()
                .any(|(_, o)| match o.as_ref() {
                    MSet::Add(v) => v == &metadata.origin,
                    _ => false,
                })
    }

    /// Check that the event is not coming from a peer that is going to be removed from the group.
    /// Returns true if the event is not ready to be delivered
    fn guard_against_concurrent_to_remove(&self, event: &Event<AnyOp<O>>) -> bool {
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
            .filter_map(|(m, v)| {
                if v <= &self.lsv {
                    Some((m.clone(), v.clone()))
                } else {
                    None
                }
            })
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

    // Store the lamport clock of the removed peer in the timestamp extension list.
    // TODO: Except if the `remove` message comes from the local peer.
    // fn store_lamport_of_removed_peer(&mut self, id: &String) {
    //     let ext_list = self
    //         .timestamp_extension
    //         .entry(Metadata::new(self.lsv.clone(), ""))
    //         .or_default();
    //     // The removed peer may be already removed from the LTM.
    //     // e.g. multiple remove operations from different peers have already been stabilized.
    //     if let Some(removed_clock) = self.ltm.get(&self.id).and_then(|clock| clock.get(id)) {
    //         ext_list.insert(id.clone(), removed_clock);
    //     }
    // }

    pub fn ltm_keys(&self) -> Vec<String> {
        self.ltm
            .keys()
            .iter()
            .filter(|k| !self.evicted.contains(*k))
            .cloned()
            .collect()
    }

    // fn fix_timestamp_inconsistencies_stored_events<T: Debug>(
    //     state: &mut BTreeMap<Metadata, T>,
    //     id: &String,
    // ) {
    //     let mut key_to_edit = Vec::<Metadata>::new();

    //     for (m, _) in state.iter() {
    //         // for unstable group membership only: assert!(m.origin != *id || m == metadata);
    //         assert!(m.origin != *id);
    //         if m.clock.contains(id) {
    //             key_to_edit.push(m.clone());
    //         }
    //     }
    //     for mut m in key_to_edit {
    //         let op = state.remove(&m).unwrap();
    //         m.clock.remove(id);
    //         state.insert(m, op);
    //     }
    // }

    /// Correct the inconsistencies in the vector clocks of two events
    /// by adding the missing keys and setting the missing values to 0
    /// Timestamp inconsistencies can occur when a peer has stablized a membership event before the other peers.
    pub fn fix_timestamp_inconsistencies_incoming_event(&self, metadata: &mut Metadata) {
        // Missing keys in the new event
        for key in &self.ltm_keys() {
            if !metadata.clock.contains(key) {
                metadata.clock.insert(key.clone(), 0);
            }
        }
        // TODO: Verify if the following code is correct
        // Missing keys in the LTM
        for key in metadata.clock.keys() {
            if !&self.ltm_keys().contains(&key) {
                // We can't remove the from the event if it is its id
                assert_ne!(key, metadata.origin);
                metadata.clock.remove(&key);
            }
        }
        assert_eq!(
            self.ltm_keys().len(),
            metadata.clock.len(),
            "Timestamp inconsistency: LTM keys ({}): {:?}, Event keys ({}): {:?}",
            &self.ltm_keys().len(),
            &self.ltm_keys(),
            metadata.clock.keys().len(),
            metadata.clock.keys(),
        );
    }

    /// Check if the converging members have finally converged to the network state.
    fn check_still_converging_members(&mut self, new: &Event<AnyOp<O>>) {
        let mut to_remove = Vec::new();
        for (w, cs) in &mut self.converging_members {
            // If the event comes from a converging member, remove it from the list (we have the proof it has converged)
            if cs.iter().any(|c| c.0 == new.metadata.origin) {
                cs.retain(|c| c.0 != new.metadata.origin);
                if cs.is_empty() {
                    to_remove.push(w.clone());
                }
            }
        }
        for w in to_remove {
            self.converging_members.remove(&w);
        }
    }

    /// Returns a subset of peers that can be safely ignored when checking for causal stability.
    fn peers_to_ignore_for_stability(&self) -> Vec<String> {
        let ignore: Vec<String> = self
            .group_membership
            .unstable
            .iter()
            .filter_map(|(_, o)| match o.as_ref() {
                MSet::Remove(v) => Some(v.clone()),
                _ => None,
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
        [self.evicted.iter().cloned().collect::<Vec<_>>(), ignore].concat()
    }

    /// Returns a list of operations that are ready to be stabilized.
    fn collect_stabilizable_events(&self, lower_bound: &Metadata) -> Vec<Metadata> {
        let mut state = self
            .state
            .unstable
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .filter_map(|(m, _)| {
                if m.clock <= lower_bound.clock {
                    Some(m.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<Metadata>>();
        let group_membership = self
            .group_membership
            .unstable
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .filter_map(|(m, _)| {
                if m.clock <= lower_bound.clock {
                    Some(m.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<Metadata>>();
        state.extend(group_membership);
        state.sort();
        state
    }

    fn check_pending(&mut self) {
        // Oldest event first
        self.pending
            .make_contiguous()
            .sort_by(|a, b| a.metadata.cmp(&b.metadata));
        let mut still_pending = VecDeque::new();
        while let Some(event) = self.pending.pop_front() {
            // If the event is causally ready...
            if !self.guard_against_out_of_order(&event.metadata)
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

    /// Synchronize the Last Timestamp Matrix (LTM) with the latest group membership information.
    fn update_ltm_membership(&mut self) {
        let gms_members = self.eval_group_membership().into_iter().collect::<Vec<_>>();
        // Add missing keys
        for member in &gms_members {
            if self.ltm.get(member).is_none() {
                // The new peer's vector clock should not be an array of 0 but the last vector clock of the peer welcoming it
                let welcome_peer = self.converging_members.iter().find_map(|(w, c)| {
                    if c.iter().any(|i| &i.0 == member) {
                        Some(w)
                    } else {
                        None
                    }
                });
                // either the new peer is welcomed by another peer...
                if let Some(welcome_peer) = welcome_peer {
                    let new_member_clock = self.ltm.get(welcome_peer).unwrap().clone();
                    self.ltm.add_key(member.clone());
                    self.ltm.update(member, &new_member_clock);
                } else {
                    // ...or the new peer is welcomed by the local peer
                    self.ltm.add_key(member.clone());
                    let my_clock = self.my_clock().clone();
                    self.ltm.update(member, &my_clock);
                }
            }
        }
        // Remove keys that are not in the group membership
        for member in self.ltm_keys() {
            if !gms_members.contains(&member) {
                if member != self.id {
                    self.evicted.insert(member.clone());
                    // self.ltm.remove_key(&member);
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
                    self.timestamp_extension.clear();
                    self.evicted.clear();
                    self.converging_members.clear();
                    assert_eq!(self.eval_group_membership().len(), 1);
                    assert_eq!(self.ltm_keys(), &[self.id.clone()]);
                }
            }
        }
    }

    /// Change the id of the local peer.
    /// Should not be used if the peer is not alone in the group.
    /// Used to rejoin a group.
    pub fn new_id(&mut self, id: &str) {
        assert!(
            self.eval_group_membership().contains(&self.id)
                && self.eval_group_membership().len() == 1
        );
        assert!(self.ltm.len() == 1);
        assert!(self.state.unstable.is_empty());
        assert!(self.group_membership.unstable.is_empty());
        self.ltm.add_key(id.to_owned());
        let my_clock = self.my_clock().clone();
        self.ltm.update(&id.to_string(), &my_clock);
        self.ltm.remove_key(&self.id);
        self.id = id.to_string();
        self.group_membership = Self::create_group_membership(&self.id);
        assert!(self.ltm.keys().contains(&self.id) && self.ltm.keys().len() == 1);
    }

    /// Create a new group membership log.
    fn create_group_membership(id: &str) -> POLog<MSet<String>> {
        let mut group_membership = POLog::default();
        let op = Rc::new(MSet::Add(id.to_string()));
        group_membership.stable.push(Rc::clone(&op));
        group_membership
            .path_trie
            .insert(PathBufKey::default(), vec![Rc::downgrade(&op)]);
        group_membership
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        crdt::{
            counter::Counter,
            duet::Duet,
            test_util::{triplet, twins},
        },
        protocol::event::Event,
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

    #[test_log::test]
    fn events_since_concurrent() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Counter<i32>>();

        let _ = tcsb_a.tc_bcast_op(Counter::Inc(1));
        let _ = tcsb_a.tc_bcast_op(Counter::Inc(1));
        let _ = tcsb_a.tc_bcast_op(Counter::Inc(1));
        let _ = tcsb_a.tc_bcast_op(Counter::Inc(1));
        let _ = tcsb_a.tc_bcast_op(Counter::Inc(1));
        let _ = tcsb_a.tc_bcast_op(Counter::Inc(1));
        assert_eq!(6, tcsb_a.eval());
        assert_eq!(6, tcsb_a.state.unstable.len());

        let _ = tcsb_b.tc_bcast_op(Counter::Dec(1));
        let _ = tcsb_b.tc_bcast_op(Counter::Dec(1));
        let _ = tcsb_b.tc_bcast_op(Counter::Dec(1));
        let _ = tcsb_b.tc_bcast_op(Counter::Dec(1));
        let _ = tcsb_b.tc_bcast_op(Counter::Dec(1));
        let _ = tcsb_b.tc_bcast_op(Counter::Dec(1));
        assert_eq!(-6, tcsb_b.eval());
        assert_eq!(6, tcsb_b.state.unstable.len());

        let events = tcsb_a.events_since(&tcsb_b.lsv, tcsb_b.my_clock());
        assert_eq!(6, events.len());

        for event in events {
            match event.op {
                Duet::First(op) => {
                    let event = Event::new(op, event.metadata);
                    tcsb_b.tc_deliver_membership(event);
                }
                Duet::Second(op) => {
                    let event = Event::new(op, event.metadata);
                    tcsb_b.tc_deliver_op(event);
                }
            }
        }

        let events = tcsb_b.events_since(&tcsb_a.lsv, tcsb_a.my_clock());
        assert_eq!(6, events.len());

        for event in events {
            match event.op {
                Duet::First(op) => {
                    let event = Event::new(op, event.metadata);
                    tcsb_a.tc_deliver_membership(event);
                }
                Duet::Second(op) => {
                    let event = Event::new(op, event.metadata);
                    tcsb_a.tc_deliver_op(event);
                }
            }
        }
    }
}
