use colored::*;
use log::{error, info};

use super::membership::{View, ViewStatus, Views};
use super::po_log::POLog;
use super::{event::Event, metadata::Metadata, pure_crdt::PureCRDT};
use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};
use crate::protocol::guard::{guard_against_duplicates, guard_against_out_of_order};
#[cfg(feature = "utils")]
use crate::utils::tracer::Tracer;
use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::ops::Bound;

pub type RedundantRelation<O> = fn(&Event<O>, &Event<O>) -> bool;

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
    /// Unique peer id
    pub id: String,
    /// Domain-specific CRDT
    pub state: POLog<O>,
    /// Buffer of operations to be delivered
    pub pending: VecDeque<Event<O>>,
    /// Group Membership views
    pub group_membership: Views,
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
        let mut group_membership = Views::new();
        group_membership.install_view(View::init(&id.to_string()));
        Self {
            id: id.to_string(),
            state: POLog::default(),
            group_membership,
            // converging_members: HashMap::new(),
            ltm: MatrixClock::new(&[id.to_string()]),
            // timestamp_extension: BTreeMap::new(),
            lsv: VectorClock::new(id.to_string()),
            pending: VecDeque::new(),
            // hideout: HashMap::new(),
            // removed_members: HashSet::new(),
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
    pub fn tc_bcast(&mut self, op: O) -> Event<O> {
        let metadata = self.generate_metadata_for_new_event();
        let event = Event::new(op.clone(), metadata.clone());
        self.tc_deliver(event.clone());
        // self.add_timestamp_extension(&mut metadata.clock);
        #[cfg(feature = "utils")]
        self.tracer.append(event.clone());
        Event::new(op, metadata)
    }

    /// Reliable Causal Broadcast (RCB) functionality.
    /// Store a new event in the buffer and check if it is ready to be delivered.
    /// Check if other pending events are made ready to be delivered by the new event.
    pub fn try_deliver(&mut self, event: Event<O>) {
        // The local peer should not call this function for its own events
        assert_ne!(
            self.id, event.metadata.origin,
            "Local peer {} should not be the origin {} of the event",
            self.id, event.metadata.origin
        );
        // If from evicted peer, unknown peer, duplicated event, ignore it
        if self.group_membership.is_member(&event.metadata.origin) {
            error!(
                "[{}] - Event from an unknown peer {} detected with timestamp {}",
                self.id.blue().bold(),
                event.metadata.origin.blue(),
                format!("{}", event.metadata.clock).red()
            );
        }
        if self.group_membership.current_installed_view().id > event.metadata.view_id {
            error!(
                "[{}] - Event from {} with an old view id {} detected with timestamp {}",
                self.id.blue().bold(),
                event.metadata.origin.blue(),
                format!("{}", event.metadata.view_id).blue(),
                format!("{}", event.metadata.clock).red()
            );
            return;
        }
        if guard_against_duplicates(&self.ltm, &event.metadata) {
            error!(
                "[{}] - Duplicated event detected from {} with timestamp {}",
                self.id.blue().bold(),
                event.metadata.origin.red(),
                format!("{}", event.metadata.clock).red()
            );
            return;
        }
        // The LTM should be synchronized with the group membership
        if guard_against_out_of_order(&self.ltm, &event.metadata) {
            error!(
                "[{}] - Out-of-order event from {} detected with timestamp {}. Operation: {}",
                self.id.blue().bold(),
                event.metadata.origin.blue(),
                format!("{}", event.metadata.clock).red(),
                format!("{:?}", event.op).green(),
            );
        }
        // Store the new event at the end of the causal buffer
        self.pending.push_back(event.clone());
        self.pending
            .make_contiguous()
            .sort_by(|a, b| a.metadata.cmp(&b.metadata));
        let mut still_pending = VecDeque::new();
        while let Some(event) = self.pending.pop_front() {
            // If the event is causally ready...
            if !guard_against_out_of_order(&self.ltm, &event.metadata) {
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
    fn tc_deliver(&mut self, event: Event<O>) {
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

        O::effect(event, &mut self.state);

        // Check if some operations are ready to be stabilized
        self.tc_stable();
    }

    /// The TCSB middleware can offer this causal stability information through extending its API with tcstablei(τ),
    /// which informs the upper layers that message with timestamp τ is now known to be causally stable
    pub fn tc_stable(&mut self) {
        let ignore = self.group_membership.leaving_members();
        let svv = self.ltm.svv(&ignore.as_slice());
        let lower_bound = Metadata::new(svv.clone(), "", 0);
        let ready_to_stabilize = self.collect_stabilizable_events(&lower_bound);
        if !ready_to_stabilize.is_empty() {
            self.lsv = self.ltm.svv(&ignore);
        }

        for metadata in &ready_to_stabilize {
            if let Some(op) = self.state.unstable.get(&metadata.borrow()) {
                info!(
                    "[{}] - Op {} with timestamp {} is causally stable",
                    self.id.blue().bold(),
                    format!("{:?}", op).green(),
                    format!("{}", metadata.borrow()).red(),
                );
                O::stable(&metadata.borrow(), &mut self.state);
            } else {
                error!(
                    "[{}] - Op with timestamp {} is not found in the state",
                    self.id.blue().bold(),
                    format!("{}", metadata.borrow()).red(),
                );
            }
        }
    }

    /// Transfer the state of a replica to another replica.
    /// The peer giving the state should be the one that have welcomed the other peer in its group membership.
    pub fn state_transfer(&mut self, other: &mut Tcsb<O>) {
        assert!(
            self.id != other.id && other.group_membership.members().contains(&self.id),
            "Peer {} is not in the group membership of peer {}",
            self.id,
            other.id
        );
        self.state = other.state.clone();
        self.group_membership = other.group_membership.clone();
        self.ltm = other.ltm.clone();
        self.ltm.most_update(&self.id);
        self.lsv = other.lsv.clone();
        // The peer will have its clock at least as high as the one of the other peer
        let other_clock = other.my_clock().clone();
        other.ltm.get_mut(&self.id).unwrap().merge(&other_clock);
        assert_eq!(self.my_clock(), other.my_clock());
        assert_eq!(self.my_clock(), self.ltm.get(&other.id).unwrap());
        assert_eq!(other.my_clock(), other.ltm.get(&self.id).unwrap());
    }

    /// Utilitary function to evaluate the current state of the whole CRDT.
    pub fn eval(&self) -> O::Value {
        O::eval(&self.state)
    }

    pub fn install_view(&mut self, members: Vec<&str>) {
        self.installing_view(members);
        self.installed_view();
    }

    pub fn installing_view(&mut self, members: Vec<&str>) {
        if members.contains(&self.id.as_str()) {
            self.group_membership.install(
                members.iter().map(|m| m.to_string()).collect(),
                ViewStatus::Installing,
            );
        } else {
            self.group_membership
                .install(vec![self.id.clone()], ViewStatus::Installing);
        }
    }

    pub fn installed_view(&mut self) {
        self.tc_stable();
        assert!(self.state.unstable.is_empty());
        for member in self.group_membership.joining_members() {
            self.ltm.add_key(member.clone());
        }
        for member in self.group_membership.leaving_members() {
            self.ltm.remove_key(member);
        }
        self.group_membership.mark_installed();
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
            if event.metadata.get_lamport(&event.metadata.origin).unwrap() > sending_peer_lamport {
                waiting_from.insert(event.metadata.origin.clone());
            }
        }
        waiting_from
    }

    /// Returns the update clock new event of this [`Tcsb<O>`].
    fn generate_metadata_for_new_event(&mut self) -> Metadata {
        let my_id = self.id.clone();
        let clock = {
            let my_clock = self.my_clock_mut();
            my_clock.increment(&my_id);
            my_clock.clone()
        };
        Metadata::new(
            clock,
            &self.id,
            self.group_membership.current_installed_view().id,
        )
    }

    /// Returns a list of operations that are ready to be stabilized.
    fn collect_stabilizable_events(&self, lower_bound: &Metadata) -> Vec<RefCell<Metadata>> {
        let state = self
            .state
            .unstable
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .filter_map(|(m, _)| {
                if m.clock <= lower_bound.clock {
                    Some(RefCell::new(m.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<RefCell<Metadata>>>();
        state
    }
}
