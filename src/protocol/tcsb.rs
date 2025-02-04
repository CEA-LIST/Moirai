use colored::*;
use log::{error, info};
use serde::{Deserialize, Serialize};

use super::membership::Views;
use super::{event::Event, metadata::Metadata};
use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};
use crate::protocol::guard::{guard_against_duplicates, guard_against_out_of_order};
use crate::protocol::log::Log;
#[cfg(feature = "utils")]
use crate::utils::tracer::Tracer;
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;

/// # Extended Reliable Causal Broadcast (RCB) middleware API
///
/// A **Tagged Causal Stable Broadcast (TCSB)** is an extended Reliable Causal Broadcast (RCB)
/// middleware API designed to offer additional information about causality during message delivery.
/// It also notifies recipients when delivered messages achieve causal stability,
/// facilitating subsequent compaction within the Partially Ordered Log of operations (PO-Log)
#[derive(Clone)]
pub struct Tcsb<L>
where
    L: Log,
{
    /// Unique peer id
    pub id: String,
    /// Domain-specific CRDT
    pub state: L,
    /// Buffer of operations to be delivered
    pub(crate) pending: VecDeque<Event<L::Op>>,
    /// Group Membership views
    group_membership: Views,
    /// Last Timestamp Matrix (LTM) is a matrix clock that keeps track of the vector clocks of all peers.
    pub ltm: MatrixClock<String, usize>,
    /// Last Stable Vector (LSV)
    pub lsv: VectorClock<String, usize>,
    /// Trace of events for debugging purposes
    #[cfg(feature = "utils")]
    pub tracer: Tracer,
}

impl<L> Tcsb<L>
where
    L: Log,
{
    /// Create a new TCSB instance.
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            state: Default::default(),
            group_membership: Views::new(vec![id.to_string()]),
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

    /// Broadcast a new domain-specific operation to all peers and deliver it to the local state.
    pub fn tc_bcast(&mut self, op: L::Op) -> Event<L::Op> {
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
    pub fn try_deliver(&mut self, event: Event<L::Op>) {
        // The local peer should not call this function for its own events
        assert_ne!(
            self.id, event.metadata.origin,
            "Local peer {} should not be the origin {} of the event",
            self.id, event.metadata.origin
        );
        // If from evicted peer, unknown peer, duplicated event, ignore it
        if !self
            .group_membership
            .installed_view()
            .members
            .contains(&event.metadata.origin)
        {
            error!(
                "[{}] - Event from an unknown peer {} detected with timestamp {}",
                self.id.blue().bold(),
                event.metadata.origin.blue(),
                format!("{}", event.metadata.clock).red()
            );
        }
        if self.view_id() > event.metadata.view_id {
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
    fn tc_deliver(&mut self, event: Event<L::Op>) {
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

        self.state.effect(event);

        // Check if some operations are ready to be stabilized
        self.tc_stable();
    }

    /// The TCSB middleware can offer this causal stability information through extending its API with tcstablei(τ),
    /// which informs the upper layers that message with timestamp τ is now known to be causally stable
    pub fn tc_stable(&mut self) {
        let ignore = self.group_membership.leaving_members(&self.id);
        info!(
            "[{}] - Starting the stability phase with ignore list {:?}",
            self.id.blue().bold(),
            ignore
        );
        let svv = self.ltm.svv(ignore.as_slice());
        let upper_bound = Metadata::new(svv.clone(), "", self.view_id());
        let ready_to_stabilize = self.state.collect_events(&upper_bound);
        if !ready_to_stabilize.is_empty() {
            self.lsv = self.ltm.svv(&ignore);
        }

        for event in &ready_to_stabilize {
            // if let Some(op) = self.state.unstable.get(&metadata.borrow()) {
            //     info!(
            //         "[{}] - Op {} with timestamp {} is causally stable",
            //         self.id.blue().bold(),
            //         format!("{:?}", op).green(),
            //         format!("{}", metadata.borrow()).red(),
            //     );
            self.state.stable(&event.metadata);
            // } else {
            //     error!(
            //         "[{}] - Op with timestamp {} is not found in the state",
            //         self.id.blue().bold(),
            //         format!("{}", metadata.borrow()).red(),
            //     );
            // }
        }
    }

    /// Transfer the state of a replica to another replica.
    /// `self` is the peer that will receive the state of the `other` peer.
    /// The peer giving the state should be the one that have welcomed the other peer in its group membership.
    pub fn state_transfer(&mut self, other: &mut Tcsb<L>) {
        let state_transfer = StateTransfer::new(other, &self.id);

        self.deliver_state(state_transfer);
        self.ltm.most_update(&self.id);

        // The peer will have its clock at least as high as the one of the other peer
        let other_clock = other.my_clock().clone();
        other.ltm.get_mut(&self.id).unwrap().merge(&other_clock);

        assert_eq!(self.my_clock(), other.my_clock());
        assert_eq!(self.my_clock(), self.ltm.get(&other.id).unwrap());
        assert_eq!(other.my_clock(), other.ltm.get(&self.id).unwrap());
    }

    /// Utilitary function to evaluate the current state of the whole CRDT.
    pub fn eval(&self) -> L::Value {
        self.state.eval()
    }

    /// Returns the members that are in the current view and the next view.
    pub fn stable_members_in_transition(&self) -> Option<Vec<&String>> {
        self.group_membership.stable_members_in_transition()
    }

    /// Returns the members that are in the installing view.
    pub fn installing_members(&self) -> Option<Vec<&String>> {
        self.group_membership.installing_members()
    }

    /// Add a view in the queue of pending views.
    pub fn add_pending_view(&mut self, members: Vec<String>) {
        info!(
            "[{}] - Adding pending view with members {:?}",
            self.id.blue().bold(),
            members
        );
        self.group_membership.add_pending_view(members);
    }

    /// Start installing the next view.
    pub fn start_installing_view(&mut self) -> bool {
        info!(
            "[{}] - Starting to install the next view",
            self.id.blue().bold()
        );
        self.group_membership.start_installing()
    }

    /// Start a stability phase and mark the current view as installed.
    pub fn mark_installed_view(&mut self) {
        info!(
            "[{}] - Marking the installing view as installed",
            self.id.blue().bold()
        );
        self.tc_stable();
        // assert!(self.state.unstable.is_empty());
        for member in self.group_membership.joining_members() {
            self.ltm.add_key(member.clone());
        }
        for member in self.group_membership.leaving_members(&self.id) {
            self.ltm.remove_key(member);
        }
        self.group_membership.mark_installed();
        if !self.group_members().contains(&self.id) {
            self.group_membership = Views::new(vec![self.id.to_string()]);
        }
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

    /// Returns the update clock new event of this [`Tcsb<L>`].
    fn generate_metadata_for_new_event(&mut self) -> Metadata {
        let my_id = self.id.clone();
        let clock = {
            let my_clock = self.my_clock_mut();
            my_clock.increment(&my_id);
            my_clock.clone()
        };
        Metadata::new(clock, &self.id, self.view_id())
    }

    pub fn group_members(&self) -> &Vec<String> {
        &self.group_membership.installed_view().members
    }

    pub fn view_id(&self) -> usize {
        self.group_membership.installed_view().id
    }

    pub fn views(&self) -> Views {
        self.group_membership.clone()
    }

    pub fn stable_across_views(&self) -> Vec<&String> {
        self.group_membership.stable_across_views()
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StateTransfer<L> {
    pub group_membership: Views,
    pub state: L,
    pub lsv: VectorClock<String, usize>,
    pub ltm: MatrixClock<String, usize>,
}

impl<L> StateTransfer<L>
where
    L: Log,
{
    pub fn new(tcsb: &Tcsb<L>, to: &String) -> Self {
        assert!(
            &tcsb.id != to && tcsb.group_members().contains(to),
            "Peer {} should be in the group of peer {}. The group members are: {:?}",
            to,
            tcsb.id,
            tcsb.group_members()
        );
        StateTransfer {
            group_membership: tcsb.group_membership.clone(),
            state: tcsb.state.clone(),
            lsv: tcsb.lsv.clone(),
            ltm: tcsb.ltm.clone(),
        }
    }
}

impl<L> Tcsb<L>
where
    L: Log,
{
    pub fn deliver_state(&mut self, state: StateTransfer<L>) {
        self.lsv = state.lsv;
        self.ltm = state.ltm;
        self.ltm.most_update(&self.id);
        self.state = state.state;
        self.group_membership = state.group_membership;
    }
}
