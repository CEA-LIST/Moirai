// Entry point of the framework.

use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    rc::Rc,
};

use colored::*;
#[cfg(feature = "utils")]
use deepsize::DeepSizeOf;
use log::{debug, error, info, warn};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use super::{
    event::Event,
    membership::{ViewInstallingStatus, Views},
};
use crate::{
    clocks::{
        clock::{Clock, Full, Partial},
        dot::Dot,
        matrix_clock::MatrixClock,
    },
    protocol::{
        guard::{guard_against_duplicates, guard_against_out_of_order},
        log::Log,
    },
};

/// Delivery status of an event.
pub enum DeliveryStatus {
    /// Causally ready to be delivered.
    Ready,
    /// Pending, needs to wait for more events.
    Pending,
    /// Error, event cannot be delivered.
    Error,
}

/// The TCSB  (Tagged Causal Stable Broadcast) protocol is a middleware that provides causal delivery of events
/// and causal stability information.
#[derive(Clone)]
// #[cfg_attr(feature = "utils", derive(DeepSizeOf))]
pub struct Tcsb<L>
where
    L: Log,
{
    /// Unique peer id
    pub id: String,
    /// Domain-specific CRDT
    pub state: L,
    /// Buffer of operations to be delivered
    pub pending: VecDeque<Event<L::Op>>,
    /// Group Membership views
    pub group_membership: Views,
    /// Last Timestamp Matrix (LTM) is a matrix clock that keeps track of the vector clocks of all peers.
    pub ltm: MatrixClock,
    /// Last Stable Vector (LSV)
    pub lsv: Clock<Full>,
}

impl<L> Tcsb<L>
where
    L: Log,
{
    /// Create a new TCSB instance.
    pub fn new(id: &str) -> Self {
        let views = Views::new(vec![id.to_string()]);
        let id_pos = views
            .installed_view()
            .data
            .member_pos(id)
            .unwrap_or_else(|| panic!("Member {id} not found in view"));
        Self {
            id: id.to_string(),
            state: L::new(),
            ltm: MatrixClock::new(&Rc::clone(&views.installed_view().data), id_pos),
            lsv: Clock::<Full>::new(&Rc::clone(&views.installed_view().data), None),
            group_membership: views,
            pending: VecDeque::new(),
        }
    }

    /// Try to deliver an event to the local replica.
    /// If not causally ready, the event is added to the pending queue.
    pub fn try_deliver(&mut self, event: Event<L::Op>) {
        match self.can_deliver(&event) {
            DeliveryStatus::Ready => {
                self.tc_deliver(event);
                self.try_deliver_pending();
            }
            DeliveryStatus::Pending => {
                self.pending.push_back(event.clone());
            }
            _ => {}
        }
    }

    /// Check if an event can be delivered to the local replica.
    /// Returns `DeliveryStatus::Ready` if the event is causally ready to be delivered,
    /// `DeliveryStatus::Pending` if it needs to wait for more events, or `DeliveryStatus::Error` if it cannot be delivered.
    pub fn can_deliver(&self, event: &Event<L::Op>) -> DeliveryStatus {
        assert_ne!(
            self.id,
            event.origin(),
            "Local peer {} should not be the origin {} of the event",
            self.id,
            event.origin()
        );
        // If from evicted peer, unknown peer, duplicated event, ignore it
        if !self
            .group_membership
            .installed_view()
            .data
            .members
            .contains(&event.origin().to_string())
        {
            error!(
                "[{}] - Event from an unknown peer {} detected with timestamp {}",
                self.id.blue().bold(),
                event.origin().blue(),
                format!("{}", event.metadata()).red()
            );
            return DeliveryStatus::Error;
        }
        // If the event is from a previous view, ignore it
        if event.metadata().view_id() < self.view_id() {
            error!(
                "[{}] - Event from {} with a view id {} inferior to the current view id {}",
                self.id.blue().bold(),
                event.origin().blue(),
                format!("{}", event.metadata().view_id()).blue(),
                format!("{}", event.metadata()).red()
            );
            return DeliveryStatus::Error;
        }
        if guard_against_duplicates(&self.ltm, event.metadata()) {
            warn!(
                "[{}] - Duplicated event detected from {} with timestamp {}",
                self.id.blue().bold(),
                event.origin().red(),
                format!("{}", event.metadata()).red()
            );
            return DeliveryStatus::Error;
        }
        if guard_against_out_of_order(&self.ltm, event.metadata()) {
            warn!(
                "[{}] - Out-of-order event from {} detected with timestamp {}. Operation: {}",
                self.id.blue().bold(),
                event.origin().blue(),
                format!("{}", event.metadata()).red(),
                format!("{:?}", event.op).green(),
            );
            return DeliveryStatus::Pending;
        }
        DeliveryStatus::Ready
    }

    /// Try to deliver all pending events that are causally ready.
    fn try_deliver_pending(&mut self) {
        let mut i = 0;
        while i < self.pending.len() {
            if let DeliveryStatus::Ready = self.can_deliver(&self.pending[i]) {
                let event = self.pending.remove(i).unwrap();
                self.tc_deliver(event);
                i = 0; // Restart from the beginning after each successful delivery
            } else {
                i += 1;
            }
        }
    }

    /// Broadcast a new domain-specific operation to all peers and deliver it to the local state.
    pub fn tc_bcast(&mut self, op: L::Op) -> Event<L::Op> {
        let event = self.create_event(op);
        self.tc_deliver(event.clone());
        event
    }

    /// Deliver an event to the local state.
    pub(super) fn tc_deliver(&mut self, event: Event<L::Op>) {
        // If the event is not from the local replica
        if self.id != event.origin() {
            info!(
                "[{}] - {} Delivering event {} from {} with timestamp {}",
                self.id.blue().bold(),
                ">>".cyan().bold(),
                format!("{:?}", event.op).green(),
                event.origin().blue(),
                format!("{event}").red()
            );
            // Update the vector clock of the sender in the LTM
            // Increment the new peer vector clock with its actual value
            // And our own vector clock with the new event
            self.my_clock_mut().merge(event.metadata());
            // TODO: this is expensive
            let vc = self.state.clock_from_event(&event);
            self.ltm.merge_clock(&vc);
        } else {
            info!(
                "[{}] - {} Broadcasting event {} with timestamp {}",
                self.id.blue().bold(),
                "<<".yellow().bold(),
                format!("{:?}", event.op).green(),
                format!("{event}").red()
            );
        }

        let new_clock = event.metadata().clone();
        self.state.effect(event, &self.ltm);

        debug!(
            "[{}] - State eval: {}",
            self.id.blue().bold(),
            format!("{:?}", self.eval()).magenta()
        );

        // Check if some operations are ready to be stabilized
        self.tc_stable(&Some(new_clock));
    }

    /// The TCSB middleware can offer this causal stability information through extending its API with tcstablei(τ),
    /// which informs the upper layers that message with timestamp τ is now known to be causally stable
    fn tc_stable(&mut self, new_clock: &Option<Clock<Partial>>) {
        let ignore = self.group_membership.leaving_members(&self.id);
        let svv = match new_clock {
            Some(c) => self.ltm.incremental_svv(c, &self.lsv, &ignore),
            None => self.ltm.svv(&ignore),
        };

        if svv == self.lsv {
            return;
        } else {
            self.lsv = svv.clone();
            debug!(
                "[{}] - LSV updated to: {}",
                self.id.blue().bold(),
                format!("{}", self.lsv).red()
            );
        }

        self.state.stable_by_clock(&svv);
    }

    /// Transfer the state of a replica to another replica.
    /// `self` is the peer that will receive the state of the `other` peer.
    /// The peer giving the state should be the one that have welcomed the other peer in its group membership.
    pub fn state_transfer(&mut self, other: &mut Tcsb<L>)
    where
        L: Clone,
    {
        let state_transfer = StateTransfer::new(other, &self.id);

        self.deliver_state(state_transfer);
        self.ltm.most_update(&self.id);

        // The peer will have its clock at least as high as the one of the other peer
        let other_clock = other.my_clock().clone();
        other.ltm.get_mut(&self.id).unwrap().merge(&other_clock);

        assert_eq!(self.my_clock().clock, other.my_clock().clock);
        assert_eq!(
            self.my_clock().clock,
            self.ltm.get(&other.id).unwrap().clock
        );
        assert_eq!(
            other.my_clock().clock,
            other.ltm.get(&self.id).unwrap().clock
        );
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
    pub fn start_installing_view(&mut self) -> ViewInstallingStatus {
        info!(
            "[{}] - Starting to install the next view",
            self.id.blue().bold()
        );
        self.group_membership.start_installing()
    }

    /// Start a stability phase and mark the current view as installed.
    pub fn mark_view_installed(&mut self) {
        info!(
            "[{}] - Marking the installing view as installed",
            self.id.blue().bold()
        );
        self.tc_stable(&None);

        // assert!(self.state.lowest_view_id() == 0 || self.state.lowest_view_id() > self.view_id());
        let new_view = &Rc::clone(&self.group_membership.installing_view().unwrap().data);
        let pos_id = new_view.members.iter().position(|m| m == &self.id);

        match pos_id {
            Some(pos) => {
                self.ltm.change_view(new_view, pos);
                self.lsv = Clock::<Full>::new(&Rc::clone(new_view), None);
                self.group_membership.mark_installed();
            }
            None => {
                self.group_membership = Views::new(vec![self.id.to_string()]);
                self.ltm
                    .change_view(&self.group_membership.installed_view().data.clone(), 0);
                self.lsv = Clock::<Full>::new(
                    &Rc::clone(&self.group_membership.installed_view().data),
                    None,
                );
                self.tc_stable(&None);
            }
        }
    }

    /// Return the mutable vector clock of the local replica
    pub fn my_clock_mut(&mut self) -> &mut Clock<Full> {
        self.ltm.origin_clock_mut()
    }

    /// Return the vector clock of the local replica
    pub fn my_clock(&self) -> &Clock<Full> {
        self.ltm.origin_clock()
    }

    /// Returns the list of peers whose local peer is waiting for messages to deliver those previously received.
    pub fn waiting_from(&self) -> HashSet<String> {
        let mut waiting_from = HashSet::<String>::new();
        for event in self.pending.iter() {
            assert!(
                event.origin() != self.id,
                "Local peer should not be in the pending list. Event: {event:?}",
            );
            let sending_peer_clock = self.ltm.get(event.origin()).unwrap();
            let sending_peer_lamport = sending_peer_clock.get(event.origin()).unwrap();
            if event.metadata().dot_val() > sending_peer_lamport {
                waiting_from.insert(event.origin().to_owned());
            }
        }
        waiting_from
    }

    fn create_event(&mut self, op: L::Op) -> Event<L::Op> {
        if let Some(v) = self.group_membership.installing_view().cloned() {
            info!(
                "[{}] - Creating event while installing view {}",
                self.id.blue().bold(),
                v.data.id,
            );
            let my_clock = self.my_clock_mut();
            let val = my_clock.increment();
            let lamport = my_clock.lamport();
            let mut new_clock = Clock::<Partial>::new(&Rc::clone(&v.data), &self.id);
            new_clock.set(&self.id, val);
            Event::new(op, new_clock, lamport)
        } else {
            let my_clock = self.my_clock_mut();
            let pos = my_clock
                .origin
                .expect("Local peer should have an origin in its clock");
            my_clock.increment();
            let val = my_clock.dot_val();
            let lamport = my_clock.lamport();
            let view = &self.group_membership.installed_view().data;
            let dot = Dot::new(pos, val, lamport, view);
            let mut clocks = VecDeque::new();
            self.state.deps(&mut clocks, view, &dot, &op);
            Event::new_nested(op.clone(), clocks, lamport)
        }
    }

    pub fn group_members(&self) -> &Vec<String> {
        &self.group_membership.installed_view().data.members
    }

    pub fn view_id(&self) -> usize {
        self.group_membership.installed_view().data.id
    }

    pub fn last_view_id(&self) -> usize {
        self.group_membership.last_view().data.id
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
#[cfg(feature = "utils")]
#[cfg_attr(feature = "utils", derive(DeepSizeOf))]
pub struct StateTransfer<L> {
    pub group_membership: Views,
    pub state: L,
    pub lsv: Clock<Full>,
    pub ltm: MatrixClock,
}

impl<L> StateTransfer<L>
where
    L: Log + Clone,
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
        let pos_id = self
            .group_membership
            .installed_view()
            .data
            .members
            .iter()
            .position(|m| m == &self.id)
            .expect("Local peer id should be in the group membership");
        self.ltm.set_id(pos_id);
    }
}
