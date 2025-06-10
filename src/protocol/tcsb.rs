use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    rc::Rc,
};

use colored::*;
use log::{debug, error, info};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use super::{
    event::Event,
    membership::{ViewInstallingStatus, Views},
};
#[cfg(feature = "tracer")]
use crate::utils::tracer::Tracer;
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

pub enum DeliveryStatus {
    Ready,
    Pending,
    Error,
}

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
    pub pending: VecDeque<Event<L::Op>>,
    /// Group Membership views
    pub group_membership: Views,
    /// Last Timestamp Matrix (LTM) is a matrix clock that keeps track of the vector clocks of all peers.
    pub ltm: MatrixClock,
    /// Last Stable Vector (LSV)
    pub lsv: Clock<Full>,
    /// Trace of events for debugging purposes
    #[cfg(feature = "tracer")]
    pub tracer: Tracer,
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
            .unwrap_or_else(|| panic!("Member {} not found in view", id));
        Self {
            id: id.to_string(),
            state: L::new(),
            ltm: MatrixClock::new(&Rc::clone(&views.installed_view().data), id_pos),
            lsv: Clock::<Full>::new(&Rc::clone(&views.installed_view().data), None),
            group_membership: views,
            pending: VecDeque::new(),
            #[cfg(feature = "tracer")]
            tracer: Tracer::new(String::from(id)),
        }
    }

    #[cfg(feature = "tracer")]
    /// Create a new TCSB instance with a tracer for debugging purposes.
    pub fn new_with_trace(id: &str) -> Self {
        use log::warn;
        warn!("[{}] - Creating a new TCSB instance with a tracer", id);
        let mut tcsb = Self::new(id);
        tcsb.tracer = Tracer::new(String::from(id));
        tcsb
    }

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

    fn can_deliver(&self, event: &Event<L::Op>) -> DeliveryStatus {
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
            error!(
                "[{}] - Duplicated event detected from {} with timestamp {}",
                self.id.blue().bold(),
                event.origin().red(),
                format!("{}", event.metadata()).red()
            );
            return DeliveryStatus::Error;
        }
        if guard_against_out_of_order(&self.ltm, event.metadata()) {
            error!(
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
        #[cfg(feature = "tracer")]
        self.tracer.append::<L>(event.clone());
        event
    }

    /// Deliver an event to the local state.
    pub(super) fn tc_deliver(&mut self, event: Event<L::Op>) {
        info!(
            "[{}] - Delivering event {} from {} with timestamp {}",
            self.id.blue().bold(),
            format!("{:?}", event.op).green(),
            event.origin().blue(),
            format!("{}", event.metadata()).red()
        );
        // If the event is not from the local replica
        if self.id != event.origin() {
            // Update the vector clock of the sender in the LTM
            // Increment the new peer vector clock with its actual value
            // And our own vector clock with the new event
            self.my_clock_mut().merge(event.metadata());
            let vc = self.state.vector_clock_from_event(&event);
            self.ltm.merge_clock(&vc);

            #[cfg(feature = "tracer")]
            self.tracer.append::<L>(event.clone());
        }

        let new_clock = event.metadata().clone();
        self.state.effect(event, &self.ltm);

        // Check if some operations are ready to be stabilized
        self.tc_stable(&Some(new_clock));
    }

    /// The TCSB middleware can offer this causal stability information through extending its API with tcstablei(τ),
    /// which informs the upper layers that message with timestamp τ is now known to be causally stable
    pub fn tc_stable(&mut self, new_clock: &Option<Clock<Partial>>) {
        let ignore = self.group_membership.leaving_members(&self.id);
        info!(
            "[{}] - Starting the stability phase with ignore list {:?}",
            self.id.blue().bold(),
            ignore
        );

        let svv = match new_clock {
            Some(c) => self.ltm.incremental_svv(c, &self.lsv, &ignore),
            None => self.ltm.svv(&ignore),
        };

        if svv == self.lsv {
            debug!(
                "[{}] - SVV is the same as LSV: {}",
                self.id.blue().bold(),
                svv
            );
            return;
        } else {
            self.lsv = svv.clone();
            debug!("[{}] - LSV updated to: {}", self.id.blue().bold(), self.lsv);
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
                "Local peer should not be in the pending list. Event: {:?}",
                event
            );
            let sending_peer_clock = self.ltm.get(event.origin()).unwrap();
            let sending_peer_lamport = sending_peer_clock.get(event.origin()).unwrap();
            if event.metadata().dot() > sending_peer_lamport {
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
            let mut new_clock = Clock::<Partial>::new(&Rc::clone(&v.data), &self.id);
            new_clock.set(&self.id, val);
            Event::new(op, new_clock)
        } else {
            let my_clock = self.my_clock_mut();
            my_clock.increment();
            let view = &self.group_membership.installed_view().data;
            let dot = Dot::from(self.my_clock());

            let mut clocks = VecDeque::new();
            self.state.deps(&mut clocks, view, &dot, &op);
            Event::new_nested(op.clone(), clocks)
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
