use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Bound;

use super::{
    event::{Event, Message, OpEvent},
    membership::Membership,
    metadata::Metadata,
    pure_crdt::PureCRDT,
    utils::{Incrementable, Keyable},
};

/// The status of the TCSB middleware.
pub enum Status {
    Disconnected,
    Connecting,
    Peer,
}

pub type RedundantRelation<K, C, O> = fn(&OpEvent<K, C, O>, &OpEvent<K, C, O>) -> bool;

/// A Partially Ordered Log (PO-Log), is a chronological record that
/// preserves all executed operations alongside their respective timestamps.
/// In actual implementations, the PO-Log can be split in two components:
/// one that simply stores the set of stable operations and the other stores the timestamped operations.
pub type POLog<K, C, O> = (Vec<O>, BTreeMap<Metadata<K, C>, Message<K, C, O>>);

/// A Tagged Causal Stable Broadcast (TCSB) is an extended Reliable Causal Broadcast (RCB)
/// middleware API designed to offer additional information about causality during message delivery.
/// It also notifies recipients when delivered messages achieve causal stability,
/// facilitating subsequent compaction within the Partially Ordered Log of operations (PO-Log)
pub struct Tcsb<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT + Clone + Debug,
{
    pub id: K,
    pub state: POLog<K, C, O>,
    /// Last Timestamp Matrix (LTM) is a matrix clock that keeps track of the vector clocks of all peers.
    pub ltm: MatrixClock<K, C>,
    /// Last Stable Vector (LSV)
    pub lsv: VectorClock<K, C>,
    pub status: Status,
}

impl<K, C, O> Tcsb<K, C, O>
where
    K: Keyable + Clone + Debug,
    C: Incrementable<C> + Clone + Debug,
    O: PureCRDT + Clone + Debug,
{
    pub fn new(id: K) -> Self {
        Self {
            id: id.clone(),
            state: (vec![], BTreeMap::new()),
            ltm: MatrixClock::new(&[id.clone()]),
            lsv: VectorClock::new(id),
            status: Status::Disconnected,
        }
    }

    /// Broadcast a new operation to all peers and deliver it to the local state.
    pub fn tc_bcast(&mut self, message: Message<K, C, O>) -> Event<K, C, O> {
        if self.guard_against_bcast_while_wrong_status(&message) {
            panic!("Wrong status detected");
        }
        let event: Event<K, C, O>;
        if let Message::Membership(Membership::Welcome(_)) = message {
            event = Event::new(
                message,
                VectorClock::from(&self.ltm.keys(), &vec![C::default(); self.ltm.len()]),
                self.id.clone(),
            );
        } else {
            let my_id = self.id.clone();
            let my_vc = self.my_vc_mut();
            my_vc.increment(&my_id);
            event = Event::new(message, my_vc.clone(), self.id.clone());
        }
        self.tc_deliver(event.clone());
        event
    }

    /// Deliver an event to the local state.
    pub fn tc_deliver(&mut self, mut event: Event<K, C, O>) {
        // Check if the event is valid
        if let Err(err) = self.guard(&event) {
            eprintln!("{}", err);
            return;
        }
        if let Event::MembershipEvent(ref membership_event) = event {
            Membership::effect(membership_event, self);
        }
        // If the event is not from the local replica and the sender is known
        if self.id != event.metadata().origin
            && self
                .my_vc()
                .keys()
                .iter()
                .any(|k| k == &event.metadata().origin)
        {
            // Update the vector clock of the sender in the LTM
            // Increment the new peer vector clock with its actual value
            self.ltm
                .update(&event.metadata().origin, &event.metadata().vc);
            // Update our own vector clock
            self.my_vc_mut().merge(&event.metadata().vc);
        }

        event = self.correct_evict_inconsistencies(event);

        if let Event::OpEvent(op_event) = event {
            O::effect(op_event, &mut self.state);
        }

        // Check if some operations are ready to be stabilized
        self.tc_stable();
    }

    /// The TCSB middleware can offer this causal stability information through extending its API with tcstablei(τ),
    /// which informs the upper layers that message with timestamp τ is now known to be causally stable
    pub fn tc_stable(&mut self) {
        // If an Evict event is in the PO-Log, the LTM svv() should ignore the evicted peer
        // We are going to accept all concurrent events from the evicted peer
        // until the evict message is "partially stable" (i.e., all peers except the evicted one have processed the evict message)
        let evicted = self
            .state
            .1
            .iter()
            .filter_map(|(_, o)| match o {
                Message::Membership(Membership::Evict(k)) => Some(k.to_owned()),
                _ => None,
            })
            .collect::<Vec<K>>();

        let lower_bound = Metadata {
            vc: self.ltm.svv(evicted.as_slice()),
            wc: 0,
            origin: K::default(),
        };

        let mut ready_to_stabilize = self
            .state
            .1
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .map(|(m, _)| m.clone())
            .collect::<Vec<Metadata<K, C>>>();

        // If there exists an evict message in the PO-Log along with concurrent events,
        // the evict message must be processed after all concurrent events
        ready_to_stabilize.sort_by(|a, b| {
            let message_a = self.state.1.get(a);
            let message_b = self.state.1.get(b);
            if PartialOrd::partial_cmp(&a.vc, &b.vc).is_none() {
                if matches!(message_a, Some(Message::Membership(Membership::Evict(_)))) {
                    return Ordering::Greater;
                } else if matches!(message_b, Some(Message::Membership(Membership::Evict(_)))) {
                    return Ordering::Less;
                }
            }
            Ordering::Equal
        });

        for metadata in ready_to_stabilize.iter() {
            let message = self.state.1.get(metadata);
            if let Some(Message::Op(_)) = message {
                O::stable(metadata, &mut self.state);
            } else if let Some(Message::Membership(_)) = message {
                Membership::stable(metadata, self);
            }
        }
    }

    /// Shortcut to evaluate the current state of the CRDT.
    pub fn eval(&self) -> O::Value {
        O::eval(&self.state)
    }

    /// Return the vector clock of the local replica
    pub(crate) fn my_vc(&self) -> &VectorClock<K, C> {
        self.ltm
            .get(&self.id)
            .expect("Local vector clock not found")
    }

    /// Return the mutable vector clock of the local replica
    pub(crate) fn my_vc_mut(&mut self) -> &mut VectorClock<K, C> {
        self.ltm
            .get_mut(&self.id)
            .expect("Local vector clock not found")
    }

    /// If there is a missing entry in the incoming event's vc that we know
    /// and there is an evict message for this entry, we add to the incoming event's vc
    /// the value of the last lamport clock of the evict node + 1
    /// This is to avoid inconsistencies when an event is sent from a node
    /// that has already processed the evict message
    fn correct_evict_inconsistencies(&self, mut event: Event<K, C, O>) -> Event<K, C, O> {
        let ltm_keys = self.ltm.keys();
        let missing_entry: Option<&K> = ltm_keys
            .iter()
            .find(|k| event.metadata().vc.get(k).is_none());

        if let Some(missing_entry) = missing_entry {
            if self.state.1.iter().any(|(_, o)| {
                if let Message::Membership(Membership::Evict(k)) = o {
                    return k == missing_entry;
                }
                false
            }) {
                let last_lamport_clock = self.my_vc().get(missing_entry).unwrap();
                let mut vc = event.metadata().vc.clone();
                vc.insert(missing_entry.clone(), last_lamport_clock + 1.into());
                event.metadata_mut().vc = vc;
            }
        }
        event
    }

    fn guard(&self, event: &Event<K, C, O>) -> Result<(), &str> {
        if self.guard_against_unknow_peer(event) {
            return Err("Unknown peer detected");
        }
        if self.guard_against_duplicates(event) {
            return Err("Duplicated event detected");
        }
        if self.guard_against_out_of_order(event) {
            return Err("Out-of-order event detected");
        }
        Ok(())
    }

    /// Check that the event has not already been delivered
    fn guard_against_duplicates(&self, event: &Event<K, C, O>) -> bool {
        self.id != event.metadata().origin
            && self
                .ltm
                .get(&event.metadata().origin)
                .map(|ltm_clock| event.metadata().vc <= *ltm_clock)
                .unwrap_or(false)
    }

    /// Check that the event is the causal successor of the last event delivered by this same replica
    fn guard_against_out_of_order(&self, event: &Event<K, C, O>) -> bool {
        self.id != event.metadata().origin && {
            let event_lamport_clock = event.metadata().vc.get(&event.metadata().origin).unwrap();
            let ltm_vc_clock = self.ltm.get(&event.metadata().origin);
            if let Some(ltm_vc_clock) = ltm_vc_clock {
                let ltm_lamport_lock = ltm_vc_clock.get(&event.metadata().origin).unwrap();
                return event_lamport_clock != ltm_lamport_lock + 1.into();
            }
            false
        }
    }

    /// Check that the event is not from an unknown peer
    fn guard_against_unknow_peer(&self, event: &Event<K, C, O>) -> bool {
        self.ltm.get(&event.metadata().origin).is_none()
            && match event {
                Event::OpEvent(_) => true,
                Event::MembershipEvent(membership_event) => {
                    !matches!(membership_event.cmd, Membership::Join)
                        && !matches!(membership_event.cmd, Membership::Welcome(_))
                }
            }
    }

    /// Check that the event is authorized to be broadcasted
    fn guard_against_bcast_while_wrong_status(&self, message: &Message<K, C, O>) -> bool {
        match message {
            Message::Op(_) => matches!(self.status, Status::Connecting),
            Message::Membership(membership) => match membership {
                Membership::Join => !matches!(self.status, Status::Disconnected),
                Membership::Welcome(_) => !matches!(self.status, Status::Peer),
                Membership::Leave => !matches!(self.status, Status::Peer),
                Membership::Evict(_) => !matches!(self.status, Status::Peer),
            },
        }
    }
}
