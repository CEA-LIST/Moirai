use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};
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
    pub ltm: MatrixClock<K, C>,
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
        let event: Event<K, C, O>;
        if let Message::Membership(Membership::Welcome(_)) = message {
            event = Event::new(
                message,
                VectorClock::from(&self.ltm.keys(), &vec![C::default(); self.ltm.len()]),
                self.id.clone(),
            );
        } else {
            let my_id = self.id.clone();
            let my_vc = self.my_vc();
            my_vc.increment(&my_id);
            event = Event::new(message, my_vc.clone(), self.id.clone());
        }
        self.tc_deliver(event.clone());
        event
    }

    /// Deliver an event to the local state.
    pub fn tc_deliver(&mut self, event: Event<K, C, O>) {
        if let Err(err) = self.guard(&event) {
            eprintln!("Error: {}", err);
            return;
        }
        if let Event::MembershipEvent(ref membership_event) = event {
            Membership::effect(membership_event, self);
        }
        if self.id != event.metadata().origin
            && self
                .my_vc()
                .clock
                .keys()
                .any(|k| k == &event.metadata().origin)
        {
            // TODO: rejoin is hard
            // Update the vector clock of the sender in the LTM
            // Increment the new peer vector clock with its actual value
            self.ltm
                .update(&event.metadata().origin, &event.metadata().vc);
            // Update our own vector clock
            self.my_vc().merge(&event.metadata().vc);
        }
        if let Event::OpEvent(op_event) = event {
            O::effect(op_event, &mut self.state);
        }

        // Check if some operations are ready to be stabilized
        self.tc_stable();
    }

    pub fn tc_stable(&mut self) {
        // If an Evict event is in the PO-Log, the LTM svv() should ignore the evicted peer
        let evicted = self
            .state
            .1
            .iter()
            .filter_map(|(_, o)| {
                if let Message::Membership(Membership::Evict(k)) = o {
                    return Some(k.to_owned());
                }
                None
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

        // TODO: if evict concurrent with other events, it should be the last event processed to avoid inconsistencies (e.g. removing a peer while we still need it to determine causal order)

        for i in 0..ready_to_stabilize.len() {
            let message = self.state.1.get(&ready_to_stabilize[i]);
            if let Some(Message::Membership(Membership::Evict(_))) = message {
                if i != ready_to_stabilize.len() - 1
                    && matches!(
                        PartialOrd::partial_cmp(
                            &ready_to_stabilize[i + 1].vc,
                            &ready_to_stabilize[i].vc
                        ),
                        None
                    )
                {
                    ready_to_stabilize[i] = ready_to_stabilize[i + 1].clone();
                    ready_to_stabilize[i + 1] = ready_to_stabilize[i].clone();
                }
            }
            if let Some(Message::Op(_)) = message {
                O::stable(&ready_to_stabilize[i], &mut self.state);
            } else if let Some(Message::Membership(_)) = message {
                Membership::stable(&ready_to_stabilize[i], self);
            }
        }
    }

    /// Shortcut to evaluate the current state of the CRDT.
    pub fn eval(&self) -> O::Value {
        O::eval(&self.state)
    }

    pub(crate) fn my_vc(&mut self) -> &mut VectorClock<K, C> {
        self.ltm
            .get_mut(&self.id)
            .expect("Local vector clock not found")
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
        // if self.guard_against_events_from_evicted_nodes(event) {
        //     return Err("Event from evicted node detected");
        // }
        Ok(())
    }

    /// Check that the event has not already been delivered
    fn guard_against_duplicates(&self, event: &Event<K, C, O>) -> bool {
        self.id != event.metadata().origin
            && self
                .ltm
                .get(&event.metadata().origin)
                .map(|ltm_clock| event.metadata().vc <= ltm_clock)
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

    // Check that the event is not from an evicted peer
    // fn guard_against_events_from_evicted_nodes(&self, event: &Event<K, C, O>) -> bool {
    //     self.state.1.iter().any(|(m, o)| {
    //         if let Message::Membership(Membership::Evict(k)) = o {
    //             return k == &event.metadata().origin && event.metadata() > m;
    //         }
    //         false
    //     })
    // }
}
