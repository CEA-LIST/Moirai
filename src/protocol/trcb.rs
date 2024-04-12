use log::{error, info};
use std::hash::Hash;
use std::ops::Add;
use std::{cmp::Ordering, fmt::Debug, ops::AddAssign};

use crate::clocks::matrix_clock::MatrixClock;
use crate::clocks::vector_clock::VectorClock;

use super::event::{Event, Message, OpEvent, ProtocolCmd, ProtocolEvent};
use super::op_rules::OpRules;

pub type POLog<K, C, M> = Vec<Event<K, C, M>>; // TODO: Use a BTreeMap
pub type StableUnstable<K, C, M> = (Vec<Event<K, C, M>>, Vec<Event<K, C, M>>);

#[derive(Debug)]
pub struct Trcb<K, C, O>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    O: Clone + Debug + OpRules,
{
    pub id: K,
    pub po_log: POLog<K, C, O>,
    pub state: Vec<O>,          // TODO: Rename to 'stable'?
    pub ltm: MatrixClock<K, C>, // Last Timestamp Matrix (LTM): each row j of the LTM is the version vector of the most recently delivered message from the node j
    pub lsv: VectorClock<K, C>, // Last Stable Version (LSV): the vector clock of the last stabilized event
    pub peers: Vec<K>,
}

impl<K, C, O> Trcb<K, C, O>
where
    K: PartialOrd + Hash + Eq + Clone + Debug,
    C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    O: Clone + Debug + OpRules,
{
    pub fn new(id: K) -> Self {
        Self {
            po_log: vec![],
            state: vec![],
            ltm: MatrixClock::new(&[id.clone()]),
            lsv: VectorClock::new(id.clone()),
            id,
            peers: vec![], // TODO: remove this field because the peers are already stored in the LTM
        }
    }

    /// Broadcast a new operation to all peers and deliver it to the local state.
    pub fn tc_bcast(&mut self, message: Message<K, O>) -> Event<K, C, O> {
        let my_vc = self
            .ltm
            .get_mut(&self.id)
            .expect("Local vector clock not found");
        my_vc.increment(&self.id);
        let event = Event::new(message, my_vc.clone(), self.id.clone());
        self.tc_deliver(event.clone());
        event
    }

    /// Deliver an event to the local state.
    pub fn tc_deliver(&mut self, event: Event<K, C, O>) {
        if self.id != event.metadata().origin {
            if let Event::ProtocolEvent(ref protocol_event) = event {
                match protocol_event.cmd {
                    ProtocolCmd::Join => {
                        self.peers.push(event.metadata().origin.clone());
                        self.ltm.add_key(event.metadata().origin.clone());
                        assert!(self.ltm.is_square());
                    }
                    ProtocolCmd::Leave => (),
                    ProtocolCmd::KickOut(_) => todo!(),
                }
            }
            // Skip duplicates
            let my_vc = self
                .ltm
                .get(&self.id)
                .expect("Local vector clock not found");
            let local_clock = my_vc
                .get(&event.metadata().origin)
                .expect("Vector clock not found");
            let event_clock = event
                .metadata()
                .vc
                .get(&event.metadata().origin)
                .expect("Vector clock not found");
            if local_clock > event_clock {
                error!("Event from {:?} with message {:?} received in {:?} is a duplicate. Local clock is {:?} while event clock is {:?}", event.metadata().origin, event.message(), self.id, local_clock, event_clock);
                return;
            }

            self.ltm
                .update(&event.metadata().origin, &event.metadata().vc);
            let mut new_vc = self
                .ltm
                .get(&event.metadata().origin)
                .expect("Vector clock not found")
                .clone();
            new_vc.merge(&event.metadata().vc);
            self.ltm.update(&self.id, &new_vc);
            assert!(self.ltm.is_square());
        } else {
            info!(
                "Delivering event from {:?} with message {:?} to itself",
                event.metadata().origin,
                event.message()
            );
        }
        self.effect(event);
        let partition = self.tc_stable();
        self.stabilise(partition);
    }

    /// Split the PO-Log into stable and unstable events.
    fn tc_stable(&mut self) -> StableUnstable<K, C, O> {
        self.po_log.iter().cloned().partition(|e| {
            let ord = PartialOrd::partial_cmp(&e.metadata().vc, &self.ltm.min());
            matches!(ord, Some(Ordering::Less) | Some(Ordering::Equal))
        })
    }

    /// Apply the effect of an operation to the local state.
    /// Check if the operation is obsolete and update the PO-Log accordingly.
    fn effect(&mut self, event: Event<K, C, O>) {
        match event {
            Event::OpEvent(op_event) => {
                // The state is updated by removing all previous events in the state that are made obsolete by the new event.
                // No protocol events are stored in the state. Only operation events are stored.
                self.state.retain(|o| {
                    let mut bottom_vc = VectorClock::<K, C>::new(self.id.clone());
                    self.peers.iter().for_each(|p| {
                        bottom_vc.increment(p);
                    });
                    let old_event = Event::new_op(o.clone(), bottom_vc, self.id.clone());
                    !O::obsolete(&old_event, &op_event)
                });

                // The PO-Log is updated by removing all previous events that are made obsolete by the new event.
                self.po_log.retain(|e: &Event<K, C, O>| match e {
                    Event::OpEvent(old_event) => !O::obsolete(old_event, &op_event),
                    Event::ProtocolEvent(_) => true,
                });

                // If no previous event in the PO-Log makes the new event obsolete, then the new event is added to the PO-Log.
                if !self.po_log.iter().any(|e| match e {
                    Event::OpEvent(old_event) => O::obsolete(&op_event, old_event),
                    Event::ProtocolEvent(_) => false,
                }) {
                    self.po_log.push(Event::OpEvent(op_event));
                }
            }
            Event::ProtocolEvent(protocol_event) => {
                self.po_log.retain(|e: &Event<K, C, O>| match e {
                    Event::OpEvent(_) => true,
                    Event::ProtocolEvent(old_event) => {
                        ProtocolEvent::obsolete(old_event, &protocol_event)
                    }
                });

                if !self.po_log.iter().any(|e| match e {
                    Event::OpEvent(_) => false,
                    Event::ProtocolEvent(old_event) => {
                        ProtocolEvent::obsolete(&protocol_event, old_event)
                    }
                }) {
                    self.po_log.push(Event::ProtocolEvent(protocol_event));
                }
            }
        }
    }

    fn stabilise(&mut self, partition: StableUnstable<K, C, O>) {
        let (stable, unstable) = partition;
        if !stable.is_empty() {
            info!("Some events have become stable in {:?}", self.id);
        }
        self.state.extend(stable.iter().filter_map(|e| match e {
            Event::OpEvent(op) => Some(op.op.clone()),
            Event::ProtocolEvent(e) => {
                match &e.cmd {
                    ProtocolCmd::Join => {}
                    ProtocolCmd::Leave => {
                        self.peers.retain(|p| p != &e.metadata.origin);
                        self.ltm.remove_key(&e.metadata.origin);
                        assert!(self.ltm.is_square());
                    }
                    ProtocolCmd::KickOut(kick_out) => {
                        self.peers.retain(|p| p != kick_out);
                        self.ltm.remove_key(kick_out);
                        assert!(self.ltm.is_square());
                    }
                }
                None // We don't store any protocol event
            }
        }));
        self.po_log = unstable;
    }

    pub fn eval(&self) -> O::Value {
        let op_event = self
            .po_log
            .iter()
            .filter_map(|event| {
                if let Event::OpEvent(op_event) = event {
                    Some(op_event)
                } else {
                    None
                }
            })
            .collect::<Vec<&OpEvent<K, C, O>>>();
        O::eval(&op_event, &self.state)
    }
}
