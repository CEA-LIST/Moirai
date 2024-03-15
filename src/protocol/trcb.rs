use log::{error, info};
use std::hash::Hash;
use std::ops::Add;
use std::{cmp::Ordering, fmt::Debug, ops::AddAssign};

use crate::clocks::matrix_clock::MatrixClock;
use crate::clocks::vector_clock::VectorClock;

use super::event::{Event, Message, OpEvent, ProtocolCmd};
use super::op_rules::OpRules;

pub type POLog<K, C, M> = Vec<Event<K, C, M>>;
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
    pub state: Vec<O>,
    pub ltm: MatrixClock<K, C>, // Last Timestamp Matrix (LTM): each row j of the LTM is the version vector of the most recently delivered message from the node j
    pub lvv: VectorClock<K, C>, // Last Vector Version (LVV): latest known version vector of the node i
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
            lvv: VectorClock::new(id.clone()),
            id,
            peers: vec![],
        }
    }

    /// Broadcast a new operation to all peers and deliver it to the local state.
    pub fn tc_bcast(&mut self, message: Message<O>) -> Event<K, C, O> {
        self.lvv.increment(&self.id);
        self.ltm.update(&self.id, &self.lvv);
        let event = Event::new(message, self.lvv.clone(), self.id.clone());
        self.tc_deliver(event.clone());
        event
    }

    /// Deliver an event to the local state.
    pub fn tc_deliver(&mut self, event: Event<K, C, O>) {
        if self.id != *event.origin() {
            if let Event::ProtocolEvent(ref protocol_event) = event {
                match protocol_event.cmd {
                    ProtocolCmd::Join => {
                        self.peers.push(event.origin().clone());
                        self.ltm.add_key(event.origin().clone());
                        self.lvv.increment(event.origin());
                    }
                    ProtocolCmd::Leave => (),
                }
            }
            if *event.vc() < self.lvv {
                error!(
                    "Event from {:?} with message {:?} received in {:?} is behind the LVV. LVV is {:?} while event VC is {:?}",
                    event.origin(), event.message(), self.id, self.lvv, event.vc()
                );
                return;
            }
            if let Some(new_lc) = event.vc().get(event.origin()) {
                if let Some(old_vc) = self.ltm.get(event.origin()) {
                    if let Some(old_lc) = old_vc.get(event.origin()) {
                        if old_lc.clone() + C::from(1) != new_lc {
                            error!("Event from {:?} with message {:?} received in {:?} is not causally ready. Local clock is {:?} while event clock is {:?}", event.origin(), event.message(), self.id, old_lc, new_lc);
                            return;
                        }
                    }
                }
            }
            self.lvv.merge(event.vc());
            self.ltm.update(event.origin(), event.vc());
        } else {
            info!(
                "Delivering event from {:?} with message {:?} to itself",
                event.origin(),
                event.message()
            );
        }
        if let Event::OpEvent(op_event) = event {
            self.effect(op_event);
        }
        let partition = self.tc_stable();
        self.stable(partition);
    }

    fn tc_stable(&mut self) -> StableUnstable<K, C, O> {
        self.po_log.iter().cloned().partition(|e| {
            let ord = PartialOrd::partial_cmp(e.vc(), &self.ltm.min());
            matches!(ord, Some(Ordering::Less) | Some(Ordering::Equal))
        })
    }

    fn effect(&mut self, event: OpEvent<K, C, O>) {
        // The state is updated by removing all previous events in the state that are made obsolete by the new event.
        self.state.retain(|o| {
            let mut bottom_vc = VectorClock::<K, C>::new(self.id.clone());
            self.peers.iter().for_each(|p| {
                bottom_vc.increment(p);
            });
            let old_event = Event::new_op(o.clone(), bottom_vc, self.id.clone());
            !O::obsolete(&old_event, &event)
        });

        // The PO-Log is updated by removing all previous events that are made obsolete by the new event.
        self.po_log.retain(|e: &Event<K, C, O>| match e {
            Event::OpEvent(old_event) => !O::obsolete(old_event, &event),
            Event::ProtocolEvent(_) => true,
        });

        // If no previous event in the PO-Log makes the new event obsolete, then the new event is added to the PO-Log.
        if !self.po_log.iter().any(|e| match e {
            Event::OpEvent(old_event) => O::obsolete(&event, old_event),
            Event::ProtocolEvent(_) => false,
        }) {
            self.po_log.push(Event::OpEvent(event.clone()));
        }
    }

    fn stable(&mut self, partition: StableUnstable<K, C, O>) {
        let (stable, unstable) = partition;
        if !stable.is_empty() {
            info!("Some events have become stable in {:?}", self.id);
        }
        self.state.extend(stable.iter().filter_map(|e| match e {
            Event::OpEvent(op) => Some(op.op.clone()),
            Event::ProtocolEvent(_) => None, // We don't store any protocol event
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
