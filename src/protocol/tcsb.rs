use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Bound;

use super::{
    event::{Event, Message, OpEvent},
    metadata::Metadata,
    pure_crdt::PureCRDT,
    utils::{Incrementable, Keyable},
};

pub type RedundantRelation<K, C, O> = fn(&OpEvent<K, C, O>, &OpEvent<K, C, O>) -> bool;

/// A Partially Ordered Log (PO-Log), is a chronological record that
/// preserves all executed operations alongside their respective timestamps.
/// In actual implementations, the PO-Log can be split in two components:
/// one that simply stores the set of stable operations and the other stores the timestamped operations.
pub type POLog<K, C, O> = (Vec<O>, BTreeMap<Metadata<K, C>, Message<K, O>>);

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
        }
    }

    /// Broadcast a new operation to all peers and deliver it to the local state.
    pub fn tc_bcast(&mut self, message: Message<K, O>) -> Event<K, C, O> {
        let my_id = self.id.clone();
        let my_vc = self.my_vc();
        my_vc.increment(&my_id);
        let event = Event::new(message, my_vc.clone(), self.id.clone());
        self.tc_deliver(event.clone());
        event
    }

    /// Deliver an event to the local state.
    pub fn tc_deliver(&mut self, event: Event<K, C, O>) {
        if self.id != event.metadata().origin {
            // TODO: guard against duplicates
            // TODO: guard against out-of-order delivery
            if self.ltm.get(&event.metadata().origin).is_none() {
                // Fill the existing vector clocks with the new peer entry (initially set to 0)
                // Create a new vector clock for the new peer (initially set to 0)
                self.ltm.add_key(event.metadata().origin.clone());
            }
            // Update the vector clock of the sender in the LTM
            // Increment the new peer vector clock with its actual value
            self.ltm
                .update(&event.metadata().origin, &event.metadata().vc);
            // Update our own vector clock
            self.my_vc().merge(&event.metadata().vc);
            assert!(self.ltm.is_square());
        }
        if let Event::OpEvent(op_event) = event {
            O::effect(op_event, &mut self.state);
        } // TODO: handle the case of a protocol event

        // Check if some operations are ready to be stabilized
        self.tc_stable();
    }

    pub fn tc_stable(&mut self) {
        let lower_bound = Metadata {
            vc: self.ltm.min(),
            wc: 0,
            origin: K::default(),
        };

        let ready_to_stabilize = self
            .state
            .1
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .map(|(m, _)| m.clone())
            .collect::<Vec<Metadata<K, C>>>();

        for metadata in ready_to_stabilize {
            O::stable(&metadata, &mut self.state);
        }
    }

    pub fn eval(&self) -> O::Value {
        O::eval(&self.state)
    }

    fn my_vc(&mut self) -> &mut VectorClock<K, C> {
        self.ltm
            .get_mut(&self.id)
            .expect("Local vector clock not found")
    }
}
