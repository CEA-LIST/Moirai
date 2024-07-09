use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};
use std::fmt::Debug;
use std::ops::Bound;
use std::{collections::BTreeMap, path::PathBuf};

use super::event::NestedOp;
use super::{
    event::Event,
    metadata::Metadata,
    pure_crdt::PureCRDT,
    utils::{Incrementable, Keyable},
};

pub type RedundantRelation<K, C, O> = fn(&Event<K, C, O>, &Event<K, C, O>) -> bool;

/// A Partially Ordered Log (PO-Log), is a chronological record that
/// preserves all executed operations alongside their respective timestamps.
/// In actual implementations, the PO-Log can be split in two components:
/// one that simply stores the set of stable operations and the other stores the timestamped operations.
pub type POLog<K, C, O> = (Vec<NestedOp<O>>, BTreeMap<Metadata<K, C>, NestedOp<O>>);

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
    pub fn tc_bcast(&mut self, op: O) -> Event<K, C, O> {
        let my_id = self.id.clone();
        let my_vc = self.my_vc_mut();
        my_vc.increment(&my_id);
        let metadata = Metadata::new(my_vc.clone(), self.id.clone());
        let event = Event::new(PathBuf::default(), op, metadata);
        self.tc_deliver(event.clone());
        event
    }

    /// Deliver an event to the local state.
    pub fn tc_deliver(&mut self, event: Event<K, C, O>) {
        // Check if the event is valid
        if let Err(err) = self.guard(&event) {
            eprintln!("{}", err);
            return;
        }
        // If the event is not from the local replica and the sender is known
        if self.id != event.metadata.origin
            && self
                .my_vc()
                .keys()
                .iter()
                .any(|k| k == &event.metadata.origin)
        {
            // Update the vector clock of the sender in the LTM
            // Increment the new peer vector clock with its actual value
            self.ltm.update(&event.metadata.origin, &event.metadata.vc);
            // Update our own vector clock
            self.my_vc_mut().merge(&event.metadata.vc);
        }

        // event = self.correct_evict_inconsistencies(event);
        O::effect(event, &mut self.state);

        // Check if some operations are ready to be stabilized
        self.tc_stable();
    }

    /// The TCSB middleware can offer this causal stability information through extending its API with tcstablei(τ),
    /// which informs the upper layers that message with timestamp τ is now known to be causally stable
    pub fn tc_stable(&mut self) {
        let lower_bound = Metadata {
            vc: self.ltm.svv(&[]),
            wc: 0,
            origin: K::default(),
        };

        let ready_to_stabilize = self
            .state
            .1
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .map(|(m, _)| m.clone())
            .collect::<Vec<Metadata<K, C>>>();

        for metadata in ready_to_stabilize.iter() {
            O::stable(metadata, &mut self.state);
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
        self.id != event.metadata.origin
            && self
                .ltm
                .get(&event.metadata.origin)
                .map(|ltm_clock| event.metadata.vc <= *ltm_clock)
                .unwrap_or(false)
    }

    /// Check that the event is the causal successor of the last event delivered by this same replica
    fn guard_against_out_of_order(&self, event: &Event<K, C, O>) -> bool {
        self.id != event.metadata.origin && {
            let event_lamport_clock = event.metadata.vc.get(&event.metadata.origin).unwrap();
            let ltm_vc_clock = self.ltm.get(&event.metadata.origin);
            if let Some(ltm_vc_clock) = ltm_vc_clock {
                let ltm_lamport_lock = ltm_vc_clock.get(&event.metadata.origin).unwrap();
                return event_lamport_clock != ltm_lamport_lock + 1.into();
            }
            false
        }
    }

    /// Check that the event is not from an unknown peer
    fn guard_against_unknow_peer(&self, event: &Event<K, C, O>) -> bool {
        self.ltm.get(&event.metadata.origin).is_none()
    }
}
