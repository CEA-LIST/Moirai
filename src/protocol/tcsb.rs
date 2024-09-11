use colored::*;
use log::{debug, info};
use radix_trie::TrieCommon;

use super::po_log::POLog;
use super::{event::Event, metadata::Metadata, pure_crdt::PureCRDT};
use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};
use crate::crdt::rw_set::RWSet;
use std::fmt::Debug;
use std::ops::Bound;
use std::path::PathBuf;

pub type RedundantRelation<O> = fn(&Event<O>, &Event<O>) -> bool;

/// A Tagged Causal Stable Broadcast (TCSB) is an extended Reliable Causal Broadcast (RCB)
/// middleware API designed to offer additional information about causality during message delivery.
/// It also notifies recipients when delivered messages achieve causal stability,
/// facilitating subsequent compaction within the Partially Ordered Log of operations (PO-Log)
pub struct Tcsb<O>
where
    O: PureCRDT + Debug,
{
    pub id: &'static str,
    pub state: POLog<O>,
    // Group Membership Service
    pub gms: POLog<RWSet<&'static str>>,
    /// Last Timestamp Matrix (LTM) is a matrix clock that keeps track of the vector clocks of all peers.
    pub ltm: MatrixClock<&'static str, usize>,
    /// Last Stable Vector (LSV)
    pub lsv: VectorClock<&'static str, usize>,
}

impl<O> Tcsb<O>
where
    O: PureCRDT + Debug,
{
    pub fn new(id: &'static str) -> Self {
        Self {
            id,
            state: POLog::default(),
            gms: POLog::default(),
            ltm: MatrixClock::new(&[id]),
            lsv: VectorClock::new(id),
        }
    }

    /// Broadcast a new operation to all peers and deliver it to the local state.
    pub fn tc_bcast(&mut self, op: O) -> Event<O> {
        let my_id = self.id;
        let my_vc = self.my_vc_mut();
        my_vc.increment(&my_id);
        let metadata = Metadata::new(my_vc.clone(), self.id);
        let event = Event::new(op, metadata);
        self.tc_deliver(event.clone());
        event
    }

    /// Deliver an event to the local state.
    pub fn tc_deliver(&mut self, event: Event<O>) {
        info!(
            "[{}] - Delivering event {} from {} with timestamp {}",
            self.id.blue().bold(),
            format!("{:?}", event.op).green(),
            event.metadata.origin.blue(),
            format!("{}", event.metadata.vc).red()
        );
        // Check if the event is valid
        if let Err(err) = self.guard(&event.metadata) {
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
        let (keep, stable, unstable) = O::effect(&event, &self.state);
        self.state.remove_redundant_ops(self.id, stable, unstable);

        if keep {
            self.state.new_event(&event);
            info!(
                "[{}] - Op {} is added to the log",
                self.id.blue().bold(),
                format!("{:?}", event.op).green()
            );
        }

        let trie_size = self.state.path_trie.values().flatten().count();
        let state_size = self.state.stable.len() + self.state.unstable.len();
        debug!(
            "[{}] - `path_trie`: {}/{} weak pointers waiting to be cleaned",
            self.id.blue().bold(),
            trie_size - state_size,
            trie_size,
        );

        self.state.garbage_collect_trie();

        // Check if some operations are ready to be stabilized
        self.tc_stable();
    }

    /// The TCSB middleware can offer this causal stability information through extending its API with tcstablei(τ),
    /// which informs the upper layers that message with timestamp τ is now known to be causally stable
    pub fn tc_stable(&mut self) {
        let lower_bound = Metadata::new(self.ltm.svv(&[]), "");
        let ready_to_stabilize = self
            .state
            .unstable
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .map(|(m, _)| m.clone())
            .collect::<Vec<Metadata>>();

        for metadata in ready_to_stabilize.iter() {
            info!(
                "[{}] - {} is causally stable (op: {})",
                self.id.blue().bold(),
                format!("{}", metadata.vc).red(),
                format!("{:?}", self.state.unstable.get(metadata).unwrap()).green()
            );
            O::stable(metadata, &mut self.state);
        }
    }

    // Testing functions for the GMS

    pub fn tc_bcast_gms(&mut self, op: RWSet<&'static str>) -> Event<RWSet<&'static str>> {
        let my_id = self.id;
        let my_vc = self.my_vc_mut();
        my_vc.increment(&my_id);
        let metadata = Metadata::new(my_vc.clone(), self.id);
        let event = Event::new(op, metadata);
        self.tc_deliver_gms(event.clone());
        event
    }

    pub fn tc_deliver_gms(&mut self, event: Event<RWSet<&'static str>>) {
        info!(
            "[{}][GMS] - Delivering event {} from {} with timestamp {}",
            self.id.blue().bold(),
            format!("{:?}", event.op).green(),
            event.metadata.origin.blue(),
            format!("{}", event.metadata.vc).red()
        );
        // Check if the event is valid
        if let Err(err) = self.guard(&event.metadata) {
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
        let (keep, stable, unstable) = RWSet::effect(&event, &self.gms);
        self.gms.remove_redundant_ops(self.id, stable, unstable);

        if keep {
            self.gms.new_event(&event);
            info!(
                "[{}] - Op {} is added to the log",
                self.id.blue().bold(),
                format!("{:?}", event.op).green()
            );
        }

        let trie_size = self.gms.path_trie.values().flatten().count();
        let state_size = self.gms.stable.len() + self.state.unstable.len();
        debug!(
            "[{}] - `path_trie`: {}/{} weak pointers waiting to be cleaned",
            self.id.blue().bold(),
            trie_size - state_size,
            trie_size,
        );

        self.gms.garbage_collect_trie();

        // Check if some operations are ready to be stabilized
        self.tc_stable();
    }

    pub fn tc_stable_gms(&mut self) {
        let ltm_members = self.ltm.keys();
        let gms_members = RWSet::eval(&self.gms, &PathBuf::default())
            .into_iter()
            .collect::<Vec<_>>();
        // keep only the gms members that are also in the ltm
        let gms_members_filtered = gms_members
            .into_iter()
            .filter(|m| ltm_members.contains(m))
            .collect::<Vec<_>>();
        // keep the members that are in the ltm but not in the gms
        let ignore = ltm_members
            .into_iter()
            .filter(|m| !gms_members_filtered.contains(m))
            .collect::<Vec<_>>();

        let lower_bound = Metadata::new(self.ltm.svv(&ignore), "");

        let ready_to_stabilize = self
            .gms
            .unstable
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .map(|(m, _)| m.clone())
            .collect::<Vec<Metadata>>();

        for metadata in ready_to_stabilize.iter() {
            info!(
                "[{}] - {} is causally stable (op: {})",
                self.id.blue().bold(),
                format!("{}", metadata.vc).red(),
                format!("{:?}", self.gms.unstable.get(metadata).unwrap()).green()
            );
            RWSet::stable(metadata, &mut self.gms);
        }
    }

    /// Utilitary function to evaluate the current state of the whole CRDT.
    pub fn eval(&self) -> O::Value {
        O::eval(&self.state, &PathBuf::default())
    }

    /// Return the vector clock of the local replica
    pub(crate) fn my_vc(&self) -> &VectorClock<&'static str, usize> {
        self.ltm
            .get(&self.id)
            .expect("Local vector clock not found")
    }

    /// Return the mutable vector clock of the local replica
    pub(crate) fn my_vc_mut(&mut self) -> &mut VectorClock<&'static str, usize> {
        self.ltm
            .get_mut(&self.id)
            .expect("Local vector clock not found")
    }

    fn guard(&self, metadata: &Metadata) -> Result<(), &str> {
        if self.guard_against_unknow_peer(metadata) {
            return Err("Unknown peer detected");
        }
        if self.guard_against_duplicates(metadata) {
            return Err("Duplicated event detected");
        }
        if self.guard_against_out_of_order(metadata) {
            return Err("Out-of-order event detected");
        }
        Ok(())
    }

    /// Check that the event has not already been delivered
    fn guard_against_duplicates(&self, metadata: &Metadata) -> bool {
        self.id != metadata.origin
            && self
                .ltm
                .get(&metadata.origin)
                .map(|ltm_clock| metadata.vc <= *ltm_clock)
                .unwrap_or(false)
    }

    /// Check that the event is the causal successor of the last event delivered by this same replica
    fn guard_against_out_of_order(&self, metadata: &Metadata) -> bool {
        self.id != metadata.origin && {
            let event_lamport_clock = metadata.vc.get(&metadata.origin).unwrap();
            let ltm_vc_clock = self.ltm.get(&metadata.origin);
            if let Some(ltm_vc_clock) = ltm_vc_clock {
                let ltm_lamport_lock = ltm_vc_clock.get(&metadata.origin).unwrap();
                return event_lamport_clock != ltm_lamport_lock + 1;
            }
            false
        }
    }

    /// Check that the event is not from an unknown peer
    fn guard_against_unknow_peer(&self, metadata: &Metadata) -> bool {
        self.ltm.get(&metadata.origin).is_none()
    }
}
