use colored::*;
use log::{debug, error, info};
use radix_trie::TrieCommon;

use super::po_log::POLog;
use super::{event::Event, metadata::Metadata, pure_crdt::PureCRDT};
use crate::clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock};
use crate::crdt::duet::Duet;
use crate::crdt::membership_set::MSet;
use std::fmt::Debug;
use std::ops::Bound;
use std::path::PathBuf;

pub type RedundantRelation<O> = fn(&Event<O>, &Event<O>) -> bool;

/// Extended Reliable Causal Broadcast (RCB) middleware API
///
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
    pub group_membership: POLog<MSet<&'static str>>,
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
        let mut group_membership = POLog::default();
        let event = Event::new(MSet::Add(id), Metadata::default());
        group_membership.new_event(&event);
        Self {
            id,
            state: POLog::default(),
            group_membership,
            ltm: MatrixClock::new(&[id]),
            lsv: VectorClock::new(id),
        }
    }

    /// Broadcast a new operation to all peers and deliver it to the local state.
    pub fn tc_bcast_op(&mut self, op: O) -> Event<O> {
        let metadata = self.generate_metadata_for_new_event();
        let event = Event::new(Duet::Second(op.clone()), metadata.clone());
        self.tc_deliver(event.clone());
        Event::new(op, metadata)
    }

    /// Broadcast a new operation to all peers and deliver it to the local state.
    pub fn tc_bcast_membership(&mut self, op: MSet<&'static str>) -> Event<MSet<&'static str>> {
        let metadata = self.generate_metadata_for_new_event();
        let event = Event::new(Duet::First(op.clone()), metadata.clone());
        self.tc_deliver(event.clone());
        Event::new(op, metadata)
    }

    pub fn tc_deliver_op(&mut self, event: Event<O>) {
        let event = Event::new(Duet::Second(event.op.clone()), event.metadata.clone());
        self.tc_deliver(event);
    }

    pub fn tc_deliver_membership(&mut self, event: Event<MSet<&'static str>>) {
        let event = Event::new(Duet::First(event.op.clone()), event.metadata.clone());
        self.tc_deliver(event);
    }

    /// Deliver an event to the local state.
    fn tc_deliver(&mut self, mut event: Event<Duet<MSet<&'static str>, O>>) {
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
        // Check for timestamp inconsistencies
        if matches!(event.op, Duet::First(_))
            && event.metadata.vc.keys().len()
                != MSet::eval(&self.group_membership, &PathBuf::default()).len()
        {
            debug!(
                "[{}] - Timestamp inconsistency detected, fixing...",
                self.id.blue().bold(),
            );
            Self::fix_timestamp_inconsistencies(&mut event, &self.ltm.keys());
        }
        // If the event is not from the local replica
        if self.id != event.metadata.origin {
            // Update the vector clock of the sender in the LTM
            // Increment the new peer vector clock with its actual value
            self.ltm.update(&event.metadata.origin, &event.metadata.vc);
            // Update our own vector clock
            self.my_vc_mut().merge(&event.metadata.vc);
        }

        match event.op {
            Duet::First(op) => {
                let event = Event::new(op, event.metadata);
                let (keep, stable, unstable) = MSet::effect(&event, &self.group_membership);
                self.group_membership
                    .remove_redundant_ops(self.id, stable, unstable);

                if keep {
                    self.group_membership.new_event(&event);
                    info!(
                        "[{}] - Op {} is added to the log",
                        self.id.blue().bold(),
                        format!("{:?}", event.op).green()
                    );
                }

                let trie_size = self.group_membership.path_trie.values().flatten().count();
                let state_size = self.group_membership.stable.len() + self.state.unstable.len();
                debug!(
                    "[{}] - `path_trie`: {}/{} weak pointers waiting to be cleaned",
                    self.id.blue().bold(),
                    trie_size - state_size,
                    trie_size,
                );

                self.group_membership.garbage_collect_trie();
            }
            Duet::Second(op) => {
                let event = Event::new(op, event.metadata);
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
            }
        }

        // Check if some operations are ready to be stabilized
        self.tc_stable();
    }

    /// The TCSB middleware can offer this causal stability information through extending its API with tcstablei(τ),
    /// which informs the upper layers that message with timestamp τ is now known to be causally stable
    fn tc_stable(&mut self) {
        let ignore = self.peers_to_ignore_for_stability();
        let lower_bound = Metadata::new(self.ltm.svv(&ignore), "");
        let ready_to_stabilize = self.collect_stabilizable_events(&lower_bound);

        for metadata in ready_to_stabilize.iter() {
            info!(
                "[{}] - {} is causally stable (op: {})",
                self.id.blue().bold(),
                format!("{}", metadata.vc).red(),
                format!("{:?}", self.state.unstable.get(metadata).unwrap()).green()
            );
            if self.state.unstable.contains_key(metadata) {
                O::stable(metadata, &mut self.state);
            } else if self.group_membership.unstable.contains_key(metadata) {
                MSet::stable(metadata, &mut self.group_membership);
            }
        }

        self.update_ltm_membership();
    }

    /// Utilitary function to evaluate the current state of the whole CRDT.
    pub fn eval(&self) -> O::Value {
        O::eval(&self.state, &PathBuf::default())
    }

    /// Return the mutable vector clock of the local replica
    pub(crate) fn my_vc_mut(&mut self) -> &mut VectorClock<&'static str, usize> {
        self.ltm
            .get_mut(&self.id)
            .expect("Local vector clock not found")
    }

    fn guard(&self, metadata: &Metadata) -> Result<(), &str> {
        if self.guard_against_unknow_peer(metadata) {
            error!(
                "[{}] - Unknown peer detected: {}",
                self.id.blue().bold(),
                metadata.origin.red()
            );
            return Err("Unknown peer detected");
        }
        if self.guard_against_duplicates(metadata) {
            error!(
                "[{}] - Duplicated event detected: {}",
                self.id.blue().bold(),
                format!("{}", metadata.vc).red()
            );
            return Err("Duplicated event detected");
        }
        if self.guard_against_out_of_order(metadata) {
            error!(
                "[{}] - Out-of-order event detected: {}",
                self.id.blue().bold(),
                format!("{}", metadata.vc).red()
            );
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

    /// Returns the update clock new event of this [`Tcsb<O>`].
    fn generate_metadata_for_new_event(&mut self) -> Metadata {
        let my_id = self.id;
        let my_vc = self.my_vc_mut();
        my_vc.increment(&my_id);
        Metadata::new(my_vc.clone(), self.id)
    }

    /// Correct the inconsistencies in the vector clocks of two events
    /// by adding the missing keys and setting the missing values to 0 or usize::MAX
    /// Timestamp inconsistencies can occur when a peer has stablized a membership event before the other peers.
    fn fix_timestamp_inconsistencies(
        new: &mut Event<Duet<MSet<&str>, O>>,
        ltm_keys: &[&'static str],
    ) {
        let op = match &new.op {
            Duet::First(op) => op,
            Duet::Second(_) => return,
        };
        for key in ltm_keys.iter() {
            if !new.metadata.vc.contains(key) {
                let value = match op {
                    MSet::Add(_) => 0,
                    MSet::Remove(_) => usize::MAX,
                };
                new.metadata.vc.insert(key, value);
            }
        }
        for key in new.metadata.vc.keys() {
            if !ltm_keys.contains(&key) {
                new.metadata.vc.remove(&key);
            }
        }
    }

    /// Returns a subset of peers that can be safely ignored when checking for causal stability.
    fn peers_to_ignore_for_stability(&self) -> Vec<&'static str> {
        let ignore: Vec<&'static str> = self
            .group_membership
            .unstable
            .iter()
            .filter_map(|(_, o)| match o.as_ref() {
                MSet::Add(_) => None,
                MSet::Remove(v) => Some(*v),
            })
            .collect();
        let ignore = if ignore.contains(&self.id) {
            self.ltm
                .keys()
                .iter()
                .filter(|k| **k != self.id)
                .copied()
                .collect()
        } else {
            ignore
        };
        ignore
    }

    /// Returns a list of operations that are ready to be stabilized.
    fn collect_stabilizable_events(&self, lower_bound: &Metadata) -> Vec<Metadata> {
        let mut state = self
            .state
            .unstable
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .map(|(m, _)| m.clone())
            .collect::<Vec<Metadata>>();
        let group_membership = self
            .group_membership
            .unstable
            .range((Bound::Unbounded, Bound::Included(lower_bound)))
            .map(|(m, _)| m.clone())
            .collect::<Vec<Metadata>>();
        state.extend(group_membership);
        state.sort();
        state
    }

    /// Synchronize the Last Timestamp Matrix (LTM) with the latest group membership information.
    fn update_ltm_membership(&mut self) {
        let gms_members = MSet::eval(&self.group_membership, &PathBuf::default())
            .into_iter()
            .collect::<Vec<_>>();
        for member in &gms_members {
            if self.ltm.get(member).is_none() {
                self.ltm.add_key(member);
            }
        }
        for member in self.ltm.keys() {
            if !gms_members.contains(&member) {
                if member != self.id {
                    self.ltm.remove_key(&member);
                } else {
                    for key in self.ltm.keys() {
                        if key != self.id {
                            self.ltm.remove_key(&key);
                        }
                    }
                }
            }
        }
    }
}
