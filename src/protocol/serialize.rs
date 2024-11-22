use crate::{
    clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock},
    crdt::{duet::Duet, membership_set::MSet},
    protocol::{event::Event, metadata::Metadata, tcsb::AnyOp},
};
use log::error;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::{
    pathbuf_key::PathBufKey,
    po_log::{Log, PathTrie},
    pure_crdt::PureCRDT,
    tcsb::{Converging, Tcsb},
};
use std::{collections::HashSet, fmt::Debug, rc::Rc};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StateTransfer<O>
where
    O: PureCRDT,
{
    pub group_membership_stable: Vec<Rc<MSet<String>>>,
    pub group_membership_unstable: Log<MSet<String>>,
    pub log_stable: Vec<Rc<O>>,
    pub log_unstable: Log<O>,
    pub lsv: VectorClock<String, usize>,
    pub ltm: MatrixClock<String, usize>,
    pub converging_members: Converging,
    pub removed_members: HashSet<String>,
}

impl<O> StateTransfer<O>
where
    O: PureCRDT,
{
    pub fn new(tcsb: &Tcsb<O>, to: &String) -> Self {
        assert!(&tcsb.id != to && tcsb.eval_group_membership().contains(to));
        StateTransfer {
            group_membership_stable: tcsb.group_membership.stable.clone(),
            group_membership_unstable: tcsb.group_membership.unstable.clone(),
            log_stable: tcsb.state.stable.clone(),
            log_unstable: tcsb.state.unstable.clone(),
            lsv: tcsb.lsv.clone(),
            ltm: tcsb.ltm.clone(),
            converging_members: tcsb.converging_members.clone(),
            removed_members: tcsb.removed_members.clone(),
        }
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Batch<O>
where
    O: PureCRDT,
{
    pub events: Vec<Event<AnyOp<O>>>,
    pub metadata: Metadata,
}

impl<O> Batch<O>
where
    O: PureCRDT,
{
    pub fn new(events: Vec<Event<AnyOp<O>>>, metadata: Metadata) -> Self {
        Self { events, metadata }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DeliveryError {
    UnknownPeer,
    DuplicatedEvent,
    OutOfOrderEvent,
    EvictedPeer,
}

impl<O> Tcsb<O>
where
    O: PureCRDT + Debug,
{
    pub fn deliver_state(&mut self, state: StateTransfer<O>) {
        self.lsv = state.lsv;
        self.ltm = state.ltm;
        self.ltm.most_update(&self.id);
        self.state.stable = state.log_stable;
        self.state.unstable = state.log_unstable;
        self.group_membership.stable = state.group_membership_stable;
        self.group_membership.unstable = state.group_membership_unstable;
        self.converging_members = state.converging_members;

        self.state.path_trie = PathTrie::new();

        for rc_op in &self.state.stable {
            let weak_op = Rc::downgrade(rc_op);
            if let Some(subtrie) = self
                .state
                .path_trie
                .get_mut(&PathBufKey::new(&O::to_path(rc_op.as_ref())))
            {
                subtrie.push(weak_op);
            } else {
                self.state
                    .path_trie
                    .insert(PathBufKey::new(&O::to_path(rc_op.as_ref())), vec![weak_op]);
            }
        }

        for (_, rc_op) in self.state.unstable.iter() {
            let weak_op = Rc::downgrade(rc_op);
            if let Some(subtrie) = self
                .state
                .path_trie
                .get_mut(&PathBufKey::new(&O::to_path(rc_op.as_ref())))
            {
                subtrie.push(weak_op);
            } else {
                self.state
                    .path_trie
                    .insert(PathBufKey::new(&O::to_path(rc_op.as_ref())), vec![weak_op]);
            }
        }

        self.group_membership.path_trie = PathTrie::new();

        for rc_op in &self.group_membership.stable {
            let weak_op = Rc::downgrade(rc_op);
            if let Some(subtrie) = self
                .group_membership
                .path_trie
                .get_mut(&PathBufKey::new(&MSet::to_path(rc_op.as_ref())))
            {
                subtrie.push(weak_op);
            } else {
                self.group_membership.path_trie.insert(
                    PathBufKey::new(&MSet::to_path(rc_op.as_ref())),
                    vec![weak_op],
                );
            }
        }

        for (_, rc_op) in self.group_membership.unstable.iter() {
            let weak_op = Rc::downgrade(rc_op);
            if let Some(subtrie) = self
                .group_membership
                .path_trie
                .get_mut(&PathBufKey::new(&MSet::to_path(rc_op.as_ref())))
            {
                subtrie.push(weak_op);
            } else {
                self.group_membership.path_trie.insert(
                    PathBufKey::new(&MSet::to_path(rc_op.as_ref())),
                    vec![weak_op],
                );
            }
        }
    }

    pub fn events_since(&self, metadata: &Metadata) -> Result<Batch<O>, DeliveryError> {
        if !self.eval_group_membership().contains(&metadata.origin) {
            error!(
                "The origin {} of the metadata is not part of the group membership: {:?}",
                metadata.origin,
                self.eval_group_membership()
            );
            if self.removed_members.contains(&metadata.origin) {
                return Err(DeliveryError::EvictedPeer);
            } else {
                return Err(DeliveryError::UnknownPeer);
            }
        }

        let since = Metadata::new(metadata.clock.clone(), "");

        // TODO: Rather than just `since`, the requesting peer should precise if it has received other events in its pending buffer.
        let gms_events: Vec<Event<AnyOp<O>>> = self
            .group_membership
            .unstable
            .iter()
            .filter_map(|(m, o)| {
                // If the dot is greater than the one in the since vector clock, then we have not delivered the event
                if m.clock.get(&m.origin).unwrap() > since.clock.get(&m.origin).unwrap() {
                    Some(Event::new(Duet::First(o.as_ref().clone()), m.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let domain_events: Vec<Event<AnyOp<O>>> = self
            .state
            .unstable
            .iter()
            .filter_map(|(m, o)| {
                // If the dot is greater than the one in the since vector clock, then we have not delivered the event
                if m.clock.get(&m.origin).unwrap() > since.clock.get(&m.origin).unwrap() {
                    Some(Event::new(Duet::Second(o.as_ref().clone()), m.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let events = [gms_events, domain_events].concat();
        Ok(Batch::new(
            events,
            Metadata::new(self.my_clock().clone(), &self.id),
        ))
    }

    pub fn deliver_batch(&mut self, batch: Result<Batch<O>, DeliveryError>) {
        match batch {
            Ok(mut batch) => {
                for event in batch.events {
                    match event.op {
                        Duet::First(op) => {
                            let event = Event::new(op, event.metadata);
                            self.tc_deliver_membership(event);
                        }
                        Duet::Second(op) => {
                            let event = Event::new(op, event.metadata);
                            self.tc_deliver_op(event);
                        }
                    }
                }
                // We may have delivered a `remove(self)` event.
                if self
                    .eval_group_membership()
                    .contains(&batch.metadata.origin)
                {
                    self.fix_timestamp_inconsistencies_incoming_event(&mut batch.metadata);
                    assert_eq!(self.ltm.keys(), batch.metadata.clock.keys());
                    self.ltm
                        .update(&batch.metadata.origin, &batch.metadata.clock);
                    self.tc_stable();
                }
            }
            Err(e) => {
                match e {
                    DeliveryError::EvictedPeer => {
                        for key in self.ltm.keys() {
                            if key != self.id {
                                self.ltm.remove_key(&key);
                            }
                        }
                        // Re-init the group membership
                        self.group_membership = Self::create_group_membership(&self.id);
                        self.removed_members.clear();
                        self.converging_members.clear();
                    }
                    DeliveryError::UnknownPeer => {}
                    _ => {
                        panic!("Unexpected error: {:?}", e);
                    }
                }
            }
        }
    }
}
