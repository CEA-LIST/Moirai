use crate::{
    clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock},
    crdt::{duet::Duet, membership_set::MSet},
    protocol::{event::Event, metadata::Metadata, tcsb::AnyOp},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::{
    pathbuf_key::PathBufKey,
    po_log::{Log, PathTrie},
    pure_crdt::PureCRDT,
    tcsb::{Converging, Tcsb},
};
use std::{collections::HashSet, fmt::Debug, ops::Bound, rc::Rc};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StateTransfer<O>
where
    O: PureCRDT,
{
    group_membership_stable: Vec<Rc<MSet<String>>>,
    group_membership_unstable: Log<MSet<String>>,
    log_stable: Vec<Rc<O>>,
    log_unstable: Log<O>,
    lsv: VectorClock<String, usize>,
    ltm: MatrixClock<String, usize>,
    converging_members: Converging,
    evicted: HashSet<String>,
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
            evicted: tcsb.evicted.clone(),
        }
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Batch<O>
where
    O: PureCRDT,
{
    events: Vec<Event<AnyOp<O>>>,
    metadata: Metadata,
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

    pub fn events_since(
        &self,
        lsv: &VectorClock<String, usize>,
        since: &VectorClock<String, usize>,
    ) -> Vec<Event<AnyOp<O>>> {
        assert!(
            lsv <= since || lsv.partial_cmp(since).is_none(),
            "LSV should be inferior, equal or even concurrent to the since clock. LSV: {lsv}, Since clock: {since}",
        );
        let mut metadata_lsv = Metadata::new(lsv.clone(), "");
        let mut metadata_since = Metadata::new(since.clone(), "");

        if self.eval_group_membership() != HashSet::from_iter(metadata_lsv.clock.keys()) {
            self.fix_timestamp_inconsistencies_incoming_event(&mut metadata_lsv);
        }

        if self.eval_group_membership() != HashSet::from_iter(metadata_since.clock.keys()) {
            self.fix_timestamp_inconsistencies_incoming_event(&mut metadata_since);
        }

        assert_eq!(
            self.ltm_current_keys(),
            metadata_since.clock.keys(),
            "Since: {:?}",
            metadata_since.clock
        );
        assert_eq!(
            self.ltm_current_keys(),
            metadata_lsv.clock.keys(),
            "LSV: {:?}",
            metadata_lsv.clock
        );
        // If the LSV is strictly greater than the since vector clock, it means the peer needs a state transfer
        // However, it should not happen because every peer should wait that everyone gets the ops before stabilizing

        // TODO: Rather than just `since`, the requesting peer should precise if it has received other events in its pending buffer.
        let events: Vec<Event<AnyOp<O>>> = self
            .group_membership
            .unstable
            .range((Bound::Excluded(&metadata_lsv), Bound::Unbounded))
            .filter_map(|(m, o)| {
                // If the dot is greater than the one in the since vector clock, then we have not delivered the event
                if m.clock.get(&m.origin).unwrap() > metadata_since.clock.get(&m.origin).unwrap() {
                    Some(Event::new(Duet::First(o.as_ref().clone()), m.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let domain_events: Vec<Event<AnyOp<O>>> = self
            .state
            .unstable
            .range((Bound::Excluded(&metadata_lsv), Bound::Unbounded))
            .filter_map(|(m, o)| {
                // If the dot is greater than the one in the since vector clock, then we have not delivered the event
                if m.clock.get(&m.origin).unwrap() > metadata_since.clock.get(&m.origin).unwrap() {
                    Some(Event::new(Duet::Second(o.as_ref().clone()), m.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        [events, domain_events].concat()
    }

    pub fn deliver_batch(&mut self, events: Vec<Event<AnyOp<O>>>) {
        for event in events {
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
    }
}
