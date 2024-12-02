use super::{
    pathbuf_key::PathBufKey,
    po_log::{Log, PathTrie},
    pure_crdt::PureCRDT,
    tcsb::{Converging, Tcsb, TimestampExtension},
};
use crate::{
    clocks::{matrix_clock::MatrixClock, vector_clock::VectorClock},
    crdt::membership_set::MSet,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
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
    pub timestamp_extension: TimestampExtension,
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
            timestamp_extension: tcsb.timestamp_extension.clone(),
            removed_members: tcsb.removed_members.clone(),
        }
    }
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
        self.timestamp_extension = state.timestamp_extension;

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
}
