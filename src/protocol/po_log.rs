use std::collections::btree_map::Values;
use std::iter::Chain;
use std::path::PathBuf;
use std::slice::Iter;
use std::{
    collections::BTreeMap,
    rc::{Rc, Weak},
};

use radix_trie::Trie;

use super::event::Event;
use super::{metadata::Metadata, pure_crdt::PureCRDT};

pub type PathTrie<O> = Trie<PathBuf, Vec<Weak<O>>>;
pub type Log<O> = BTreeMap<Metadata, Rc<O>>;

/// A Partially Ordered Log (PO-Log), is a chronological record that
/// preserves all executed operations alongside their respective timestamps.
/// In actual implementations, the PO-Log can be split in two components:
/// one that simply stores the set of stable operations and the other stores the timestamped operations.
#[derive(Debug)]
pub struct POLog<O>
where
    O: PureCRDT,
{
    pub stable: Vec<Rc<O>>,
    pub unstable: Log<O>,
    pub path_trie: PathTrie<O>,
}

impl<O> POLog<O>
where
    O: PureCRDT,
{
    pub fn new() -> Self {
        Self {
            stable: vec![],
            unstable: BTreeMap::new(),
            path_trie: Trie::new(),
        }
    }

    pub fn new_event(&mut self, event: &Event<O>) {
        let rc_op = Rc::new(event.op.clone());
        let weak_op = Rc::downgrade(&rc_op);
        self.unstable.insert(event.metadata.clone(), rc_op);
        if let Some(subtrie) = self.path_trie.get_mut(&O::to_path(&event.op)) {
            subtrie.push(weak_op);
        } else {
            self.path_trie.insert(O::to_path(&event.op), vec![weak_op]);
        }
    }

    pub fn new_stable(&mut self, op: Rc<O>) {
        self.stable.push(op);
    }

    pub fn iter(&self) -> Chain<Iter<Rc<O>>, Values<Metadata, Rc<O>>> {
        self.stable.iter().chain(self.unstable.values())
    }

    pub fn is_empty(&self) -> bool {
        self.stable.is_empty() && self.unstable.is_empty()
    }
}

impl<O> Default for POLog<O>
where
    O: PureCRDT,
{
    fn default() -> Self {
        Self::new()
    }
}
