use super::event::Event;
use super::pathbuf_key::PathBufKey;
use super::{metadata::Metadata, pure_crdt::PureCRDT};
use colored::Colorize;
use log::info;
use radix_trie::Trie;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::collections::btree_map::{Values, ValuesMut};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::iter::Chain;
use std::rc::{Rc, Weak};
use std::slice::{Iter, IterMut};

pub type PathTrie<O> = Trie<PathBufKey, Vec<Weak<O>>>;
pub type Log<O> = BTreeMap<Metadata, Rc<O>>;

/// # Causal DAG operation history
///
/// A Partially Ordered Log (PO-Log), is a chronological record that
/// preserves all executed operations alongside their respective timestamps.
/// In actual implementations, the PO-Log can be split in two components:
/// one that simply stores the set of stable operations and the other stores the timestamped operations.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct POLog<O>
where
    O: PureCRDT + Debug,
{
    pub stable: Vec<Rc<O>>,
    pub unstable: Log<O>,
    pub path_trie: PathTrie<O>,
}

impl<O> POLog<O>
where
    O: PureCRDT + Debug,
{
    pub fn new() -> Self {
        Self {
            stable: vec![],
            unstable: BTreeMap::new(),
            path_trie: Trie::new(),
        }
    }

    pub fn new_event(&mut self, event: &Event<O>) {
        let state_len_before = self.stable.len() + self.unstable.len();
        let rc_op = Rc::new(event.op.clone());
        let weak_op = Rc::downgrade(&rc_op);
        if self.unstable.contains_key(&event.metadata) {
            info!(
                "Event with metadata {:?} already present in the log: {:?}",
                event.metadata,
                self.unstable.get(&event.metadata).unwrap()
            );
        }
        let is_key_present = self.unstable.insert(event.metadata.clone(), rc_op);
        assert!(
            is_key_present.is_none(),
            "Key already present in the log with value {:?}",
            self.unstable.get(&event.metadata).unwrap()
        );
        let state_len_after = self.stable.len() + self.unstable.len();

        if let Some(subtrie) = self
            .path_trie
            .get_mut(&PathBufKey::new(&O::to_path(&event.op)))
        {
            subtrie.push(weak_op);
        } else {
            self.path_trie
                .insert(PathBufKey::new(&O::to_path(&event.op)), vec![weak_op]);
        }

        let path_trie_count = radix_trie::TrieCommon::values(&self.path_trie)
            .flatten()
            .count();
        assert!(path_trie_count >= state_len_after);
        assert_eq!(state_len_after, state_len_before + 1);
    }

    /// Garbage collect dead weak references from the path trie.
    pub fn garbage_collect_trie(&mut self) {
        let keys = radix_trie::TrieCommon::keys(&self.path_trie)
            .cloned()
            .collect::<Vec<PathBufKey>>();
        for key in keys {
            self.garbage_collect_trie_by_path(&key);
        }
    }

    /// Garbage collect dead weak references from the path trie, given a path.
    pub fn garbage_collect_trie_by_path(&mut self, path: &PathBufKey) {
        if let Some(subtrie) = self.path_trie.get_mut(path) {
            subtrie.retain(|weak_op| weak_op.upgrade().is_some());
            if subtrie.is_empty() {
                self.path_trie.remove(path);
            }
        }
    }

    /// Clean up the state by removing redundant operations
    pub fn remove_redundant_ops(&mut self, id: &str, stable: Vec<usize>, unstable: Vec<Metadata>) {
        for (i, val) in stable.iter().enumerate() {
            let removed = self.stable.remove(val - i);
            info!(
                "[{}] - Op {} is redundant",
                id.blue().bold(),
                format!("{:?}", removed.as_ref()).green()
            );
        }
        for m in unstable {
            let opt_removed = self.unstable.remove(&m);
            if let Some(removed) = opt_removed {
                info!(
                    "[{}] - Op {} is redundant",
                    id.blue().bold(),
                    format!("{:?}", removed.as_ref()).green()
                );
            }
        }
    }

    /// Should only be used in `eval()`
    pub fn new_stable(&mut self, op: Rc<O>) {
        self.stable.push(op);
    }

    pub fn iter(&self) -> Chain<Iter<Rc<O>>, Values<Metadata, Rc<O>>> {
        self.stable.iter().chain(self.unstable.values())
    }

    pub fn iter_mut(&mut self) -> Chain<IterMut<Rc<O>>, ValuesMut<Metadata, Rc<O>>> {
        self.stable.iter_mut().chain(self.unstable.values_mut())
    }

    pub fn is_empty(&self) -> bool {
        self.stable.is_empty() && self.unstable.is_empty()
    }
}

impl<O> Default for POLog<O>
where
    O: PureCRDT + Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<O> Display for POLog<O>
where
    O: PureCRDT + Debug + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Stable: [")?;
        for (i, op) in self.stable.iter().enumerate() {
            if i == self.stable.len() - 1 {
                write!(f, "{}", op)?;
            } else {
                write!(f, "{}, ", op)?;
            }
        }
        write!(f, "]\nUnstable: [")?;
        for (i, (m, op)) in self.unstable.iter().enumerate() {
            if i == self.unstable.len() - 1 {
                write!(f, "{}: {}", m, op)?;
            } else {
                write!(f, "{}: {}, ", m, op)?;
            }
        }
        write!(f, "]")
    }
}
