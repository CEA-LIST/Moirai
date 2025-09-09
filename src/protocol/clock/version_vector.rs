use std::{cmp::Ordering, collections::HashMap, fmt::Display};

// #[cfg(feature = "utils")]
// use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::{
    protocol::{
        event::id::EventId,
        membership::{view::View, ReplicaId, ReplicaIdx},
    },
    utils::mut_owner::Reader,
};

/// Sequence number
pub type Seq = usize;

// #[cfg_attr(feature = "utils", derive(DeepSizeOf))]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct Version {
    entries: HashMap<ReplicaIdx, Seq>,
    origin_idx: ReplicaIdx,
    view: Reader<View>,
}

impl Version {
    pub fn new(view: &Reader<View>, origin_idx: ReplicaIdx) -> Self {
        let entries = view.borrow().members().map(|(idx, _)| (*idx, 0)).collect();
        Self {
            entries,
            view: Reader::clone(view),
            origin_idx,
        }
    }

    /// Increment the origin's entry.
    ///
    /// # Complexity
    /// Runs in `O(1)` time complexity
    pub fn increment(&mut self) -> usize {
        let seq = self
            .entries
            .entry(self.origin_idx())
            .and_modify(|v| *v += 1)
            .or_insert(1);
        *seq
    }

    /// Merge two clocks that share the same view.
    ///
    /// # Complexity
    /// Checks that the views are identical.
    /// Then runs in `O(n)` time complexity with `n` being the number of members in the view
    pub fn merge(&mut self, other: &Self) {
        // if `self` dominate `other`, then no need to merge.
        if EventId::from(other).is_predecessor_of(self) {
            return;
        }
        // `other` view because we merge its values in ours
        for (_, id) in other.view.borrow().members() {
            let self_seq = self.seq_by_id(id).unwrap_or(0);
            let other_seq = other.seq_by_id(id).unwrap_or(0);
            if self_seq < other_seq {
                self.set_by_id(id, other_seq);
            }
        }
    }

    pub(in crate::protocol::clock) fn seq_by_idx(&self, idx: ReplicaIdx) -> Option<Seq> {
        self.entries.get(&idx).cloned()
    }

    pub fn seq_by_id(&self, id: &ReplicaId) -> Option<Seq> {
        self.view
            .borrow()
            .get_idx(id)
            .and_then(|idx| self.entries.get(&idx).cloned())
    }

    pub fn sum(&self) -> usize {
        self.entries.values().sum()
    }

    #[cfg(test)]
    fn set_by_idx(&mut self, idx: ReplicaIdx, value: Seq) {
        self.entries.insert(idx, value);
    }

    pub fn set_by_id(&mut self, id: &ReplicaId, value: Seq) {
        if let Some(idx) = self.view.borrow().get_idx(id) {
            self.entries.insert(idx, value);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn origin_idx(&self) -> ReplicaIdx {
        self.origin_idx
    }

    pub fn origin_seq(&self) -> Seq {
        self.seq_by_idx(self.origin_idx).unwrap()
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = EventId> + 'a {
        self.entries
            .iter()
            .map(|(idx, seq)| EventId::new(*idx, *seq, self.view.clone()))
    }

    #[cfg(test)]
    pub(in crate::protocol::clock) fn build(
        view: &Reader<View>,
        origin_idx: ReplicaIdx,
        values: &[usize],
    ) -> Self {
        let mut v = Version::new(view, origin_idx);
        for (idx, val) in values.iter().enumerate() {
            v.set_by_idx(idx, *val);
        }
        v
    }
}

impl From<&Version> for EventId {
    fn from(version: &Version) -> Self {
        assert!(version.origin_seq() > 0);
        EventId::new(
            version.origin_idx(),
            version.origin_seq(),
            Reader::clone(&version.view),
        )
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{{ ")?;
        let mut first = true;
        let view = self.view.borrow();
        let mut sorted_members: Vec<_> = view.members().collect();
        sorted_members.sort_by(|(_, id1), (_, id2)| id1.cmp(id2));
        for (idx, m) in sorted_members {
            if let Some(val) = self.entries.get(idx) {
                if first {
                    write!(f, "{m}: {val}")?;
                    first = false;
                } else {
                    write!(f, ", {m}: {val}")?;
                }
            }
        }
        write!(f, " }}")?;
        write!(
            f,
            "@{}",
            self.view
                .borrow()
                .get_id(self.origin_idx)
                .unwrap_or(&"<unknown>".to_string())
        )?;
        Ok(())
    }
}

impl PartialOrd for Version {
    // TODO: add shortcut
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut less = false;
        let mut greater = false;

        for (_, id) in self.view.borrow().members() {
            let self_val = self.seq_by_id(id).unwrap_or(0);
            let other_val = other.seq_by_id(id).unwrap_or(0);

            match self_val.cmp(&other_val) {
                Ordering::Less => less = true,
                Ordering::Greater => greater = true,
                _ => (),
            }

            // If both less and greater are true, the clocks are concurrent
            if less && greater {
                return None;
            }
        }

        if less {
            Some(Ordering::Less)
        } else if greater {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Equal)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::mut_owner::MutOwner;

    use super::*;

    fn view() -> MutOwner<View> {
        let mut view = View::new(&"a".to_string());
        view.add(&"b".to_string());
        view.add(&"c".to_string());
        MutOwner::new(view)
    }

    fn different_views() -> (MutOwner<View>, MutOwner<View>) {
        let mut view_1 = View::new(&"a".to_string());
        view_1.add(&"b".to_string());
        view_1.add(&"c".to_string());
        let v1 = MutOwner::new(view_1);

        let mut view_2 = View::new(&"b".to_string());
        view_2.add(&"c".to_string());
        view_2.add(&"a".to_string());
        let v2 = MutOwner::new(view_2);

        (v1, v2)
    }

    #[test]
    fn concurrent_clock() {
        let view = view().as_reader();
        let mut v1 = Version::new(&view, 0);
        let mut v2 = Version::new(&view, 1);

        v1.increment();
        v2.increment();

        assert_eq!(v1.partial_cmp(&v2), None);
    }

    #[test]
    fn shortcut_clock() {
        let view = view().as_reader();
        let mut v1 = Version::new(&view, 0);
        let mut v2 = Version::new(&view, 1);

        v1.increment();
        v1.increment();

        v2.merge(&v1);

        assert_eq!(v1.entries, v2.entries);

        v2.increment();
        assert_eq!(v1.partial_cmp(&v2), Some(Ordering::Less));
    }

    #[test]
    fn same_clock() {
        let view = view().as_reader();
        let mut v1 = Version::new(&view, 0);
        let mut v2 = Version::new(&view, 0);

        v1.increment();
        v2.increment();

        assert_eq!(v1.partial_cmp(&v2), Some(Ordering::Equal));
    }

    #[test]
    fn clocks() {
        let (view_1, view_2) = different_views();
        let view_1 = view_1.as_reader();
        let view_2 = view_2.as_reader();

        let mut v1 = Version::new(&view_1, 0);
        let mut v2 = Version::new(&view_2, 0);

        v1.increment();
        v1.increment();
        v2.increment();
        v2.increment();
        v2.increment();

        assert_eq!(v1.seq_by_id(&"a".to_string()).unwrap(), 2);
        assert_eq!(v1.seq_by_id(&"b".to_string()).unwrap(), 0);
        assert_eq!(v1.seq_by_id(&"c".to_string()).unwrap(), 0);

        assert_eq!(v2.seq_by_id(&"a".to_string()).unwrap(), 0);
        assert_eq!(v2.seq_by_id(&"b".to_string()).unwrap(), 3);
        assert_eq!(v2.seq_by_id(&"c".to_string()).unwrap(), 0);

        v1.merge(&v2);
        assert_eq!(v1.seq_by_id(&"a".to_string()).unwrap(), 2);
        assert_eq!(v1.seq_by_id(&"b".to_string()).unwrap(), 3);
        assert_eq!(v1.seq_by_id(&"c".to_string()).unwrap(), 0);

        v2.merge(&v1);
        assert_eq!(v2.seq_by_id(&"a".to_string()).unwrap(), 2);
        assert_eq!(v2.seq_by_id(&"b".to_string()).unwrap(), 3);
        assert_eq!(v2.seq_by_id(&"c".to_string()).unwrap(), 0);
    }
}
