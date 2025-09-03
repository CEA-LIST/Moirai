use std::collections::HashMap;

// #[cfg(feature = "utils")]
// use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::{
    protocol::{
        event::id::EventId,
        membership::{ReplicaId, ReplicaIdx, View},
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
        Self {
            entries: HashMap::new(),
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

    pub fn seq_by_idx(&self, idx: ReplicaIdx) -> Option<Seq> {
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

    pub fn set_by_idx(&mut self, idx: ReplicaIdx, value: Seq) {
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

    pub fn origin_idx(&self) -> ReplicaIdx {
        self.origin_idx
    }

    pub fn origin_seq(&self) -> Seq {
        self.seq_by_idx(self.origin_idx).unwrap()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ReplicaIdx, &Seq)> {
        self.entries.iter()
    }

    // pub fn origin(&self) -> EventId {
    //     EventId::new(
    //         self.origin,
    //         self.get_by_idx(self.origin).unwrap_or(0),
    //         Rc::clone(&self.view),
    //     )
    // }

    // pub fn iter(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
    //     self.clock.iter().map(|(&k, &v)| (k, v))
    // }

    // pub fn get_by_idx(&self, idx: usize) -> Option<usize> {
    //     self.clock.get(&idx).cloned()
    // }

    // pub fn set_by_idx(&mut self, idx: usize, value: usize) {
    //     self.clock.insert(idx, value);
    // }

    // pub fn view(&self) -> &View {
    //     &self.view
    // }

    // pub fn origin_idx(&self) -> usize {
    //     self.origin
    // }

    // pub fn sum(&self) -> usize {
    //     self.clock.values().sum()
    // }

    // pub fn len(&self) -> usize {
    //     self.clock.len()
    // }

    // pub(crate) fn build(view: &Rc<View>, origin: usize, values: &[usize]) -> Version {
    //     Version {
    //         clock: values.iter().enumerate().map(|(i, &v)| (i, v)).collect(),
    //         origin,
    //         view: Rc::clone(view),
    //     }
    // }
}

impl From<&Version> for EventId {
    fn from(version: &Version) -> Self {
        EventId::new(
            version.origin_idx(),
            version.origin_seq(),
            Reader::clone(&version.view),
        )
    }
}

// impl Display for Version {
//     fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
//         write!(f, "{{ ")?;
//         let mut first = true;
//         for (idx, m) in self.view.members().iter().enumerate() {
//             if let Some(val) = self.clock.get(&idx) {
//                 if first {
//                     write!(f, "{m}: {val}")?;
//                     first = false;
//                 } else {
//                     write!(f, ", {m}: {val}")?;
//                 }
//             }
//         }
//         write!(f, " }}")?;
//         write!(f, "@{}", self.view.local_replica_id())?;
//         Ok(())
//     }
// }

// impl PartialOrd for Version {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         match self.view.id.cmp(&other.view.id) {
//             Ordering::Less => return Some(Ordering::Less),
//             Ordering::Greater => return Some(Ordering::Greater),
//             _ => {}
//         };

//         if self.clock.keys().collect::<HashSet<_>>() != other.clock.keys().collect::<HashSet<_>>() {
//             panic!("Clocks must behave like vector clocks to be comparable.");
//         }

//         if self.get(self.origin()) == other.get(other.origin()) && self.origin == other.origin {
//             return Some(Ordering::Equal);
//         }

//         if self.get(self.origin()) <= other.get(self.origin()) {
//             return Some(Ordering::Less);
//         }

//         if other.get(other.origin()) <= self.get(other.origin()) {
//             return Some(Ordering::Greater);
//         }

//         let mut less = false;
//         let mut greater = false;

//         for m in self.view.members().iter() {
//             let self_val = self.get(m).unwrap();
//             let other_val = other.get(m).unwrap();

//             match self_val.cmp(&other_val) {
//                 Ordering::Less => less = true,
//                 Ordering::Greater => greater = true,
//                 _ => (),
//             }

//             // If both less and greater are true, the clocks are concurrent
//             if less && greater {
//                 return None;
//             }
//         }

//         if less {
//             Some(Ordering::Less)
//         } else if greater {
//             Some(Ordering::Greater)
//         } else {
//             Some(Ordering::Equal)
//         }
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::protocol::membership::{View, ViewStatus};

//     #[test_log::test]
//     fn concurrent_clock() {
//         let view = View::new(
//             0,
//             vec!["a".to_string(), "b".to_string(), "c".to_string()],
//             ViewStatus::Installed,
//         );
//         let rc = Rc::clone(&view.data);
//         let mut v1 = Version::<Full>::new(&rc, Some("a"));
//         let mut v2 = Version::<Full>::new(&rc, Some("b"));
//         v1.increment();
//         v2.increment();

//         assert_eq!(v1.partial_cmp(&v2), None);
//     }

//     #[test_log::test]
//     fn shortcut_clock() {
//         let view = View::new(
//             0,
//             vec!["a".to_string(), "b".to_string(), "c".to_string()],
//             ViewStatus::Installed,
//         );
//         let rc = Rc::clone(&view.data);
//         let mut v1 = Version::<Full>::new(&rc, Some("a"));
//         let mut v2 = Version::<Full>::new(&rc, Some("b"));
//         v1.increment();
//         v1.increment();
//         v2.merge(&v1);

//         assert_eq!(v1.clock, v2.clock);

//         v2.increment();

//         assert_eq!(v1.partial_cmp(&v2), Some(Ordering::Less));
//     }

//     #[test_log::test]
//     fn same_clock() {
//         let view = View::new(
//             0,
//             vec!["a".to_string(), "b".to_string(), "c".to_string()],
//             ViewStatus::Installed,
//         );
//         let rc = Rc::clone(&view.data);
//         let mut v1 = Version::<Full>::new(&rc, Some("a"));
//         let mut v2 = Version::<Full>::new(&rc, Some("a"));
//         v1.increment();
//         v2.increment();

//         assert_eq!(v1.partial_cmp(&v2), Some(Ordering::Equal));
//     }

//     #[test_log::test]
//     fn same_origin_clock() {
//         let view = View::new(
//             0,
//             vec!["a".to_string(), "b".to_string(), "c".to_string()],
//             ViewStatus::Installed,
//         );
//         let rc = Rc::clone(&view.data);
//         let mut v1 = Version::<Full>::new(&rc, Some("a"));
//         let mut v2 = Version::<Full>::new(&rc, Some("a"));
//         v1.increment();
//         v2.increment();
//         v2.increment();

//         assert_eq!(v1.partial_cmp(&v2), Some(Ordering::Less));
//     }

//     #[test_log::test]
//     fn clock() {
//         let view = View::new(
//             0,
//             vec!["a".to_string(), "b".to_string(), "c".to_string()],
//             ViewStatus::Installed,
//         );
//         let rc = Rc::clone(&view.data);
//         let mut v1 = Version::<Full>::new(&rc, Some("a"));
//         let mut v2 = Version::<Full>::new(&rc, Some("b"));

//         v1.increment();
//         v1.increment();
//         v2.increment();
//         v2.increment();
//         v2.increment();

//         assert_eq!(v1.get("a").unwrap(), 2);
//         assert_eq!(v1.get("b").unwrap(), 0);
//         assert_eq!(v1.get("c").unwrap(), 0);

//         assert_eq!(v2.get("a").unwrap(), 0);
//         assert_eq!(v2.get("b").unwrap(), 3);
//         assert_eq!(v2.get("c").unwrap(), 0);

//         v1.merge(&v2);
//         assert_eq!(v1.get("a").unwrap(), 2);
//         assert_eq!(v1.get("b").unwrap(), 3);
//         assert_eq!(v1.get("c").unwrap(), 0);

//         v2.merge(&v1);
//         assert_eq!(v2.get("a").unwrap(), 2);
//         assert_eq!(v2.get("b").unwrap(), 3);
//         assert_eq!(v2.get("c").unwrap(), 0);
//     }

//     #[test_log::test]
//     fn display() {
//         let view = View::new(
//             0,
//             vec!["a".to_string(), "b".to_string(), "c".to_string()],
//             ViewStatus::Installed,
//         );
//         let rc = Rc::clone(&view.data);
//         let v1 = Version::<Full>::new(&rc, Some("a"));
//         assert_eq!(format!("{}", v1), "{ a: 0, b: 0, c: 0 }@a");
//     }
// }
