use std::{cmp::Ordering, fmt::Display, hash::Hash};

use log::error;
// #[cfg(feature = "utils")]
// use deepsize::DeepSizeOf;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use tsify::Tsify;

use crate::{
    protocol::{event::id::EventId, membership::ReplicaId},
    utils::intern_str::{ReplicaIdx, Resolver},
};

/// Sequence number
pub type Seq = usize;

#[derive(Debug, Clone, PartialEq)]
struct VersionEntries(Vec<Seq>);

impl VersionEntries {
    fn get(&self, idx: ReplicaIdx) -> Seq {
        *self.0.get(idx.0).unwrap_or(&0)
    }

    fn get_mut(&mut self, idx: ReplicaIdx) -> &mut Seq {
        self.fill_to(idx.0 + 1);
        self.0.get_mut(idx.0).unwrap()
    }

    fn fill_to(&mut self, len: usize) {
        if len > self.0.len() {
            self.0.resize(len, 0);
        }
    }

    /// # Complexity
    /// Runs in `O(n)` time complexity with `n` being the number of members in the view
    fn join(&mut self, other: &Self) {
        self.fill_to(other.0.len());
        self.0
            .iter_mut()
            .zip(other.0.iter().chain(std::iter::repeat(&0)))
            .for_each(|(a, b)| {
                if *a < *b {
                    *a = *b;
                }
            });
    }

    /// # Complexity
    /// Runs in `O(n)` time complexity with `n` being the number of members in the view
    fn meet(&mut self, other: &Self) {
        self.fill_to(other.0.len());
        self.0
            .iter_mut()
            .zip(other.0.iter().chain(std::iter::repeat(&0)))
            .for_each(|(a, b)| {
                if *a > *b {
                    *a = *b;
                }
            });
    }

    fn values(&self) -> impl Iterator<Item = &Seq> {
        self.0.iter()
    }
}

// #[cfg_attr(feature = "utils", derive(DeepSizeOf))]
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize, Tsify),
    tsify(into_wasm_abi, from_wasm_abi)
)]
pub struct Version {
    entries: VersionEntries,
    origin_idx: ReplicaIdx,
    resolver: Resolver,
}

impl Hash for Version {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.origin_idx.hash(state);
        self.entries.0.hash(state);
    }
}

impl Version {
    pub fn new(origin_idx: ReplicaIdx, resolver: Resolver) -> Self {
        let entries = vec![0; resolver.len()];
        Self {
            entries: VersionEntries(entries),
            origin_idx,
            resolver,
        }
    }

    /// Increment the origin's entry.
    ///
    /// # Complexity
    /// Runs in `O(1)` time complexity
    pub fn increment(&mut self) -> usize {
        let seq = self.entries.get_mut(self.origin_idx);
        *seq += 1;
        *seq
    }

    /// Merge two clocks that share the same view.
    ///
    /// # Complexity
    /// Checks that the views are identical.
    /// Then runs in `O(n)` time complexity with `n` being the number of members in the view
    // TODO: using lamport timestamp: if other_old.lamport = other_new.lamport + 1, then just incr the other origin value
    pub fn join(&mut self, other: &Self) {
        // if `self` dominate `other`, then no need to merge.
        // TODO: lsv has an origin_id
        // if EventId::from(other).is_predecessor_of(self) {
        //     return;
        // }
        self.entries.join(&other.entries);
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (ReplicaIdx, Seq)> + 'a {
        self.entries
            .0
            .iter()
            .enumerate()
            .map(|(i, v)| (ReplicaIdx(i), *v))
    }

    pub fn meet(&mut self, other: &Self) {
        self.entries.meet(&other.entries);
    }

    pub fn seq_by_idx(&self, idx: ReplicaIdx) -> Seq {
        self.entries.get(idx)
    }

    pub fn sum(&self) -> usize {
        self.entries.values().sum()
    }

    pub fn set_by_idx(&mut self, idx: ReplicaIdx, value: Seq) {
        *self.entries.get_mut(idx) = value;
    }

    pub fn len(&self) -> usize {
        self.resolver.len()
    }

    pub fn is_empty(&self) -> bool {
        self.resolver.is_empty()
    }

    pub fn origin_idx(&self) -> ReplicaIdx {
        self.origin_idx
    }

    pub fn origin_id(&self) -> &ReplicaId {
        self.resolver.resolve(self.origin_idx).unwrap()
    }

    pub fn origin_seq(&self) -> Seq {
        self.seq_by_idx(self.origin_idx)
    }

    #[cfg(test)]
    pub(in crate::protocol::clock) fn build(
        resolver: Resolver,
        origin_idx: ReplicaIdx,
        values: &[usize],
    ) -> Self {
        let mut v = Version::new(origin_idx, resolver);
        for (idx, val) in values.iter().enumerate() {
            v.set_by_idx(ReplicaIdx(idx), *val);
        }
        v
    }
}

impl From<&Version> for EventId {
    fn from(version: &Version) -> Self {
        if version.origin_seq() == 0 {
            error!("Version {} has an origin sequence number of 0", version);
        }
        EventId::new(
            version.origin_idx(),
            version.origin_seq(),
            version.resolver.clone(),
        )
    }
}

impl Display for Version {
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // ["a":1,"b":2,"c":3]@a
        write!(
            _f,
            "{{{}}}@{}",
            self.iter()
                .map(|(idx, seq)| format!("\"{}\":{}", self.resolver.resolve(idx).unwrap(), seq))
                .collect::<Vec<String>>()
                .join(", "),
            self.origin_id()
        )
    }
}

impl PartialEq for Version {
    fn eq(&self, other: &Self) -> bool {
        self.resolver == other.resolver
            && self.origin_idx == other.origin_idx
            && self.entries == other.entries
    }
}

impl Eq for Version {}

impl PartialOrd for Version {
    // TODO: add shortcut + check correctness
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.resolver != other.resolver {
            panic!("Comparing versions with different views");
        }
        let mut self_greater = false;
        let mut other_greater = false;

        let l = self.entries.0.len().max(other.entries.0.len());

        for (a, b) in self
            .entries
            .values()
            .chain(std::iter::repeat(&0))
            .zip(other.entries.values().chain(std::iter::repeat(&0)))
            .take(l)
        {
            match a.cmp(b) {
                Ordering::Greater => self_greater = true,
                Ordering::Less => other_greater = true,
                Ordering::Equal => {}
            }
            if self_greater && other_greater {
                return None;
            }
        }
        match (self_greater, other_greater) {
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            (false, false) => Some(Ordering::Equal),
            (true, true) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::intern_str::Interner;

    #[test]
    fn concurrent_clock() {
        let mut interner = Interner::new();
        let a_idx = interner.intern("a");
        let b_idx = interner.intern("b");

        let mut v1 = Version::new(a_idx.0, interner.resolver().clone());
        let mut v2 = Version::new(b_idx.0, interner.resolver().clone());

        v1.increment();
        v2.increment();

        assert_eq!(v1.partial_cmp(&v2), None);
    }

    #[test]
    fn shortcut_clock() {
        let mut interner = Interner::new();
        let a_idx = interner.intern("a");
        let b_idx = interner.intern("b");

        let mut v1 = Version::new(a_idx.0, interner.resolver().clone());
        let mut v2 = Version::new(b_idx.0, interner.resolver().clone());

        v1.increment();
        v1.increment();

        v2.join(&v1);

        assert_eq!(v1.entries, v2.entries);

        v2.increment();
        assert_eq!(v1.partial_cmp(&v2), Some(Ordering::Less));
    }

    #[test]
    fn same_clock() {
        let mut interner = Interner::new();
        let a_idx = interner.intern("a");

        let mut v1 = Version::new(a_idx.0, interner.resolver().clone());
        let mut v2 = Version::new(a_idx.0, interner.resolver().clone());

        v1.increment();
        v2.increment();

        assert_eq!(v1.partial_cmp(&v2), Some(Ordering::Equal));
    }
}
