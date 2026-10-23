use std::{
    fmt::{Debug, Formatter, Result},
    rc::Rc,
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use elsa::FrozenVec;
use rustc_hash::FxHashMap;

use crate::replica::{ReplicaId, ReplicaIdx};

/// Append-only storage for replica IDs.
///
/// The `Box` keeps each [`ReplicaId`] at a stable address while the backing
/// `FrozenVec` grows. This lets the resolver return `&ReplicaId` rather than
/// the `&str` exposed by a `FrozenVec<String>`.
struct ReplicaIds(FrozenVec<Box<ReplicaId>>);

impl ReplicaIds {
    fn new() -> Self {
        Self(FrozenVec::new())
    }

    fn push(&self, id: ReplicaId) {
        self.0.push(Box::new(id));
    }

    fn get(&self, index: usize) -> Option<&ReplicaId> {
        self.0.get(index)
    }

    fn iter(&self) -> impl Iterator<Item = &ReplicaId> {
        self.0.iter()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(feature = "test_utils")]
impl DeepSizeOf for ReplicaIds {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        // `FrozenVec`'s generic implementation visits `T::Target`, so for
        // `Box<ReplicaId>` it misses the allocation containing `ReplicaId`.
        // Count that allocation explicitly for every live entry, then let
        // `ReplicaId` account for its String buffer. Unused vector capacity
        // only consists of Box-sized slots.
        self.0.capacity() * std::mem::size_of::<Box<ReplicaId>>()
            + self
                .iter()
                .map(|id| std::mem::size_of_val(id) + id.deep_size_of_children(context))
                .sum::<usize>()
    }
}

#[derive(Clone)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct Resolver {
    inner: Rc<ReplicaIds>,
}

impl Default for Resolver {
    fn default() -> Self {
        Self {
            inner: Rc::new(ReplicaIds::new()),
        }
    }
}

impl Resolver {
    /// # Complexity
    /// `O(1)`
    pub fn resolve(&self, idx: ReplicaIdx) -> Option<&ReplicaId> {
        self.inner.get(idx.0)
    }

    /// # Complexity
    /// `O(n)`
    pub fn get(&self, id: &ReplicaId) -> Option<ReplicaIdx> {
        self.inner.iter().position(|s| s == id).map(ReplicaIdx)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn into_vec(&self) -> Vec<ReplicaId> {
        self.inner.iter().cloned().collect()
    }
}

impl Debug for Resolver {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        for (i, s) in self.inner.iter().enumerate() {
            write!(f, "{i} => {s}")?;
            if i < self.inner.len() - 1 {
                write!(f, ", ")?;
            }
        }
        Ok(())
    }
}

impl PartialEq for Resolver {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}

#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct Translator {
    inner: Vec<Vec<ReplicaIdx>>,
}

impl Debug for Translator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, row) in self.inner.iter().enumerate() {
            write!(f, "{i} => {row:?}")?;
            if i < self.inner.len() - 1 {
                write!(f, ", ")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct Interner {
    str_to_int: FxHashMap<ReplicaId, ReplicaIdx>,
    int_to_str: Resolver,
    /// Each process keep a translation of its indices to the one of the other (matrix).
    /// Indices of the Vec = other process mapping
    /// Content of the Vec = local process mapping
    translator: Translator,
}

impl Default for Interner {
    fn default() -> Self {
        Self::new()
    }
}

impl Interner {
    pub fn new() -> Self {
        Self {
            str_to_int: FxHashMap::default(),
            int_to_str: Resolver {
                inner: Rc::new(ReplicaIds::new()),
            },
            translator: Translator { inner: Vec::new() },
        }
    }

    /// Translate a replica index from another replica to the local one.
    pub fn translate(&self, from: ReplicaIdx, remote_idx: ReplicaIdx) -> ReplicaIdx {
        let row = self.translator.inner.get(from.0).unwrap();
        let local_idx = row.get(remote_idx.0).unwrap();
        *local_idx
    }

    pub fn update_translation(
        &mut self,
        from: ReplicaIdx,
        incoming_resolver: &Resolver,
    ) -> Vec<ReplicaIdx> {
        if self.translator.inner.get(from.0).unwrap().len() == incoming_resolver.len() {
            // No need to update
            Vec::new()
        } else if self.translator.inner.get(from.0).unwrap().len() < incoming_resolver.len() {
            let mut new_indices = Vec::new();
            let row_len = self.translator.inner.get_mut(from.0).unwrap().len();
            for i in row_len..incoming_resolver.len() {
                let id = incoming_resolver.resolve(ReplicaIdx(i)).unwrap();
                let (local_idx, is_new) = self.intern(id);
                self.translator
                    .inner
                    .get_mut(from.0)
                    .unwrap()
                    .push(local_idx);
                if is_new {
                    new_indices.push(local_idx);
                }
            }
            new_indices
        } else {
            panic!("Inconsistent state: incoming resolver is smaller than the known one");
        }
    }

    pub fn intern(&mut self, id: &ReplicaId) -> (ReplicaIdx, bool) {
        if let Some(&idx) = self.str_to_int.get(id) {
            return (idx, false);
        }
        let idx = self.int_to_str.inner.len();
        self.int_to_str.inner.push(id.clone());
        self.str_to_int.insert(id.clone(), ReplicaIdx(idx));
        self.translator.inner.push(vec![]);

        assert_eq!(self.int_to_str.inner.len(), self.str_to_int.len());
        assert_eq!(self.int_to_str.inner.len(), self.translator.inner.len());

        (ReplicaIdx(idx), true)
    }

    pub fn resolve(&self, idx: ReplicaIdx) -> Option<&ReplicaId> {
        self.int_to_str.resolve(idx)
    }

    pub fn get(&self, id: &ReplicaId) -> Option<ReplicaIdx> {
        self.str_to_int.get(id).copied()
    }

    pub fn resolver(&self) -> &Resolver {
        &self.int_to_str
    }
}
