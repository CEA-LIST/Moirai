use std::{
    fmt::{Debug, Formatter, Result},
    sync::Arc,
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use elsa::sync::FrozenVec;
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
        // shortcut: sync FrozenVec hides spare capacity, account for it when its API exposes it.
        self.0.len() * std::mem::size_of::<Box<ReplicaId>>()
            + self
                .iter()
                .map(|id| std::mem::size_of_val(id) + id.deep_size_of_children(context))
                .sum::<usize>()
    }
}

#[derive(Clone)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct Resolver {
    inner: Arc<ReplicaIds>,
}

impl Default for Resolver {
    fn default() -> Self {
        Self {
            inner: Arc::new(ReplicaIds::new()),
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
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Resolver {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.into_vec().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Resolver {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let members = Vec::<ReplicaId>::deserialize(deserializer)?;
        TRANSFER_MEMBERS.with(|allowed| {
            if let Some(allowed) = allowed.borrow().as_ref() {
                for member in &members {
                    if allowed.get(member).is_none() {
                        return Err(serde::de::Error::custom(format!(
                            "unknown transferred member {member}"
                        )));
                    }
                }
            }
            Ok(())
        })?;
        let mut interner = Interner::new();
        for member in members {
            if !interner.intern(&member).1 {
                return Err(serde::de::Error::custom("duplicate replica in resolver"));
            }
        }
        Ok(interner.resolver().clone())
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
                inner: Arc::new(ReplicaIds::new()),
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
        // Adoption can replace a sender's member indices; delayed messages can still use its old view.
        let row = &self.translator.inner[from.0];
        if row
            .iter()
            .enumerate()
            .any(|(idx, local)| incoming_resolver.resolve(ReplicaIdx(idx)) != self.resolve(*local))
        {
            self.translator.inner[from.0].clear();
        }
        let mut new_indices = Vec::new();
        for i in self.translator.inner[from.0].len()..incoming_resolver.len() {
            let id = incoming_resolver.resolve(ReplicaIdx(i)).unwrap();
            let (local_idx, is_new) = self.intern(id);
            self.translator.inner[from.0].push(local_idx);
            if is_new {
                new_indices.push(local_idx);
            }
        }
        new_indices
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

    pub fn resolver(&self) -> Resolver {
        self.int_to_str.clone()
    }
}

/// One node's member table, shared by its hosted logs.
pub type SharedInterner = std::sync::Arc<std::sync::Mutex<Interner>>;

pub fn lock_interner(table: &SharedInterner) -> std::sync::MutexGuard<'_, Interner> {
    table
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Interner {
    pub fn into_shared(self) -> SharedInterner {
        std::sync::Arc::new(std::sync::Mutex::new(self))
    }
}

impl Resolver {
    pub fn agrees_with(&self, other: &Self) -> bool {
        (0..self.len().min(other.len()))
            .all(|idx| self.resolve(ReplicaIdx(idx)) == other.resolve(ReplicaIdx(idx)))
    }

    /// Validate typed resolvers while decoding an opaque transferred log.
    pub fn with_members<R>(&self, decode: impl FnOnce() -> R) -> R {
        struct Restore(Option<Resolver>);
        impl Drop for Restore {
            fn drop(&mut self) {
                TRANSFER_MEMBERS.with(|members| *members.borrow_mut() = self.0.take());
            }
        }
        let _restore =
            Restore(TRANSFER_MEMBERS.with(|members| members.replace(Some(self.clone()))));
        decode()
    }
}

thread_local! {
    static TRANSFER_MEMBERS: std::cell::RefCell<Option<Resolver>> = const { std::cell::RefCell::new(None) };
}
