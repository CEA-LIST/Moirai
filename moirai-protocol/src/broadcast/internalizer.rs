/////////////////////////////////////////////////////////////////////////////
//  Copyright (c) 2025, 2026 CEA LIST.
//
//  This program and the accompanying materials
//  are made available under the terms of the Apache License, Version 2.0
//  which accompanies this distribution, and is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Contributors:
// Léo Olivier -  (CEA LIST) - Initial implementation
//
/////////////////////////////////////////////////////////////////////////////

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
    const fn new() -> Self {
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

#[cfg(feature = "serde")]
impl serde::Serialize for Resolver {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.into_vec(), serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Resolver {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let members = <Vec<ReplicaId> as serde::Deserialize>::deserialize(deserializer)?;
        SHARED.with_borrow_mut(|shared| match shared {
            Some(Some(table)) if !members.is_empty() => {
                if table.inner.iter().eq(members.iter()) {
                    Ok(table.clone())
                } else {
                    Err(serde::de::Error::custom(
                        "a snapshot with two member tables",
                    ))
                }
            }
            Some(slot) if !members.is_empty() => {
                let table = Self::from_members(members).map_err(serde::de::Error::custom)?;
                *slot = Some(table.clone());
                Ok(table)
            }
            _ => Self::from_members(members).map_err(serde::de::Error::custom),
        })
    }
}

#[cfg(feature = "serde")]
impl Resolver {
    fn from_members(members: Vec<ReplicaId>) -> std::result::Result<Self, &'static str> {
        let mut interner = Interner::new();
        for member in members {
            if !interner.intern(&member).1 {
                return Err("duplicate replica in resolver");
            }
        }
        Ok(interner.resolver().clone())
    }
}

#[cfg(feature = "serde")]
thread_local! {
    /// While a snapshot deserializes: the one member table all its resolvers share.
    static SHARED: std::cell::RefCell<Option<Option<Resolver>>> = const { std::cell::RefCell::new(None) };
}

/// Runs `f`, a snapshot's deserialization, with every non-empty resolver it
/// reads sharing one table, and returns that table. An adopter interns
/// itself into it, so every id, version and frontier inside the adopted log
/// resolves through the adopter's own table without being rewritten.
#[cfg(feature = "serde")]
pub(crate) fn with_shared_resolver<T>(f: impl FnOnce() -> T) -> (T, Option<Resolver>) {
    struct Scope;
    impl Drop for Scope {
        fn drop(&mut self) {
            SHARED.set(None);
        }
    }
    SHARED.set(Some(None));
    let scope = Scope;
    let out = f();
    let table = SHARED.with_borrow_mut(|shared| shared.take().flatten());
    drop(scope);
    (out, table)
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

    /// An interner over an existing table, which interning then extends in place.
    #[cfg(feature = "serde")]
    pub(crate) fn from_resolver(resolver: Resolver) -> Self {
        Self {
            str_to_int: resolver
                .inner
                .iter()
                .enumerate()
                .map(|(idx, id)| (id.clone(), ReplicaIdx(idx)))
                .collect(),
            translator: Translator {
                inner: vec![Vec::new(); resolver.len()],
            },
            int_to_str: resolver,
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

    pub const fn resolver(&self) -> &Resolver {
        &self.int_to_str
    }
}
