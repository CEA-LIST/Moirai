use std::{fmt::Debug, sync::Arc};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use elsa::sync::FrozenVec;

#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    HashMap,
    replica::{ReplicaId, ReplicaIdOwned, ReplicaIdx},
};

#[derive(Clone)]
pub struct Resolver {
    inner: Arc<FrozenVec<ReplicaIdOwned>>,
}

#[cfg(feature = "serde")]
impl Serialize for Resolver {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as a Vec<String>
        self.into_vec().serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for Resolver {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = Vec::<ReplicaIdOwned>::deserialize(deserializer)?;
        let frozen = FrozenVec::new();
        for item in vec {
            frozen.push(item);
        }
        Ok(Resolver {
            inner: Arc::new(frozen),
        })
    }
}

#[cfg(feature = "test_utils")]
impl DeepSizeOf for Resolver {
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        // FrozenVec doesn't implement DeepSizeOf, so we approximate by getting the vector
        let vec = self.into_vec();
        vec.deep_size_of_children(context)
    }
}

impl Resolver {
    pub fn resolve(&self, idx: ReplicaIdx) -> Option<&ReplicaId> {
        self.inner.get(idx.0)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn into_vec(&self) -> Vec<ReplicaIdOwned> {
        (*self.inner).clone().into_vec()
    }
}

impl Debug for Resolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    str_to_int: HashMap<ReplicaIdOwned, ReplicaIdx>,
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
            str_to_int: HashMap::default(),
            int_to_str: Resolver {
                inner: Arc::new(FrozenVec::new()),
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

    /// Bring the cached translation of `from`'s indices up to date with the
    /// resolver it just sent, returning the replicas this call learned about.
    ///
    /// # Why this verifies instead of appending
    ///
    /// A row caches "the sender's index `i` means my index `row[i]`". That used
    /// to be extended by length alone: equal lengths meant nothing to do. The
    /// assumption underneath was that a replica's own index ordering only ever
    /// grows by appending, which held for as long as the only way to learn a
    /// member was to receive a message from it.
    ///
    /// State transfer breaks it. A fresh joiner adopts its donor's ordering
    /// wholesale, so its resolver is *reordered*, usually without changing
    /// length. A peer that had already cached a row for it would then keep
    /// translating by the old ordering — and this fails silently, in the worst
    /// possible way: every column of every incoming clock lands on the wrong
    /// replica, so operations are neither rejected nor delivered, they simply
    /// wait in the inbox for a causal predecessor that will never arrive.
    /// Measured exactly that way, as a joiner whose first write after adopting
    /// never became causally ready anywhere.
    ///
    /// So the row is verified against the incoming resolver and rebuilt from
    /// the first position that disagrees. The cost is one string comparison per
    /// member per message, on a path that already walks the sender's whole
    /// version vector — no change in complexity.
    pub fn update_translation(
        &mut self,
        from: ReplicaIdx,
        incoming_resolver: &Resolver,
    ) -> Vec<ReplicaIdx> {
        let row_len = self.translator.inner.get(from.0).unwrap().len();
        let mut agreed = 0;
        while agreed < row_len && agreed < incoming_resolver.len() {
            let cached = self.translator.inner[from.0][agreed];
            if self.int_to_str.resolve(cached) != incoming_resolver.resolve(ReplicaIdx(agreed)) {
                break;
            }
            agreed += 1;
        }
        if agreed < row_len {
            self.translator.inner[from.0].truncate(agreed);
        }

        let mut new_indices = Vec::new();
        for i in agreed..incoming_resolver.len() {
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
    }

    pub fn intern(&mut self, id: &ReplicaId) -> (ReplicaIdx, bool) {
        if let Some(&idx) = self.str_to_int.get(id) {
            return (idx, false);
        }
        let idx = self.int_to_str.inner.len();
        self.int_to_str.inner.push(id.to_string());
        self.str_to_int.insert(id.to_string(), ReplicaIdx(idx));
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

pub trait InternalizeOp {
    fn internalize(self, interner: &Interner) -> Self;
}

impl<T: InternalizeOp> InternalizeOp for Box<T> {
    fn internalize(self, interner: &Interner) -> Self {
        Box::new((*self).internalize(interner))
    }
}
