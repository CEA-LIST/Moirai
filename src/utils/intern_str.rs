use std::{fmt::Debug, rc::Rc};

use elsa::FrozenVec;

use crate::{
    protocol::replica::{ReplicaId, ReplicaIdOwned, ReplicaIdx},
    HashMap,
};

#[derive(Clone)]
pub struct Resolver {
    inner: Rc<FrozenVec<ReplicaIdOwned>>,
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
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}

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
                inner: Rc::new(FrozenVec::new()),
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
