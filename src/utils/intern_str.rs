use elsa::FrozenVec;
use std::fmt::Debug;
use std::rc::Rc;

use crate::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReplicaIdx(pub usize);

#[derive(Clone)]
pub struct Resolver {
    inner: Rc<FrozenVec<String>>,
}

impl Resolver {
    pub fn resolve(&self, idx: ReplicaIdx) -> Option<&str> {
        self.inner.get(idx.0)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn into_vec(&self) -> Vec<String> {
        (*self.inner).clone().into_vec()
    }
}

impl Debug for Resolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, s) in self.inner.iter().enumerate() {
            writeln!(f, "{} => {}", i, s)?;
        }
        Ok(())
    }
}

impl PartialEq for Resolver {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}

#[derive(Debug)]
pub struct Interner {
    str_to_int: HashMap<String, ReplicaIdx>,
    int_to_str: Resolver,
}

impl Interner {
    pub fn new() -> Self {
        Self {
            str_to_int: HashMap::default(),
            int_to_str: Resolver {
                inner: Rc::new(FrozenVec::new()),
            },
        }
    }

    pub fn intern(&mut self, s: &str) -> (ReplicaIdx, bool) {
        if let Some(&idx) = self.str_to_int.get(s) {
            return (idx, false);
        }
        let idx = self.int_to_str.inner.len();
        self.int_to_str.inner.push(s.to_string());
        self.str_to_int.insert(s.to_string(), ReplicaIdx(idx));
        (ReplicaIdx(idx), true)
    }

    pub fn resolve(&self, idx: ReplicaIdx) -> Option<&str> {
        self.int_to_str.resolve(idx)
    }

    pub fn get(&self, s: &str) -> Option<ReplicaIdx> {
        self.str_to_int.get(s).copied()
    }

    pub fn resolver(&self) -> &Resolver {
        &self.int_to_str
    }
}
