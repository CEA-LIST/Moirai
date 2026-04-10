use std::{
    cmp::Ordering,
    fmt::Display,
    hash::{Hash, Hasher},
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::{
    clock::version_vector::{Seq, Version},
    replica::{ReplicaId, ReplicaIdx},
    utils::intern_str::{InternalizeOp, Interner, Resolver},
};

/// Represents the unique identifier for an operation.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct EventId {
    idx: ReplicaIdx,
    seq: Seq,
    resolver: Resolver,
}

impl EventId {
    pub fn new(idx: ReplicaIdx, seq: Seq, resolver: Resolver) -> Self {
        Self { idx, seq, resolver }
    }

    pub fn origin_id(&self) -> &ReplicaId {
        self.resolver.resolve(self.idx).unwrap()
    }

    pub fn seq(&self) -> Seq {
        self.seq
    }

    pub fn idx(&self) -> ReplicaIdx {
        self.idx
    }

    /// Check if this event id is a predecessor of the given version.
    /// # Note
    /// Returns `true` if sequence number of the version for the replica id is greater OR equal.
    pub fn is_predecessor_of(&self, version: &Version) -> bool {
        let ver_seq = version.seq_by_idx(self.idx);
        ver_seq >= self.seq
    }

    pub fn resolver(&self) -> &Resolver {
        &self.resolver
    }
}

impl InternalizeOp for EventId {
    fn internalize(self, interner: &Interner) -> Self {
        let idx = interner.get(self.origin_id()).unwrap_or_else(|| {
            panic!(
                "Cannot translate embedded EventId for unknown replica origin {}",
                self.origin_id()
            )
        });
        EventId::new(idx, self.seq(), interner.resolver().clone())
    }
}

impl Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}:{})", self.origin_id(), self.seq())
    }
}

impl Hash for EventId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.origin_id().hash(state);
        self.seq.hash(state);
    }
}

// TODO: Should be removed and replaced by a policy

impl PartialOrd for EventId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EventId {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.origin_id().cmp(other.origin_id()) {
            Ordering::Equal => self.seq.cmp(&other.seq),
            ord => ord,
        }
    }
}

impl PartialEq for EventId {
    fn eq(&self, other: &Self) -> bool {
        self.origin_id() == other.origin_id() && self.seq == other.seq
    }
}

impl Eq for EventId {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::intern_str::Interner;

    #[cfg(feature = "test_utils")]
    #[test]
    fn size_event_id() {
        let mut vec: Vec<String> = vec![];
        println!("Size of empty vec: {}", vec.deep_size_of());
        vec.push("a".to_string());
        println!("Size of vec with a: {}", vec.deep_size_of());
        vec.push("b".to_string());
        println!("Size of vec with a and b: {}", vec.deep_size_of());

        let frozen_vec = std::rc::Rc::new(vec.into_iter().collect::<elsa::FrozenVec<String>>());
        println!("Size of frozen vec: {}", frozen_vec.deep_size_of());

        let mut interner = Interner::new();
        println!(
            "resolver size after creation: {}",
            interner.resolver().deep_size_of()
        );
        let (idx_a, _) = interner.intern(&"A".to_string());
        println!(
            "resolver size after adding a: {}",
            interner.resolver().deep_size_of()
        );
        let (idx_b, _) = interner.intern(&"B".to_string());
        println!(
            "resolver size after adding b: {}",
            interner.resolver().deep_size_of()
        );

        let event1 = EventId::new(idx_a, 1, interner.resolver().clone());
        let event2 = EventId::new(idx_a, 2, interner.resolver().clone());
        let _ = EventId::new(idx_b, 1, interner.resolver().clone());

        println!("Size event 1 idx: {}", event1.idx.deep_size_of());
        println!("Size event 1 resolver: {}", event1.resolver.deep_size_of());
        println!("Size event 1 seq: {}", event1.seq.deep_size_of());
        println!("------------------------------");
        println!("Size event 2 idx: {}", event2.idx.deep_size_of());
        println!("Size event 2 resolver: {}", event2.resolver.deep_size_of());
        println!("Size event 2 seq: {}", event2.seq.deep_size_of());

        let mut interner = Interner::new();
        for i in 0..100 {
            let id = format!("{}", i);
            interner.intern(&id);
        }
        println!(
            "resolver size after adding 100 entries: {}",
            interner.resolver().deep_size_of()
        );
        let event_0 = EventId::new(ReplicaIdx(0), 1, interner.resolver().clone());
        let event_1 = EventId::new(ReplicaIdx(1), 1, interner.resolver().clone());

        let size_event_0 = event_0.deep_size_of();
        println!("Size of event 0: {}", size_event_0);
        let size_seq = event_1.seq.deep_size_of();
        let size_idx = event_1.idx.deep_size_of();
        let container = vec![event_0.clone(), event_1.clone()];
        let size_container = Vec::<EventId>::new().deep_size_of();
        let size_rc = 8; // Assuming 8 bytes for the Rc pointer in the resolver
        assert_eq!(
            size_event_0 + size_seq + size_idx + size_container + size_rc,
            container.deep_size_of()
        );
        println!(
            "Size of event 1: {}",
            size_seq + size_idx + size_container + size_rc
        );
        println!(
            "Size of container with 2 events: {}",
            container.deep_size_of()
        );
    }
}
