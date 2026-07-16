use std::{
    cmp::Ordering,
    fmt::Display,
    hash::{Hash, Hasher},
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::{
    broadcast::internalizer::{InternalizeOp, Interner, Resolver},
    clock::version_vector::{Seq, Version},
    replica::{ReplicaId, ReplicaIdx},
};

/// Represents the unique identifier for an event.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct EventId {
    /// The index of the replica that issued the event.
    idx: ReplicaIdx,
    /// The sequence number of the event for the given replica.
    seq: Seq,
    /// Used to resolve the replica index to a replica ID.
    resolver: Resolver,
    /// Distinguishes deterministic derived events emitted from the same source event.
    disambiguator: Option<u32>,
}

impl EventId {
    pub fn new(idx: ReplicaIdx, seq: Seq, resolver: Resolver) -> Self {
        Self::new_with_disambiguator(idx, seq, resolver, None)
    }

    pub fn new_with_disambiguator(
        idx: ReplicaIdx,
        seq: Seq,
        resolver: Resolver,
        disambiguator: Option<u32>,
    ) -> Self {
        Self {
            idx,
            seq,
            resolver,
            disambiguator,
        }
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

    pub fn disambiguator(&self) -> Option<u32> {
        self.disambiguator
    }

    pub fn with_disambiguator(mut self, disambiguator: u32) -> Self {
        self.disambiguator = Some(disambiguator);
        self
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
        EventId::new_with_disambiguator(
            idx,
            self.seq(),
            interner.resolver().clone(),
            self.disambiguator(),
        )
    }
}

impl Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(disambiguator) = self.disambiguator {
            write!(f, "({}:{}#{})", self.origin_id(), self.seq(), disambiguator)
        } else {
            write!(f, "({}:{})", self.origin_id(), self.seq())
        }
    }
}

impl Hash for EventId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.origin_id().hash(state);
        self.seq.hash(state);
        self.disambiguator.hash(state);
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
            Ordering::Equal => match self.seq.cmp(&other.seq) {
                Ordering::Equal => self.disambiguator.cmp(&other.disambiguator),
                ord => ord,
            },
            ord => ord,
        }
    }
}

impl PartialEq for EventId {
    fn eq(&self, other: &Self) -> bool {
        self.origin_id() == other.origin_id()
            && self.seq == other.seq
            && self.disambiguator == other.disambiguator
    }
}

impl Eq for EventId {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disambiguator_is_part_of_identity_but_not_causality() {
        let mut interner = Interner::new();
        let (idx, _) = interner.intern(&"a".to_string());
        let base = EventId::new(idx, 54, interner.resolver().clone());
        let derived_1 =
            EventId::new_with_disambiguator(idx, 54, interner.resolver().clone(), Some(1));
        let derived_2 =
            EventId::new_with_disambiguator(idx, 54, interner.resolver().clone(), Some(2));

        assert_ne!(base, derived_1);
        assert_ne!(derived_1, derived_2);
        assert!(base < derived_1);
        assert!(derived_1 < derived_2);

        let mut ids = std::collections::HashSet::new();
        ids.insert(base.clone());
        ids.insert(derived_1.clone());
        ids.insert(derived_2.clone());
        assert_eq!(ids.len(), 3);

        let mut version = Version::new(idx, interner.resolver().clone());
        version.set_by_idx(idx, 54);
        assert!(base.is_predecessor_of(&version));
        assert!(derived_1.is_predecessor_of(&version));
        assert!(derived_2.is_predecessor_of(&version));
        assert_eq!(format!("{}", derived_2), "(a:54#2)");
    }

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
        let size_disambiguator = event_1.disambiguator.deep_size_of();
        let container = vec![event_0.clone(), event_1.clone()];
        let size_container = Vec::<EventId>::new().deep_size_of();
        let size_rc = 8; // Assuming 8 bytes for the Rc pointer in the resolver
        assert_eq!(
            size_event_0 + size_seq + size_idx + size_disambiguator + size_container + size_rc,
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
