use std::{
    fmt::Display,
    hash::{Hash, Hasher},
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::{
    broadcast::internalizer::{Interner, Resolver},
    clock::version_vector::{Seq, Version},
    replica::{ReplicaId, ReplicaIdOwned, ReplicaIdx},
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

/// `EventId` stores a local `ReplicaIdx`, which is only meaningful with the
/// replica's own interner. `ResolvedEventId` stores the resolved replica id
/// instead, so it can safely be embedded in operation payloads.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
pub struct ResolvedEventId {
    /// Plain string identifier of the issuing replica.
    origin_id: ReplicaIdOwned,
    /// Sequence number.
    seq: Seq,
    /// Distinguishes deterministic derived events emitted from the same source event.
    disambiguator: Option<u32>,
}

impl ResolvedEventId {
    pub fn new(origin_id: ReplicaIdOwned, seq: Seq) -> Self {
        Self::new_with_disambiguator(origin_id, seq, None)
    }

    pub fn new_with_disambiguator(
        origin_id: ReplicaIdOwned,
        seq: Seq,
        disambiguator: Option<u32>,
    ) -> Self {
        Self {
            origin_id,
            seq,
            disambiguator,
        }
    }

    pub fn origin_id(&self) -> &ReplicaId {
        &self.origin_id
    }

    pub fn seq(&self) -> Seq {
        self.seq
    }

    pub fn disambiguator(&self) -> Option<u32> {
        self.disambiguator
    }
}

impl From<&EventId> for ResolvedEventId {
    fn from(event_id: &EventId) -> Self {
        Self::new_with_disambiguator(
            event_id.origin_id().to_string(),
            event_id.seq(),
            event_id.disambiguator(),
        )
    }
}

impl From<EventId> for ResolvedEventId {
    fn from(event_id: EventId) -> Self {
        Self::from(&event_id)
    }
}

impl From<(&ResolvedEventId, &Interner)> for EventId {
    fn from((event_id, interner): (&ResolvedEventId, &Interner)) -> Self {
        let idx = interner.get(event_id.origin_id()).unwrap_or_else(|| {
            panic!(
                "Cannot translate embedded EventId for unknown replica origin {}",
                event_id.origin_id()
            )
        });
        EventId::new_with_disambiguator(
            idx,
            event_id.seq(),
            interner.resolver().clone(),
            event_id.disambiguator(),
        )
    }
}

impl From<(ResolvedEventId, &Interner)> for EventId {
    fn from((event_id, interner): (ResolvedEventId, &Interner)) -> Self {
        Self::from((&event_id, interner))
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

impl PartialEq for EventId {
    fn eq(&self, other: &Self) -> bool {
        self.origin_id() == other.origin_id()
            && self.seq == other.seq
            && self.disambiguator == other.disambiguator
    }
}

impl Eq for EventId {}

impl Display for ResolvedEventId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(disambiguator) = self.disambiguator {
            write!(f, "({}:{}#{})", self.origin_id, self.seq, disambiguator)
        } else {
            write!(f, "({}:{})", self.origin_id, self.seq)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
