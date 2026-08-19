use crate::{
    clock::version_vector::Version,
    event::{Event, id::EventId, tagged_op::TaggedOp},
    replica::ReplicaIdx,
    state::unstable_state::IsUnstableCore,
    utils::hashmap::HashMap,
};
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct EventHistory<O> {
    // TODO: use a vec rather than a hashmap
    /// Note: we assume events are inserted in order without gaps.
    /// The store should not be pruned as the index of the vec is used to determine the sequence number of an event.
    pub store: HashMap<ReplicaIdx, Vec<(TaggedOp<O>, Version)>>,
}

impl<O> Default for EventHistory<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O> EventHistory<O> {
    fn new() -> Self {
        Self {
            store: HashMap::default(),
        }
    }

    /// Let v ∈ G, previous(v) denotes the set of vertices z ∈ G such that z is the last vertex from z.id for which z ⇝ v
    pub fn previous(&self, version: &Version, r: ReplicaIdx) -> Option<&TaggedOp<O>> {
        let k = version.seq_by_idx(r);

        if k == 0 {
            None
        } else {
            self.store.get(&r)?.get(k - 1).map(|e| &e.0)
        }
    }

    /// Let v ∈ G, next(v) denotes the set of vertices w ∈ G such that w is the first vertex from w.id for which v ⇝ w
    pub fn next(&self, event_id: &EventId, r: ReplicaIdx) -> Option<&TaggedOp<O>> {
        if event_id.seq() == 0 {
            return None;
        }

        let events = self.store.get(&r)?;

        let index = events
            .partition_point(|(_, version)| version.seq_by_idx(event_id.idx()) < event_id.seq());

        events.get(index).map(|(event, _)| event)
    }
}

impl<O> IsUnstableCore<O> for EventHistory<O>
where
    O: Debug + Clone,
{
    fn append(&mut self, event: Event<O>) {
        let origin = event.id().idx();
        let sequence = event.id().seq();
        let expected_sequence = self.store.get(&origin).map_or(1, |events| events.len() + 1);

        assert!(
            event.version().origin_idx() == origin,
            "EventHistory event dot origin mismatch: event ID uses {origin:?}, version uses {:?}",
            event.version().origin_idx()
        );
        assert!(
            event.version().origin_seq() == sequence,
            "EventHistory event dot sequence mismatch: event ID uses {sequence}, version uses {}",
            event.version().origin_seq()
        );
        assert!(
            sequence == expected_sequence,
            "EventHistory requires dense per-origin insertion: expected sequence {expected_sequence}, got {sequence} for replica {origin:?}"
        );

        if let Some((_, previous_version)) =
            self.store.get(&origin).and_then(|events| events.last())
        {
            for (replica, previous_sequence) in previous_version.iter() {
                let sequence = event.version().seq_by_idx(replica);
                assert!(
                    sequence >= previous_sequence,
                    "EventHistory requires componentwise-monotone versions per origin: component {replica:?} regressed from {previous_sequence} to {sequence}"
                );
            }
        }

        let tagged_op = TaggedOp::from(&event);

        self.store
            .entry(origin)
            .or_default()
            .push((tagged_op, event.version().clone()));
    }

    fn get(&self, event_id: &EventId) -> Option<&TaggedOp<O>> {
        let index = event_id.seq().checked_sub(1)?;
        let (tagged_op, _) = self.store.get(&event_id.idx())?.get(index)?;

        (tagged_op.id() == event_id).then_some(tagged_op)
    }

    fn predecessors(&self, version: &Version) -> Vec<&TaggedOp<O>>
    where
        O: Clone,
    {
        self.store
            .iter()
            .flat_map(|(r, events)| {
                let k = version.seq_by_idx(*r);

                events[..k.min(events.len())].iter().map(|(t, _)| t)
            })
            .collect()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a,
    {
        self.store
            .values()
            .flat_map(|events| events.iter().map(|(t, _)| t))
    }

    fn len(&self) -> usize {
        self.store.values().map(Vec::len).sum()
    }

    fn is_empty(&self) -> bool {
        self.store.values().all(Vec::is_empty)
    }
}

// impl<O> IsUnstablePrune<O> for EventHistory<O>
// where
//     O: Debug + Clone,
// {
//     fn remove(&mut self, event_id: &EventId) {
//         self.store
//             .get_mut(&event_id.idx())
//             .expect("Event ID not found in store")
//             .remove(event_id.seq() as usize - 1);
//     }

//     fn retain<T: Fn(&TaggedOp<O>) -> bool>(&mut self, predicate: T) {
//         for events in self.store.values_mut() {
//             events.retain(|(t, _)| predicate(t));
//         }
//     }

//     fn clear(&mut self) {
//         self.store.clear();
//     }
// }

#[cfg(test)]
mod tests {
    use crate::{
        broadcast::internalizer::{Interner, Resolver},
        event::lamport::Lamport,
    };

    use super::*;

    struct Fixture {
        resolver: Resolver,
        a: ReplicaIdx,
        b: ReplicaIdx,
        c: ReplicaIdx,
    }

    impl Fixture {
        fn new() -> Self {
            let mut interner = Interner::new();
            let (a, _) = interner.intern("A");
            let (b, _) = interner.intern("B");
            let (c, _) = interner.intern("C");

            Self {
                resolver: interner.resolver().clone(),
                a,
                b,
                c,
            }
        }

        fn event(
            &self,
            origin: ReplicaIdx,
            sequence: usize,
            observed: &[(ReplicaIdx, usize)],
            op: u8,
        ) -> Event<u8> {
            let mut version = Version::new(origin, self.resolver.clone());
            version.set_by_idx(origin, sequence);
            for &(replica, sequence) in observed {
                version.set_by_idx(replica, sequence);
            }

            Event::new(
                EventId::new(origin, sequence, self.resolver.clone()),
                Lamport::from(&version),
                op,
                version,
            )
        }

        fn id(&self, origin: ReplicaIdx, sequence: usize) -> EventId {
            EventId::new(origin, sequence, self.resolver.clone())
        }

        fn version(&self, origin: ReplicaIdx, observed: &[(ReplicaIdx, usize)]) -> Version {
            let mut version = Version::new(origin, self.resolver.clone());
            for &(replica, sequence) in observed {
                version.set_by_idx(replica, sequence);
            }
            version
        }
    }

    #[test]
    fn empty_unknown_and_sequence_zero_queries_are_safe() {
        let fixture = Fixture::new();
        let mut history = EventHistory::<u8>::default();
        let zero = fixture.id(fixture.a, 0);
        let unknown = fixture.id(fixture.c, 1);
        let zero_version = fixture.version(fixture.a, &[]);

        assert!(history.is_empty());
        assert_eq!(history.len(), 0);
        assert!(history.get(&zero).is_none());
        assert!(history.get(&unknown).is_none());
        assert!(history.previous(&zero_version, fixture.a).is_none());
        assert!(history.previous(&zero_version, fixture.c).is_none());
        assert!(history.next(&zero, fixture.a).is_none());
        assert!(history.next(&unknown, fixture.c).is_none());

        history.append(fixture.event(fixture.a, 1, &[], 1));
        assert!(history.get(&zero).is_none());
        assert!(history.next(&zero, fixture.a).is_none());
    }

    #[test]
    fn append_get_and_len_cover_dense_per_origin_histories() {
        let fixture = Fixture::new();
        let mut history = EventHistory::default();

        history.append(fixture.event(fixture.a, 1, &[], 11));
        history.append(fixture.event(fixture.b, 1, &[(fixture.a, 1)], 21));
        history.append(fixture.event(fixture.a, 2, &[(fixture.b, 1)], 12));

        assert_eq!(history.len(), 3);
        assert_eq!(history.iter().count(), 3);
        assert!(!history.is_empty());
        assert_eq!(history.get(&fixture.id(fixture.a, 1)).unwrap().op(), &11);
        assert_eq!(history.get(&fixture.id(fixture.a, 2)).unwrap().op(), &12);
        assert_eq!(history.get(&fixture.id(fixture.b, 1)).unwrap().op(), &21);
        assert!(history.get(&fixture.id(fixture.a, 3)).is_none());
        assert!(history.get(&fixture.id(fixture.c, 1)).is_none());
    }

    #[test]
    fn get_requires_the_exact_event_id() {
        let fixture = Fixture::new();
        let mut history = EventHistory::default();
        history.append(fixture.event(fixture.a, 1, &[], 11));

        let derived = fixture.id(fixture.a, 1).with_disambiguator(7);

        assert!(history.get(&derived).is_none());
    }

    #[test]
    fn previous_obeys_zero_unknown_and_sequence_boundaries() {
        let fixture = Fixture::new();
        let mut history = EventHistory::default();
        history.append(fixture.event(fixture.a, 1, &[], 11));
        history.append(fixture.event(fixture.a, 2, &[], 12));
        history.append(fixture.event(fixture.b, 1, &[(fixture.a, 1)], 21));

        let zero = fixture.version(fixture.b, &[]);
        let through_a1 = fixture.version(fixture.b, &[(fixture.a, 1)]);
        let through_a2 = fixture.version(fixture.b, &[(fixture.a, 2)]);
        let missing_a3 = fixture.version(fixture.b, &[(fixture.a, 3)]);

        assert!(history.previous(&zero, fixture.a).is_none());
        assert_eq!(
            history.previous(&through_a1, fixture.a).unwrap().id(),
            &fixture.id(fixture.a, 1)
        );
        assert_eq!(
            history.previous(&through_a2, fixture.a).unwrap().id(),
            &fixture.id(fixture.a, 2)
        );
        assert!(history.previous(&missing_a3, fixture.a).is_none());
        assert!(history.previous(&through_a2, fixture.c).is_none());
    }

    #[test]
    fn next_returns_the_first_event_that_observes_the_candidate() {
        let fixture = Fixture::new();
        let mut history = EventHistory::default();
        history.append(fixture.event(fixture.b, 1, &[], 21));
        history.append(fixture.event(fixture.b, 2, &[(fixture.a, 1)], 22));
        history.append(fixture.event(fixture.b, 3, &[(fixture.a, 1)], 23));
        history.append(fixture.event(fixture.b, 4, &[(fixture.a, 2)], 24));

        assert_eq!(
            history
                .next(&fixture.id(fixture.a, 1), fixture.b)
                .unwrap()
                .id(),
            &fixture.id(fixture.b, 2)
        );
        assert_eq!(
            history
                .next(&fixture.id(fixture.a, 2), fixture.b)
                .unwrap()
                .id(),
            &fixture.id(fixture.b, 4)
        );
        assert!(history.next(&fixture.id(fixture.a, 3), fixture.b).is_none());
        assert!(history.next(&fixture.id(fixture.a, 1), fixture.c).is_none());
    }

    #[test]
    fn predecessors_are_inclusive_and_clamped_to_the_observed_history() {
        let fixture = Fixture::new();
        let mut history = EventHistory::default();
        history.append(fixture.event(fixture.a, 1, &[], 11));
        history.append(fixture.event(fixture.a, 2, &[], 12));
        history.append(fixture.event(fixture.b, 1, &[], 21));
        history.append(fixture.event(fixture.b, 2, &[], 22));

        let frontier = fixture.version(fixture.a, &[(fixture.a, 1), (fixture.b, 2)]);
        let mut operations = history
            .predecessors(&frontier)
            .into_iter()
            .map(|event| *event.op())
            .collect::<Vec<_>>();
        operations.sort_unstable();

        assert_eq!(operations, vec![11, 21, 22]);

        let beyond_history = fixture.version(fixture.a, &[(fixture.a, 99), (fixture.b, 99)]);
        assert_eq!(history.predecessors(&beyond_history).len(), 4);
    }

    #[test]
    #[should_panic(expected = "EventHistory requires dense per-origin insertion")]
    fn append_rejects_sequence_zero() {
        let fixture = Fixture::new();
        let mut history = EventHistory::default();

        history.append(fixture.event(fixture.a, 0, &[], 10));
    }

    #[test]
    #[should_panic(expected = "EventHistory requires dense per-origin insertion")]
    fn append_rejects_sequence_gaps() {
        let fixture = Fixture::new();
        let mut history = EventHistory::default();

        history.append(fixture.event(fixture.a, 2, &[], 12));
    }

    #[test]
    #[should_panic(expected = "EventHistory requires dense per-origin insertion")]
    fn append_rejects_duplicate_sequences() {
        let fixture = Fixture::new();
        let mut history = EventHistory::default();
        history.append(fixture.event(fixture.a, 1, &[], 11));

        history.append(fixture.event(fixture.a, 1, &[], 99));
    }

    #[test]
    #[should_panic(expected = "EventHistory event dot origin mismatch")]
    fn append_rejects_a_dot_with_a_different_origin() {
        let fixture = Fixture::new();
        let mut history = EventHistory::default();
        let version = fixture.version(fixture.b, &[(fixture.a, 1), (fixture.b, 1)]);
        let event = Event::new(
            fixture.id(fixture.a, 1),
            Lamport::from(&version),
            11,
            version,
        );

        history.append(event);
    }

    #[test]
    #[should_panic(expected = "EventHistory event dot sequence mismatch")]
    fn append_rejects_a_dot_with_a_different_sequence() {
        let fixture = Fixture::new();
        let mut history = EventHistory::default();
        let version = fixture.version(fixture.a, &[(fixture.a, 2)]);
        let event = Event::new(
            fixture.id(fixture.a, 1),
            Lamport::from(&version),
            11,
            version,
        );

        history.append(event);
    }

    #[test]
    #[should_panic(expected = "EventHistory requires componentwise-monotone versions per origin")]
    fn append_rejects_a_regressing_causal_component() {
        let fixture = Fixture::new();
        let mut history = EventHistory::default();
        history.append(fixture.event(fixture.a, 1, &[(fixture.b, 1)], 11));

        history.append(fixture.event(fixture.a, 2, &[], 12));
    }
}
