use std::{cmp::Ordering, collections::BinaryHeap};

use moirai_protocol::{event::id::EventId, state::unstable_state::CausalReplay};

use crate::HashMap;

/// Events that must be undone and redone to move the prepared document to a
/// new causal frontier.
pub(super) struct Transition {
    pub(super) retreat: Vec<EventId>,
    pub(super) advance: Vec<EventId>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Current,
    Target,
    Shared,
}

#[derive(Clone, Eq, PartialEq)]
struct QueueEntry {
    delivery_order: usize,
    event_id: EventId,
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.delivery_order
            .cmp(&other.delivery_order)
            .then_with(|| self.event_id.origin_id().cmp(other.event_id.origin_id()))
            .then_with(|| self.event_id.seq().cmp(&other.event_id.seq()))
            .then_with(|| {
                self.event_id
                    .disambiguator()
                    .cmp(&other.event_id.disambiguator())
            })
    }
}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

/// Compute the causal difference between the prepared head and a target
/// frontier. Newer events are visited first until both walks reach only shared
/// ancestors.
pub(super) fn transition<O, U>(
    state: &U,
    prepared_head: Option<&EventId>,
    target_frontier: &[EventId],
) -> Transition
where
    U: CausalReplay<O>,
{
    #[allow(clippy::mutable_key_type)]
    let mut sides: HashMap<EventId, Side> = HashMap::default();
    let mut queue = BinaryHeap::new();
    let mut shared_count = 0usize;

    #[allow(clippy::mutable_key_type)]
    fn enqueue<O, U>(
        state: &U,
        sides: &mut HashMap<EventId, Side>,
        queue: &mut BinaryHeap<QueueEntry>,
        shared_count: &mut usize,
        event_id: EventId,
        side: Side,
    ) where
        U: CausalReplay<O>,
    {
        match sides.get(&event_id).copied() {
            None => {
                queue.push(QueueEntry {
                    delivery_order: state.delivery_order(&event_id).unwrap(),
                    event_id: event_id.clone(),
                });
                if side == Side::Shared {
                    *shared_count += 1;
                }
                sides.insert(event_id, side);
            }
            Some(previous) if previous != side && previous != Side::Shared => {
                sides.insert(event_id, Side::Shared);
                *shared_count += 1;
            }
            Some(_) => {}
        }
    }

    if let Some(event_id) = prepared_head {
        enqueue(
            state,
            &mut sides,
            &mut queue,
            &mut shared_count,
            event_id.clone(),
            Side::Current,
        );
    }

    for event_id in target_frontier {
        enqueue(
            state,
            &mut sides,
            &mut queue,
            &mut shared_count,
            event_id.clone(),
            Side::Target,
        );
    }

    let mut retreat = Vec::new();
    let mut advance = Vec::new();

    while queue.len() > shared_count {
        let event_id = queue.pop().unwrap().event_id;
        let side = sides[&event_id];

        match side {
            Side::Shared => shared_count -= 1,
            Side::Current => retreat.push(event_id.clone()),
            Side::Target => advance.push(event_id.clone()),
        }

        for parent in state.direct_predecessors(&event_id) {
            enqueue(
                state,
                &mut sides,
                &mut queue,
                &mut shared_count,
                parent,
                side,
            );
        }
    }

    Transition { retreat, advance }
}
