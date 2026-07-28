//! What the CRDT actually *did* with each delivered event.
//!
//! A replica's rendered state says what everybody agreed on. It does not say
//! *how* — which of two concurrent operations survived, which one a redundancy
//! relation removed, which one was discarded on arrival without ever entering a
//! log. That is the interesting half of a pure operation-based CRDT, and until
//! now it was only inferable by staring at a final value.
//!
//! Nothing here decides anything. Every field is a decision
//! [`POLog::effect`](crate::state::po_log::POLog) has already taken by the time
//! it is recorded: `redundant_itself` returning true, the `retain` predicate
//! dropping an entry, a parent resetting a child. This module writes those
//! outcomes down; it does not compute them.
//!
//! # Off unless asked
//!
//! The buffer is a thread-local `Option`, `None` until [`set_enabled`] is
//! called. While it is `None` every recording call is one thread-local read and
//! a branch — no allocation, no formatting, no lock. A replica whose operator is
//! not watching pays that and nothing else.
//!
//! # Thread affinity
//!
//! Deliberately thread-local rather than shared. Delivery happens on exactly one
//! thread — the replica's event loop — so a `Mutex` would only add contention
//! with the HTTP thread for no benefit, and a trace collected on some *other*
//! thread would be nobody's delivery order. [`drain`] must therefore be called
//! from the same thread that delivers, which for `GenericNode` is `run()`.

use std::cell::RefCell;

use crate::event::{id::EventId, lamport::Lamport};

/// How many records may accumulate between two [`drain`] calls before the
/// oldest are dropped.
///
/// A monitoring buffer that grows without bound is a memory leak waiting for
/// its consumer to fall behind. The consumer drains once per event-loop
/// iteration, so reaching this means it has stopped; losing the oldest records
/// is the right failure.
const CAPACITY: usize = 4096;

/// One operation this delivery removed from a log, and whether it was
/// *concurrent* with the arriving operation or causally before it.
///
/// The distinction is the whole point. An operation removed because it was
/// causally dominated lost to time; an operation removed while concurrent lost
/// to a redundancy relation, which is a conflict resolution and is what an
/// observer wants to see.
#[derive(Debug, Clone)]
pub struct Superseded {
    /// The removed operation's id. Kept as an [`EventId`] rather than rendered:
    /// cloning one is an `Rc` increment, and formatting it is an allocation on
    /// the delivery path that only matters if anybody ever reads it.
    pub id: EventId,
    /// `true` when the removed operation was concurrent with the arriving one.
    pub concurrent: bool,
}

/// What happened to one delivered event.
#[derive(Debug, Clone)]
pub struct Delivery {
    /// The event delivered. Carries its origin replica and sequence number, and
    /// clones as an `Rc` increment — which is why it is kept whole rather than
    /// decomposed into a `String` here, on the delivery path, for the benefit of
    /// a consumer that may format it once per several hundred deliveries.
    pub id: EventId,
    pub lamport: usize,
    /// A log appended the operation: it is now part of the state.
    pub applied: bool,
    /// A `redundant_itself` relation (R) discarded the operation on arrival —
    /// it was delivered, and it is not in any log.
    pub redundant_on_arrival: bool,
    /// A parent reset a child log because of this operation: a map `Remove` or
    /// `Clear`, which is how update-wins is enforced.
    pub reset: bool,
    /// Operations this delivery removed, by `redundant_by_when_redundant` (R0),
    /// `redundant_by_when_not_redundant` (R1), or a parent reset.
    ///
    /// Only *unstable* operations appear here. Ones already folded into a stable
    /// state no longer carry an id to name them by, so a redundancy relation
    /// that prunes stable state is invisible to this trace. In practice the
    /// interesting resolutions are concurrent, and concurrent operations are by
    /// construction not yet stable.
    pub superseded: Vec<Superseded>,
}

impl Delivery {
    fn new(id: &EventId, lamport: &Lamport) -> Self {
        Self {
            id: id.clone(),
            lamport: lamport.val(),
            applied: false,
            redundant_on_arrival: false,
            reset: false,
            superseded: Vec::new(),
        }
    }
}

thread_local! {
    /// `None` means nobody is watching; see the module header.
    static TRACE: RefCell<Option<Vec<Delivery>>> = const { RefCell::new(None) };
}

/// Start or stop recording on the calling thread.
///
/// Disabling drops whatever had accumulated, which is what a consumer that has
/// gone away wants.
pub fn set_enabled(on: bool) {
    TRACE.with(|t| {
        *t.borrow_mut() = on.then(Vec::new);
    });
}

/// `true` while this thread is recording.
pub fn is_enabled() -> bool {
    TRACE.with(|t| t.borrow().is_some())
}

/// Take everything recorded since the last drain.
pub fn drain() -> Vec<Delivery> {
    TRACE.with(|t| match t.borrow_mut().as_mut() {
        Some(buffer) => std::mem::take(buffer),
        None => Vec::new(),
    })
}

/// Open a record for an event about to be delivered. Called by
/// [`Replica::deliver`](crate::replica::Replica), the one place every delivery
/// passes through, local and remote alike.
pub(crate) fn begin(id: &EventId, lamport: &Lamport) {
    TRACE.with(|t| {
        if let Some(buffer) = t.borrow_mut().as_mut() {
            if buffer.len() >= CAPACITY {
                buffer.remove(0);
            }
            buffer.push(Delivery::new(id, lamport));
        }
    });
}

/// Amend the record currently open, if any.
fn note(f: impl FnOnce(&mut Delivery)) {
    TRACE.with(|t| {
        if let Some(delivery) = t.borrow_mut().as_mut().and_then(|b| b.last_mut()) {
            f(delivery);
        }
    });
}

/// A log appended the arriving operation.
pub(crate) fn note_applied() {
    note(|d| d.applied = true);
}

/// A relation R discarded the arriving operation before any log took it.
pub(crate) fn note_redundant_on_arrival() {
    note(|d| d.redundant_on_arrival = true);
}

/// A parent reset a child log because of the arriving operation.
pub(crate) fn note_reset() {
    note(|d| d.reset = true);
}

/// The arriving operation removed `id` from a log.
pub(crate) fn note_superseded(id: &EventId, concurrent: bool) {
    note(|d| {
        d.superseded.push(Superseded {
            id: id.clone(),
            concurrent,
        })
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::intern_str::Interner;

    fn an_event_id(id: &str, seq: usize) -> EventId {
        let mut interner = Interner::new();
        let (idx, _) = interner.intern(&id.to_string());
        EventId::new(idx, seq, interner.resolver().clone())
    }

    #[test]
    fn records_nothing_until_enabled() {
        set_enabled(false);
        begin(&an_event_id("a", 1), &Lamport::new(0));
        note_applied();
        assert!(drain().is_empty());
    }

    #[test]
    fn records_the_outcome_of_a_delivery() {
        set_enabled(true);
        begin(&an_event_id("a", 3), &Lamport::new(0));
        note_applied();
        note_superseded(&an_event_id("b", 2), true);

        let drained = drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].id.origin_id(), "a");
        assert_eq!(drained[0].id.seq(), 3);
        assert!(drained[0].applied);
        assert_eq!(drained[0].superseded.len(), 1);
        assert_eq!(drained[0].superseded[0].id.origin_id(), "b");
        assert_eq!(drained[0].superseded[0].id.seq(), 2);
        assert!(drained[0].superseded[0].concurrent);

        // Draining empties the buffer rather than replaying it.
        assert!(drain().is_empty());
        set_enabled(false);
    }

    #[test]
    fn drops_the_oldest_rather_than_growing_without_bound() {
        set_enabled(true);
        for seq in 0..(CAPACITY + 10) {
            begin(&an_event_id("a", seq), &Lamport::new(0));
        }
        let drained = drain();
        assert_eq!(drained.len(), CAPACITY);
        // The first ten are the ones that went.
        assert_eq!(drained[0].id.seq(), 10);
        set_enabled(false);
    }
}
