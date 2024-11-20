use std::collections::{HashSet, VecDeque};

use crate::{clocks::matrix_clock::MatrixClock, crdt::membership_set::MSet};

use super::{event::Event, metadata::Metadata, po_log::POLog, pure_crdt::PureCRDT, tcsb::AnyOp};

/// Check that the event is not from an evicted peer
pub fn guard_against_evicted(evicted: &HashSet<String>, metadata: &Metadata) -> bool {
    evicted.contains(&metadata.origin)
}

/// Check that the event has not already been delivered
pub fn guard_against_duplicates(ltm: &MatrixClock<String, usize>, metadata: &Metadata) -> bool {
    ltm.get(&metadata.origin)
        .map(|other_clock| metadata.clock <= *other_clock)
        .unwrap_or(false)
}

/// Check that the event is the causal successor of the last event delivered by this same replica
/// Returns true if the event is out of order
pub fn guard_against_out_of_order(
    ltm: &MatrixClock<String, usize>,
    evicted: &HashSet<String>,
    metadata: &Metadata,
) -> bool {
    // We assume that the LTM and the event clock have the same number of entries
    assert_eq!(
        ltm.filtered_keys(evicted),
        metadata.clock.keys(),
        "LTM Keys ({}): {:?}, Event Keys ({}): {:?}",
        ltm.filtered_keys(evicted).len(),
        ltm.filtered_keys(evicted),
        metadata.clock.len(),
        metadata.clock.keys()
    );
    // We assume that the event clock has an entry for its origin
    let event_lamport_clock = metadata.clock.get(&metadata.origin).unwrap();
    // We assume we know this origin
    let ltm_origin_clock = ltm.get(&metadata.origin).unwrap();
    // We assume that the clock we have for this origin has an entry for this origin
    let ltm_lamport_lock = ltm_origin_clock.get(&metadata.origin).unwrap();
    // Either the event is the next in the sequence or the event is causally superior to the origin eviction
    let is_origin_out_of_order = event_lamport_clock != ltm_lamport_lock + 1;
    let are_other_entries_out_of_order = metadata
        .clock
        .iter()
        .filter(|(k, _)| *k != &metadata.origin)
        .any(|(k, v)| {
            let ltm_clock = ltm.get(k).unwrap();
            let ltm_value = ltm_clock.get(k).unwrap();
            *v > ltm_value
        });
    is_origin_out_of_order || are_other_entries_out_of_order
}

/// Check that the event is not from an unknown peer
/// The peer is unknown if it is not in the LTM and there is no unstable `add`
/// operation for it in the group membership
pub fn guard_against_unknow_peer(
    ltm: &MatrixClock<String, usize>,
    metadata: &Metadata,
    group_membership: &POLog<MSet<String>>,
) -> bool {
    ltm.get(&metadata.origin).is_none()
        && !group_membership
            .unstable
            .iter()
            .any(|(_, o)| match o.as_ref() {
                MSet::Add(v) => v == &metadata.origin,
                _ => false,
            })
}

/// Check that the event is not coming from a peer that is going to be removed from the group.
/// Returns true if the event is not ready to be delivered
pub fn guard_against_concurrent_to_remove<O: PureCRDT>(
    event: &Event<AnyOp<O>>,
    group_membership: &POLog<MSet<String>>,
    pending: &VecDeque<Event<AnyOp<O>>>,
) -> bool {
    // Do not deliver the event if the origin is going to be removed from the group...
    let will_be_removed = group_membership
        .unstable
        .iter()
        .any(|(_, o)| matches!(o.as_ref(), MSet::Remove(v) if v == &event.metadata.origin));
    // ...unless the event is necessary to deliver other peers events
    let necessary = pending.iter().any(|e| {
        e.metadata.get_lamport(&event.metadata.origin) >= event.metadata.get_origin_lamport()
            && e.metadata.origin != event.metadata.origin
    });
    will_be_removed && !necessary
}
