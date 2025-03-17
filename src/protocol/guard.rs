use std::{cmp::Ordering, collections::HashSet};

use crate::clocks::{clock::Clock, dependency_clock::DependencyClock, matrix_clock::MatrixClock};

/// Check that the event is not from an evicted peer
pub fn guard_against_removed_members(
    removed_members: &HashSet<String>,
    metadata: &DependencyClock,
) -> bool {
    removed_members.contains(metadata.origin())
}

/// Check that the event has not already been delivered
pub fn guard_against_duplicates(
    ltm: &MatrixClock<String, usize>,
    metadata: &DependencyClock,
) -> bool {
    ltm.get(&metadata.origin().to_string())
        .map(|other_clock| metadata.partial_cmp(*other_clock) == Some(Ordering::Less))
        .unwrap_or(false)
}

/// Check that the event is the causal successor of the last event delivered by this same replica
/// Returns true if the event is out of order
pub fn guard_against_out_of_order(
    ltm: &MatrixClock<String, usize>,
    metadata: &DependencyClock,
) -> bool {
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
