use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use log::error;

use crate::clocks::{clock::Clock, dependency_clock::DependencyClock, matrix_clock::MatrixClock};

/// Check that the event is not from an evicted peer
pub fn guard_against_removed_members(
    removed_members: &HashSet<String>,
    metadata: &DependencyClock,
) -> bool {
    removed_members.contains(metadata.origin())
}

/// Check that the event has not already been delivered
pub fn guard_against_duplicates(ltm: &MatrixClock, clock: &DependencyClock) -> bool {
    ltm.get(clock.origin())
        .map(|other_clock| clock.partial_cmp(other_clock) == Some(Ordering::Less))
        .unwrap_or(false)
}

/// Check that the event is the causal successor of the last event delivered by this same replica
/// Returns true if the event is out of order
pub fn guard_against_out_of_order(ltm: &MatrixClock, clock: &DependencyClock) -> bool {
    let map: HashMap<String, usize> = clock.clone().into();
    for (origin, cnt) in map {
        if origin == clock.origin() {
            if cnt != ltm.dot(&origin) + 1 {
                return true;
            } else {
                continue;
            }
        }
        if cnt > ltm.dot(&origin) {
            error!("bizarre");
            return true;
        }
    }
    false
}
