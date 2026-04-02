use std::collections::{BTreeMap, BTreeSet};

use moirai_protocol::event::id::EventId;

#[derive(Clone, Debug, Default)]
pub struct PresenceState {
    pub inserted: bool,
    // Birth dots currently present in the prepared causal context.
    pub born_dots: BTreeSet<EventId>,
    // Deletes are counted per dot. Two concurrent deletes may both target the same
    // birth dot, and retreat/advance must undo them one occurrence at a time.
    pub deleted_dots: BTreeMap<EventId, u32>,
}

impl PresenceState {
    pub fn is_integrated(&self) -> bool {
        self.inserted
    }

    pub fn is_visible(&self) -> bool {
        self.inserted
            && self
                .born_dots
                .iter()
                .any(|dot| self.deleted_dots.get(dot).copied().unwrap_or(0) == 0)
    }

    pub fn visible_dots(&self) -> BTreeSet<EventId> {
        // Update-wins semantics is expressed here: a dot is visible if it exists in the
        // prepared context and no prepared delete currently cancels it.
        self.born_dots
            .iter()
            .filter(|dot| self.deleted_dots.get(*dot).copied().unwrap_or(0) == 0)
            .cloned()
            .collect()
    }

    pub fn add_deleted(&mut self, dot: &EventId) {
        *self.deleted_dots.entry(dot.clone()).or_insert(0) += 1;
    }

    pub fn remove_deleted(&mut self, dot: &EventId) {
        match self.deleted_dots.get_mut(dot) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                self.deleted_dots.remove(dot);
            }
            None => {}
        }
    }
}
